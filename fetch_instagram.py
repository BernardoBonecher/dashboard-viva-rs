"""
fetch_instagram.py — Puxa dados do Instagram Graph API e gera instagram_data.json.

Como rodar:
    py fetch_instagram.py

Requer as variáveis de ambiente IG_BUSINESS_ID e IG_ACCESS_TOKEN.

Localmente:
    $env:IG_BUSINESS_ID="17841445023497218"
    $env:IG_ACCESS_TOKEN="EAAZ..."
    py fetch_instagram.py

No GitHub Actions, esses valores vêm dos Secrets do repo.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import HTTPError

IG_BUSINESS_ID = os.environ.get("IG_BUSINESS_ID")
IG_ACCESS_TOKEN = os.environ.get("IG_ACCESS_TOKEN")
GRAPH_VERSION = "v23.0"
BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"
OUTPUT_PATH = Path(__file__).parent / "instagram_data.json"
HISTORY_PATH = Path(__file__).parent / "instagram_history.json"
MEDIA_LIMIT = 50  # quantos posts recentes puxar


def http_get(path: str, params: dict) -> dict:
    url = f"{BASE}/{path}?{urlencode(params)}"
    with urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_account() -> dict:
    return http_get(IG_BUSINESS_ID, {
        "access_token": IG_ACCESS_TOKEN,
        "fields": "username,name,biography,profile_picture_url,followers_count,follows_count,media_count",
    })


def fetch_recent_media(limit: int = MEDIA_LIMIT) -> list:
    data = http_get(f"{IG_BUSINESS_ID}/media", {
        "access_token": IG_ACCESS_TOKEN,
        "fields": "id,caption,media_type,media_product_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count",
        "limit": limit,
    })
    return data.get("data", [])


def fetch_media_insights(media_id: str, media_type: str, media_product_type: str = "") -> dict:
    """Insights por post. Métricas dependem do tipo (FEED/REELS/STORY).

    Pra FEED (IMAGE, CAROUSEL_ALBUM): reach, saved, shares
    Pra REELS / VIDEO: reach, saved, shares, plays (alguns)
    """
    metric = "reach,saved,shares"
    try:
        return http_get(f"{media_id}/insights", {
            "access_token": IG_ACCESS_TOKEN,
            "metric": metric,
        })
    except HTTPError:
        return {"data": []}


def flatten_insights(insights: dict) -> dict:
    """Transforma a resposta {data: [{name: x, values: [{value: n}]}]} em {x: n}."""
    out = {}
    for entry in insights.get("data", []):
        vals = entry.get("values", [])
        if vals:
            out[entry["name"]] = vals[0].get("value", 0)
    return out


def update_history(account: dict, posts: list) -> int:
    """Adiciona/atualiza snapshot do dia em instagram_history.json.

    Como o IG Graph API não dá histórico de seguidores, salvamos nós mesmos
    1 snapshot por dia. O arquivo é commitado pelo workflow do GitHub Actions
    pra acumular ao longo do tempo.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    history = {"snapshots": []}
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass

    snapshot = {
        "date": today,
        "followers_count": int(account.get("followers_count", 0) or 0),
        "follows_count": int(account.get("follows_count", 0) or 0),
        "media_count": int(account.get("media_count", 0) or 0),
        "posts_window": len(posts),
        "sum_likes": sum(p.get("like_count", 0) or 0 for p in posts),
        "sum_comments": sum(p.get("comments_count", 0) or 0 for p in posts),
        "sum_reach": sum(p.get("reach", 0) or 0 for p in posts),
        "sum_saves": sum(p.get("saved", 0) or 0 for p in posts),
    }

    # Substitui snapshot de hoje (se já existir) e mantém ordem por data.
    snapshots = [s for s in history.get("snapshots", []) if s.get("date") != today]
    snapshots.append(snapshot)
    snapshots.sort(key=lambda s: s["date"])

    HISTORY_PATH.write_text(
        json.dumps({"snapshots": snapshots}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return len(snapshots)


def main() -> None:
    if not IG_BUSINESS_ID or not IG_ACCESS_TOKEN:
        raise RuntimeError(
            "IG_BUSINESS_ID e IG_ACCESS_TOKEN são obrigatórios. "
            "Defina como variáveis de ambiente antes de rodar."
        )

    print("Puxando dados do Instagram...")
    print("  • Conta...")
    account = fetch_account()
    print(f"    ✓ @{account['username']} — {account['followers_count']:,} seguidores, {account['media_count']:,} posts")

    print(f"  • Últimos {MEDIA_LIMIT} posts...")
    posts = fetch_recent_media()
    print(f"    ✓ {len(posts)} posts recebidos")

    print("  • Insights por post (likes/comments já vêm do passo anterior, agora reach/saved/shares)...")
    for i, post in enumerate(posts, 1):
        if i % 10 == 0 or i == len(posts):
            print(f"    {i}/{len(posts)}")
        insights = fetch_media_insights(
            post["id"],
            post.get("media_type", ""),
            post.get("media_product_type", ""),
        )
        post.update(flatten_insights(insights))

    payload = {
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "account": account,
        "posts": posts,
    }

    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n✓ Dados salvos em {OUTPUT_PATH.name} ({size_kb:,.0f} KB)")

    n_snapshots = update_history(account, posts)
    print(f"✓ Snapshot diário salvo em {HISTORY_PATH.name} ({n_snapshots} snapshots no histórico)")


if __name__ == "__main__":
    main()
