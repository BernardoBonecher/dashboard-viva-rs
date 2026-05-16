"""
setup_instagram.py — Setup inicial das credenciais do Instagram Graph API.

Como usar:
1. Edita as 3 variáveis abaixo (APP_ID, APP_SECRET, SHORT_USER_TOKEN).
2. Roda: py setup_instagram.py
3. O script imprime o Page Access Token (que não expira) e o
   Instagram Business Account ID — você cola os dois como Secrets no GitHub.

Os valores acima são SENSÍVEIS. Depois de rodar com sucesso, apague-os deste arquivo.
"""

import sys
from urllib.parse import urlencode
from urllib.request import urlopen
import json

# ============================================================
# EDITE AQUI antes de rodar (depois apague os valores!)
# ============================================================
APP_ID = "COLE_O_APP_ID_AQUI"
APP_SECRET = "COLE_O_APP_SECRET_AQUI"
SHORT_USER_TOKEN = "COLE_O_TOKEN_DO_GRAPH_API_EXPLORER_AQUI"
# ============================================================

GRAPH_VERSION = "v23.0"
BASE = f"https://graph.facebook.com/{GRAPH_VERSION}"


def http_get(path: str, params: dict) -> dict:
    url = f"{BASE}/{path}?{urlencode(params)}"
    with urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    if "COLE_" in APP_ID or "COLE_" in APP_SECRET or "COLE_" in SHORT_USER_TOKEN:
        print("⚠ Edite o topo deste arquivo com seus valores antes de rodar.")
        sys.exit(1)

    print("1/3 · Trocando token de curta duração por long-lived user token (60 dias)...")
    data = http_get("oauth/access_token", {
        "grant_type": "fb_exchange_token",
        "client_id": APP_ID,
        "client_secret": APP_SECRET,
        "fb_exchange_token": SHORT_USER_TOKEN,
    })
    long_user_token = data["access_token"]
    print("    ✓ Long-lived user token obtido.\n")

    print("2/3 · Listando páginas do Facebook que você administra...")
    pages_data = http_get("me/accounts", {
        "access_token": long_user_token,
        "fields": "id,name,access_token,instagram_business_account",
    })
    pages = pages_data.get("data", [])
    print(f"    ✓ {len(pages)} página(s) encontrada(s):\n")
    for i, p in enumerate(pages, 1):
        ig = p.get("instagram_business_account")
        ig_info = f"IG ID: {ig['id']}" if ig else "(SEM Instagram conectado)"
        print(f"    [{i}] {p['name']}  ·  {ig_info}")
    print()

    pages_with_ig = [p for p in pages if p.get("instagram_business_account")]
    if not pages_with_ig:
        print("⚠ Nenhuma página tem conta Instagram conectada.")
        print("  Vá no Meta Business Suite → Configurações → Contas → Instagram → conectar.")
        sys.exit(1)

    # Filtra pela página do Viva O RS especificamente (case-insensitive)
    target = "viva o rs"
    matches = [p for p in pages_with_ig if p["name"].lower() == target]
    if not matches:
        print(f"⚠ Página '{target}' não encontrada. Páginas disponíveis:")
        for p in pages_with_ig:
            print(f"    - {p['name']}")
        sys.exit(1)
    page = matches[0]
    print(f"✓ Usando página: {page['name']}\n")

    ig_id = page["instagram_business_account"]["id"]
    page_token = page["access_token"]

    print("3/3 · Validando que conseguimos ler insights do Instagram...")
    try:
        ig_info = http_get(ig_id, {
            "access_token": page_token,
            "fields": "username,followers_count,media_count",
        })
        print(f"    ✓ Conta: @{ig_info.get('username', '?')}")
        print(f"      Seguidores: {ig_info.get('followers_count', '?'):,}")
        print(f"      Posts: {ig_info.get('media_count', '?'):,}\n")
    except Exception as e:
        print(f"    ⚠ Erro lendo dados da conta: {e}")
        print("      Token e ID foram gerados, mas pode haver problema de permissão.\n")

    print("=" * 64)
    print("SUCESSO. Adicione esses dois Secrets no GitHub:")
    print("   github.com/BernardoBonecher/dashboard-viva-rs/settings/secrets/actions")
    print("=" * 64)
    print()
    print(f"  IG_BUSINESS_ID  = {ig_id}")
    print()
    print(f"  IG_ACCESS_TOKEN = {page_token}")
    print()
    print("=" * 64)
    print("O Page Access Token NÃO expira (até você mudar a senha do FB ou")
    print("revogar permissões do app). Apague-o deste arquivo após colar no GitHub.")


if __name__ == "__main__":
    main()
