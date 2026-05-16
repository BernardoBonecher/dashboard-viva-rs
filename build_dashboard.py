"""
build_dashboard.py — Puxa dados do GA4 (Viva o RS) e gera dashboard.html.

Faz DUAS queries:
1. Granular (data × página × título × cidade × origem): pageviews, tempo, eventos.
   Estas métricas são "somáveis" — sobrevivem ao agrupamento por página.
2. Totais (data × cidade × origem, sem pagePath): sessions, totalUsers, duração,
   engajamento. Estas métricas NÃO são somáveis quando agrupadas por página
   (a mesma sessão visita N páginas → vira N linhas e infla os números).
"""

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account

# --- Configuração ---------------------------------------------------------
PROPERTY_ID = "448015610"
CREDENTIALS_PATH = Path(
    os.environ.get("GA4_CREDENTIALS_PATH")
    or Path(__file__).parent / "credentials" / "ga4-viva-rs.json"
)
TEMPLATE_PATH = Path(__file__).parent / "dashboard_template.html"
OUTPUT_PATH = Path(__file__).parent / "dashboard.html"
INSTAGRAM_PATH = Path(__file__).parent / "instagram_data.json"
INSTAGRAM_HISTORY_PATH = Path(__file__).parent / "instagram_history.json"

START_DATE = "2025-01-01"
END_DATE = "yesterday"
PAGE_SIZE = 100000


def build_client() -> BetaAnalyticsDataClient:
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def _paginated_query(client, dimensions, metrics, transform_row, label):
    """Roda uma query em blocos de PAGE_SIZE até pegar todas as linhas."""
    all_rows = []
    offset = 0
    while True:
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            offset=offset,
            limit=PAGE_SIZE,
        )
        response = client.run_report(request)
        for row in response.rows:
            all_rows.append(transform_row(row))
        total = response.row_count
        fetched = offset + len(response.rows)
        print(f"    {label}: {fetched:,}/{total:,}")
        if fetched >= total or len(response.rows) == 0:
            break
        offset = fetched
    return all_rows


def fetch_granular_data(client) -> pd.DataFrame:
    """Granular: data × pagePath × cidade × origem (pageviews/eventos/tempo)."""
    rows = _paginated_query(
        client,
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pagePath"),
            Dimension(name="pageTitle"),
            Dimension(name="city"),
            Dimension(name="sessionSourceMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="screenPageViews"),
            Metric(name="userEngagementDuration"),
            Metric(name="averageSessionDuration"),
            Metric(name="engagementRate"),
            Metric(name="eventCount"),
        ],
        transform_row=lambda row: {
            "data": row.dimension_values[0].value,
            "pagina": row.dimension_values[1].value or "(sem path)",
            "titulo": row.dimension_values[2].value or "",
            "cidade": row.dimension_values[3].value or "(não definida)",
            "origem": row.dimension_values[4].value or "(direto)",
            "sessoes": int(row.metric_values[0].value),
            "usuarios": int(row.metric_values[1].value),
            "pageviews": int(row.metric_values[2].value),
            "tempo_engajamento": float(row.metric_values[3].value),
            "duracao_sessao": float(row.metric_values[4].value),
            "taxa_engajamento": float(row.metric_values[5].value),
            "eventos": int(row.metric_values[6].value),
        },
        label="granular",
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    return df


def fetch_totals_data(client) -> pd.DataFrame:
    """Totais sem pagePath — sessions/users/duration não inflam."""
    rows = _paginated_query(
        client,
        dimensions=[
            Dimension(name="date"),
            Dimension(name="city"),
            Dimension(name="sessionSourceMedium"),
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="averageSessionDuration"),
            Metric(name="engagementRate"),
        ],
        transform_row=lambda row: {
            "data": row.dimension_values[0].value,
            "cidade": row.dimension_values[1].value or "(não definida)",
            "origem": row.dimension_values[2].value or "(direto)",
            "sessoes": int(row.metric_values[0].value),
            "usuarios": int(row.metric_values[1].value),
            "duracao_sessao": float(row.metric_values[2].value),
            "taxa_engajamento": float(row.metric_values[3].value),
        },
        label="totais",
    )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    return df


def _compact_granular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"paginas": [], "titulos": [], "cidades": [], "origens": [], "rows": []}

    paginas = sorted(df["pagina"].unique().tolist())
    cidades = sorted(df["cidade"].unique().tolist())
    origens = sorted(df["origem"].unique().tolist())

    titulo_map = df[df["titulo"] != ""].groupby("pagina")["titulo"].first().to_dict()
    titulos = [titulo_map.get(p, "") for p in paginas]

    pag_idx = {p: i for i, p in enumerate(paginas)}
    cid_idx = {c: i for i, c in enumerate(cidades)}
    ori_idx = {o: i for i, o in enumerate(origens)}

    rows = [
        [
            r.data,
            pag_idx[r.pagina],
            cid_idx[r.cidade],
            ori_idx[r.origem],
            r.sessoes,
            r.usuarios,
            r.pageviews,
            round(r.tempo_engajamento, 1),
            round(r.duracao_sessao, 1),
            round(r.taxa_engajamento, 4),
            r.eventos,
        ]
        for r in df.itertuples(index=False)
    ]

    return {
        "paginas": paginas,
        "titulos": titulos,
        "cidades": cidades,
        "origens": origens,
        "rows": rows,
    }


def _compact_totals(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"cidades": [], "origens": [], "rows": []}

    cidades = sorted(df["cidade"].unique().tolist())
    origens = sorted(df["origem"].unique().tolist())
    cid_idx = {c: i for i, c in enumerate(cidades)}
    ori_idx = {o: i for i, o in enumerate(origens)}

    rows = [
        [
            r.data,
            cid_idx[r.cidade],
            ori_idx[r.origem],
            r.sessoes,
            r.usuarios,
            round(r.duracao_sessao, 1),
            round(r.taxa_engajamento, 4),
        ]
        for r in df.itertuples(index=False)
    ]

    return {"cidades": cidades, "origens": origens, "rows": rows}


def _load_instagram_data():
    """Carrega instagram_data.json se existir (gerado por fetch_instagram.py)."""
    if not INSTAGRAM_PATH.exists():
        return None
    try:
        return json.loads(INSTAGRAM_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  ⚠ Erro lendo {INSTAGRAM_PATH.name}: {e}")
        return None


def generate_html(df_granular: pd.DataFrame, df_totals: pd.DataFrame) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    payload = {
        "granular": _compact_granular(df_granular),
        "totals": _compact_totals(df_totals),
    }
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    ig_data = _load_instagram_data()
    if ig_data and INSTAGRAM_HISTORY_PATH.exists():
        try:
            history = json.loads(INSTAGRAM_HISTORY_PATH.read_text(encoding="utf-8"))
            ig_data["history"] = history.get("snapshots", [])
        except Exception as e:
            print(f"  ⚠ Erro lendo {INSTAGRAM_HISTORY_PATH.name}: {e}")

    ig_json = json.dumps(ig_data, ensure_ascii=False, separators=(",", ":")) if ig_data else "null"
    if ig_data:
        n_hist = len(ig_data.get("history", []))
        print(f"  · Instagram: @{ig_data['account']['username']} ({len(ig_data.get('posts', []))} posts, {n_hist} snapshots no histórico)")
    else:
        print("  · Instagram: (sem dados — fetch_instagram.py não foi executado)")

    start_date = df_granular["data"].min() if not df_granular.empty else "—"
    end_date = df_granular["data"].max() if not df_granular.empty else "—"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    return (
        template
        .replace("__DATA_JSON__", data_json)
        .replace("__INSTAGRAM_JSON__", ig_json)
        .replace("__START_DATE__", _fmt_date(start_date))
        .replace("__END_DATE__", _fmt_date(end_date))
        .replace("__GENERATED_AT__", generated_at)
    )


def _fmt_date(iso_date: str) -> str:
    if iso_date == "—":
        return iso_date
    try:
        return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return iso_date


def main() -> None:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Credenciais não encontradas: {CREDENTIALS_PATH}")
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template não encontrado: {TEMPLATE_PATH}")

    print(f"Conectando no GA4 (property {PROPERTY_ID})...")
    client = build_client()

    print(f"Puxando dados ({START_DATE} → {END_DATE}):")
    print("  • Query granular (com pagePath)...")
    df_granular = fetch_granular_data(client)
    print(f"    ✓ {len(df_granular):,} linhas granulares")

    print("  • Query de totais (sem pagePath)...")
    df_totals = fetch_totals_data(client)
    print(f"    ✓ {len(df_totals):,} linhas de totais")

    if not df_granular.empty:
        print("\n  Resumo:")
        print(f"  · {df_granular['pagina'].nunique():,} páginas únicas")
        print(f"  · {df_granular['cidade'].nunique():,} cidades únicas")
        print(f"  · Pageviews (somáveis, corretos): {df_granular['pageviews'].sum():,}")
        if not df_totals.empty:
            print(f"  · Sessões (sem inflar): {df_totals['sessoes'].sum():,}")
            print(f"  · Usuários únicos (sem inflar): {df_totals['usuarios'].sum():,}")

    print("\nGerando dashboard.html...")
    html = generate_html(df_granular, df_totals)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  ✓ Dashboard salvo em {OUTPUT_PATH.name} ({size_kb:,.0f} KB)")


if __name__ == "__main__":
    main()
