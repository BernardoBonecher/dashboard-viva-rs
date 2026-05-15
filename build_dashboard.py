"""
build_dashboard.py — Puxa dados do GA4 (Viva o RS) e gera dashboard.html.

Como rodar:
    py build_dashboard.py

O que ele faz:
1. Conecta no GA4 com a service account.
2. Faz queries paginadas com dimensões: data, página, título, cidade, origem.
3. Compacta os dados (índices em vez de strings repetidas) e injeta num HTML.
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
# No GitHub Actions, o JSON é gravado a partir de um Secret (ver workflow).
# Localmente, usa o arquivo da pasta credentials/.
CREDENTIALS_PATH = Path(
    os.environ.get("GA4_CREDENTIALS_PATH")
    or Path(__file__).parent / "credentials" / "ga4-viva-rs.json"
)
TEMPLATE_PATH = Path(__file__).parent / "dashboard_template.html"
OUTPUT_PATH = Path(__file__).parent / "dashboard.html"

# Período: pode usar "2025-01-01", "30daysAgo", "yesterday" etc.
START_DATE = "2025-01-01"
END_DATE = "yesterday"

# GA4 retorna no máximo 100k linhas por chamada; paginamos com offset.
PAGE_SIZE = 100000


def build_client() -> BetaAnalyticsDataClient:
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def fetch_granular_data(client: BetaAnalyticsDataClient) -> pd.DataFrame:
    """Paginada: pede em blocos de PAGE_SIZE até cobrir row_count."""
    all_rows = []
    offset = 0

    while True:
        request = RunReportRequest(
            property=f"properties/{PROPERTY_ID}",
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
            date_ranges=[DateRange(start_date=START_DATE, end_date=END_DATE)],
            offset=offset,
            limit=PAGE_SIZE,
        )
        response = client.run_report(request)

        for row in response.rows:
            all_rows.append(
                {
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
                }
            )

        total = response.row_count
        fetched = offset + len(response.rows)
        print(f"  ... {fetched:,}/{total:,} linhas")

        if fetched >= total or len(response.rows) == 0:
            break
        offset = fetched

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df["data"] = pd.to_datetime(df["data"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    return df


def build_compact_payload(df: pd.DataFrame) -> dict:
    """Reduz o tamanho do JSON usando índices em vez de strings repetidas.

    Em vez de cada linha repetir "Porto Alegre", "google / organic" etc.,
    guardamos as listas únicas uma vez e referenciamos por inteiro.
    """
    if df.empty:
        return {"paginas": [], "titulos": [], "cidades": [], "origens": [], "rows": []}

    paginas = sorted(df["pagina"].unique().tolist())
    cidades = sorted(df["cidade"].unique().tolist())
    origens = sorted(df["origem"].unique().tolist())

    # Um título por página (o primeiro não-vazio que aparecer).
    titulo_serie = df[df["titulo"] != ""].groupby("pagina")["titulo"].first()
    titulo_map = titulo_serie.to_dict()
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


def generate_html(df: pd.DataFrame) -> str:
    template = TEMPLATE_PATH.read_text(encoding="utf-8")

    payload = build_compact_payload(df)
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

    start_date = df["data"].min() if not df.empty else "—"
    end_date = df["data"].max() if not df.empty else "—"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    return (
        template
        .replace("__DATA_JSON__", data_json)
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

    print(f"Puxando dados granulares ({START_DATE} → {END_DATE})...")
    df = fetch_granular_data(client)
    print(f"  ✓ {len(df):,} linhas brutas")

    if not df.empty:
        print(f"  · {df['pagina'].nunique():,} páginas únicas")
        print(f"  · {df['cidade'].nunique():,} cidades únicas")
        print(f"  · {df['origem'].nunique():,} origens únicas")
        print(f"  · Total: {df['sessoes'].sum():,} sessões, {df['pageviews'].sum():,} pageviews")

    print("Gerando dashboard.html...")
    html = generate_html(df)
    OUTPUT_PATH.write_text(html, encoding="utf-8")

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  ✓ Dashboard salvo em {OUTPUT_PATH.name} ({size_kb:,.0f} KB)")


if __name__ == "__main__":
    main()
