"""
test_ga4.py — Testa a conexão com o Google Analytics 4 (Viva o RS).

Puxa sessões, usuários ativos e pageviews dos últimos 7 dias,
quebrados por data, e imprime numa tabela.
"""

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
CREDENTIALS_PATH = Path(__file__).parent / "credentials" / "ga4-viva-rs.json"


def build_client() -> BetaAnalyticsDataClient:
    """Cria o cliente do GA4 autenticado com a service account."""
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=credentials)


def fetch_last_7_days(client: BetaAnalyticsDataClient) -> pd.DataFrame:
    """Roda o relatório do GA4 e devolve um DataFrame com os dados."""
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name="date")],
        metrics=[
            Metric(name="sessions"),
            Metric(name="activeUsers"),
            Metric(name="screenPageViews"),
        ],
        date_ranges=[DateRange(start_date="7daysAgo", end_date="yesterday")],
    )

    response = client.run_report(request)

    rows = []
    for row in response.rows:
        rows.append(
            {
                "data": row.dimension_values[0].value,
                "sessoes": int(row.metric_values[0].value),
                "usuarios_ativos": int(row.metric_values[1].value),
                "pageviews": int(row.metric_values[2].value),
            }
        )

    df = pd.DataFrame(rows)
    # GA4 devolve a data como "YYYYMMDD" — convertendo para datetime ordenável.
    df["data"] = pd.to_datetime(df["data"], format="%Y%m%d").dt.date
    return df.sort_values("data").reset_index(drop=True)


def main() -> None:
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(
            f"Arquivo de credenciais não encontrado em: {CREDENTIALS_PATH}"
        )

    print(f"Conectando no GA4 — property {PROPERTY_ID}...")
    client = build_client()

    print("Puxando dados dos últimos 7 dias...\n")
    df = fetch_last_7_days(client)

    print("Resultados (últimos 7 dias):")
    print(df.to_string(index=False))

    print("\nTotais do período:")
    totais = df[["sessoes", "usuarios_ativos", "pageviews"]].sum()
    print(totais.to_string())


if __name__ == "__main__":
    main()
