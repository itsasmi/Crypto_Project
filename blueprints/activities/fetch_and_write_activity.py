import azure.functions as func
import azure.durable_functions as df
import logging
import requests
import pandas as pd
import io
import os
from datetime import timezone
from azure.storage.blob import BlobServiceClient
from dateutil import parser
from dateutil.relativedelta import relativedelta

bp = df.Blueprint()


@bp.activity_trigger(input_name="params")
def fetch_and_write_activity(params: dict) -> dict:
    logging.info(f"[Fetch] Params received: {params}")

    symbol = params["trading_pair"]

    # ---- PARSE START TIMESTAMP ----
    start_ts = parser.isoparse(params["start_timestamp"])
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=timezone.utc)
    else:
        start_ts = start_ts.astimezone(timezone.utc)

    # ---- MONTH WINDOW ----
    month_start = start_ts.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    next_month = month_start + relativedelta(months=1)

    start_ms = int(month_start.timestamp() * 1000)
    end_ms = int(next_month.timestamp() * 1000)

    logging.info(f"[Fetch:{symbol}] Window {month_start} → {next_month}")

    # ---- FETCH BINANCE DATA ----
    all_klines = []
    current = start_ms

    while current < end_ms:
        resp = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": "1m",
                "startTime": current,
                "endTime": end_ms,
                "limit": 1000
            },
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_klines.extend(data)
        current = data[-1][6] + 1  # close_time + 1 ms

        if len(data) < 1000:
            break

    if not all_klines:
        logging.info(f"[Fetch:{symbol}] No rows fetched")
        return {
            "rows_written": 0,
            "max_timestamp": None
        }

    df_data = pd.DataFrame(all_klines, columns=[
        "open_time","open","high","low","close","volume",
        "close_time","quote_asset_volume","trades",
        "taker_buy_base","taker_buy_quote","ignore"
    ])

    # ---- WRITE TO BLOB ----
    year = month_start.year
    month = f"{month_start.month:02d}"
    blob_path = f"binance/{symbol}/{year}/{month}.csv"

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={os.environ['STORAGE_ACCOUNT_NAME']};"
        f"AccountKey={os.environ['STORAGE_ACCOUNT_KEY']};"
        f"EndpointSuffix=core.windows.net"
    )

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client("raw", blob_path)

    buffer = io.StringIO()
    df_data.to_csv(buffer, index=False)
    blob_client.upload_blob(buffer.getvalue(), overwrite=True)

    # ---- COMPUTE MAX TIMESTAMP WRITTEN ----
    max_close_time_ms = df_data["close_time"].max()
    max_timestamp = (
        pd.to_datetime(max_close_time_ms, unit="ms", utc=True)
        .isoformat()
    )

    logging.info(
        f"[Fetch:{symbol}] ✅ Wrote {len(df_data)} rows → raw/{blob_path}"
    )

    return {
        "rows_written": len(df_data),
        "max_timestamp": max_timestamp
    }
