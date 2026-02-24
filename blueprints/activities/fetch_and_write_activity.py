import azure.functions as func
import azure.durable_functions as df
import logging
import requests
import pandas as pd
import io
import os
from datetime import timezone, datetime
from azure.storage.blob import BlobServiceClient
from dateutil import parser
from dateutil.relativedelta import relativedelta

bp = df.Blueprint()

# ---- BINANCE FALLBACK ENDPOINTS ----
BINANCE_ENDPOINTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]

def _fetch_klines(params: dict) -> list:
    for base_url in BINANCE_ENDPOINTS:
        try:
            resp = requests.get(
                f"{base_url}/api/v3/klines",
                params=params,
                timeout=30
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logging.warning(f"[Fetch] {base_url} failed: {str(e)}, trying next...")
    raise ConnectionError("All Binance endpoints failed — region may be blocked")


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

    # ✅ PARSE fetch_from — only present for current month / incomplete month re-runs
    # Used to count only NEW rows since last run (not all rows in blob)
    fetch_from_raw = params.get("fetch_from")
    if fetch_from_raw:
        fetch_from = parser.isoparse(fetch_from_raw)
        if fetch_from.tzinfo is None:
            fetch_from = fetch_from.replace(tzinfo=timezone.utc)
        fetch_from_ms = int(fetch_from.timestamp() * 1000)
    else:
        fetch_from = None
        fetch_from_ms = None

    # ---- MONTH WINDOW (FIXED) ----
    # If stored timestamp is NOT first day of month (i.e. it's an end-of-month
    # close_time like 2022-08-31 23:59:59), advance to NEXT month to avoid
    # re-processing the same month again
    if start_ts.day != 1:
        month_start = (
            start_ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            + relativedelta(months=1)
        )
    else:
        month_start = start_ts.replace(hour=0, minute=0, second=0, microsecond=0)

    next_month = month_start + relativedelta(months=1)

    now_utc = datetime.now(timezone.utc)

    # ✅ is_current_month flag — used to decide what max_timestamp to store
    is_current_month = next_month > now_utc

    if is_current_month:
        next_month = now_utc

    start_ms = int(month_start.timestamp() * 1000)
    end_ms = int(next_month.timestamp() * 1000)

    logging.info(f"[Fetch:{symbol}] Window {month_start} → {next_month}")
    if fetch_from:
        logging.info(f"[Fetch:{symbol}] Counting new rows only after {fetch_from}")

    # ---- FETCH BINANCE DATA ----
    all_klines = []
    current = start_ms

    while current < end_ms:
        data = _fetch_klines({
            "symbol": symbol,
            "interval": "1m",
            "startTime": current,
            "endTime": end_ms,
            "limit": 1000
        })

        if not data:
            break

        all_klines.extend(data)
        current = data[-1][6] + 1

        if len(data) < 1000:
            break

    if not all_klines:
        logging.info(f"[Fetch:{symbol}] No rows fetched")
        return {
            "rows_written": 0,
            "new_rows": 0,
            "max_timestamp": month_start.isoformat()
        }

    # ---- REMOVE LAST FIELD (ignore) ----
    trimmed_klines = [row[:11] for row in all_klines]

    df_data = pd.DataFrame(trimmed_klines, columns=[
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume"
    ])

    # ✅ ADD TRADING PAIR COLUMN
    df_data.insert(0, "trading_pair", symbol)

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

    # ---- COMPUTE MAX TIMESTAMP ----
    # ✅ KEY FIX: For COMPLETED past months → store 1st of NEXT month as max_timestamp
    # This ensures orchestrator reads day=1 next time → CASE 3 (clean boundary)
    # → advances to next month correctly without re-fetching same month
    #
    # For CURRENT month → store actual last candle time
    # This ensures orchestrator reads mid-month time next time → CASE 1 (current month re-run)
    # → re-fetches from 1st and counts only new rows
    if is_current_month:
        # Current month — store actual last candle timestamp
        max_close_time_ms = df_data["close_time"].max()
        max_timestamp = (
            pd.to_datetime(max_close_time_ms, unit="ms", utc=True)
            .isoformat()
        )
        logging.info(f"[Fetch:{symbol}] Current month → storing actual last candle: {max_timestamp}")
    else:
        # Completed past month → store 1st of next month so orchestrator advances cleanly
        max_timestamp = next_month.isoformat()
        logging.info(f"[Fetch:{symbol}] Completed past month → storing next month start: {max_timestamp}")

    # ✅ COUNT ONLY NEW ROWS SINCE LAST RUN
    # Incomplete/current month re-run → only rows after fetch_from_ms
    # Fresh month fetch (fetch_from=None) → all rows as normal
    if fetch_from_ms is not None:
        new_rows = int((df_data["close_time"] > fetch_from_ms).sum())
    else:
        new_rows = len(df_data)

    logging.info(
        f"[Fetch:{symbol}] ✅ Wrote {len(df_data)} total rows → raw/{blob_path} "
        f"| New rows since last run: {new_rows}"
    )

    return {
        "rows_written": len(df_data),  # total rows in blob
        "new_rows": new_rows,           # new rows since last run
        "max_timestamp": max_timestamp  # next month 1st for past months, actual time for current
    }