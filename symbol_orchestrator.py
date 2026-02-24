import azure.durable_functions as df
import logging
from datetime import datetime, timezone
from dateutil import parser
from dateutil.relativedelta import relativedelta

bp = df.Blueprint()

@bp.orchestration_trigger(context_name="context")
def symbol_orchestrator(context: df.DurableOrchestrationContext):

    input_data = context.get_input() or {}

    symbol = input_data.get("trading_pair")
    manual_start_ts = input_data.get("manual_start_timestamp")
    is_regeneration = input_data.get("is_regeneration", False)

    if not symbol:
        raise ValueError("trading_pair missing")

    if not context.is_replaying:
        logging.info(f"[{symbol}] Orchestration started")

    now_utc = context.current_utc_datetime.replace(tzinfo=timezone.utc)
    current_month_start = now_utc.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )

    # ✅ CASE 0: Manual start timestamp provided
    if manual_start_ts:
        manual_dt = parser.isoparse(manual_start_ts)
        if manual_dt.tzinfo is None:
            manual_dt = manual_dt.replace(tzinfo=timezone.utc)

        manual_month_start = manual_dt.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        tracking = yield context.call_activity("get_last_timestamp_activity", symbol)

        if tracking and tracking["last_timestamp"]:
            last_processed_dt = parser.isoparse(tracking["last_timestamp"])
            if last_processed_dt.tzinfo is None:
                last_processed_dt = last_processed_dt.replace(tzinfo=timezone.utc)

            tracking_month = last_processed_dt.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )

            if tracking_month == manual_month_start:
                start_timestamp    = manual_month_start.isoformat()
                fetch_from         = tracking["last_timestamp"]
                existing_month_rows = 0        # ✅ never subtract cumulative total
                tracking_rows      = 0
                if not context.is_replaying:
                    logging.info(f"[{symbol}] Manual re-run on existing month → counting new rows after {fetch_from}")
            else:
                start_timestamp    = manual_month_start.isoformat()
                fetch_from         = None
                existing_month_rows = 0
                tracking_rows      = 0
                if not context.is_replaying:
                    logging.info(f"[{symbol}] Manual start → fresh fetch from {start_timestamp}")
        else:
            start_timestamp    = manual_month_start.isoformat()
            fetch_from         = None
            existing_month_rows = 0
            tracking_rows      = 0
            if not context.is_replaying:
                logging.info(f"[{symbol}] Manual start → no tracking, fresh fetch from {start_timestamp}")

    else:
        tracking = yield context.call_activity("get_last_timestamp_activity", symbol)

        if not tracking or tracking.get("row_count", 0) == 0:
            start_timestamp     = "2021-01-01T00:00:00Z"
            tracking_rows       = 0
            fetch_from          = None
            existing_month_rows = 0
            if not context.is_replaying:
                logging.info(f"[{symbol}] Fresh start — no tracking record found. Starting from 2021-01-01")
        else:
            last_ts_str    = tracking["last_timestamp"]
            tracking_rows  = tracking["row_count"]

            last_processed_dt = parser.isoparse(last_ts_str)
            if last_processed_dt.tzinfo is None:
                last_processed_dt = last_processed_dt.replace(tzinfo=timezone.utc)

            if not context.is_replaying:
                logging.info(f"[{symbol}] Last timestamp from DB: {last_processed_dt} | Cumulative rows: {tracking_rows}")

            # ✅ CASE 1: Current month
            if last_processed_dt >= current_month_start:
                start_timestamp     = current_month_start.isoformat()
                fetch_from          = last_ts_str
                existing_month_rows = 0
                if not context.is_replaying:
                    logging.info(f"[{symbol}] CASE 1 — Current month re-run → blob from {start_timestamp}, counting new rows after {fetch_from}")

            # ✅ CASE 2: Mid-month in past (incomplete)
            elif last_processed_dt.day != 1:
                start_timestamp     = last_processed_dt.replace(
                    day=1, hour=0, minute=0, second=0, microsecond=0
                ).isoformat()
                fetch_from          = last_ts_str
                existing_month_rows = 0
                if not context.is_replaying:
                    logging.info(f"[{symbol}] CASE 2 — Incomplete past month → re-fetching from {start_timestamp}, counting new rows after {fetch_from}")

            # ✅ CASE 3: Clean boundary (day == 1) — most common daily timer path
            else:
                start_timestamp     = last_processed_dt.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ).isoformat()
                fetch_from          = None
                existing_month_rows = 0
                if not context.is_replaying:
                    logging.info(f"[{symbol}] CASE 3 — Clean boundary → fetching full month from {start_timestamp}")

    total_rows_added = 0
    last_ts_written  = start_timestamp

    # 2️⃣ Loop month by month
    while True:

        loop_month_start = parser.isoparse(last_ts_written).replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        if loop_month_start.tzinfo is None:
            loop_month_start = loop_month_start.replace(tzinfo=timezone.utc)

        # Stop if past current month
        if loop_month_start > current_month_start:
            if not context.is_replaying:
                logging.info(f"[{symbol}] ✅ Fully caught up, stopping")
            break

        fetch_result = yield context.call_activity(
            "fetch_and_write_activity",
            {
                "trading_pair":    symbol,
                "start_timestamp": last_ts_written,
                "is_regeneration": is_regeneration,
                "fetch_from":      fetch_from
            }
        )

        rows_added     = fetch_result.get("new_rows", 0)
        rows_written   = fetch_result.get("rows_written", 0)
        max_ts_written = fetch_result.get("max_timestamp")
        total_rows_added += rows_added

        # 3️⃣ Update tracking — new_rows=0 is valid
        if rows_written > 0 and max_ts_written:
            yield context.call_activity(
                "update_tracking_activity",
                {
                    "trading_pair":             symbol,
                    "last_processed_timestamp": max_ts_written,
                    "record_count":             rows_added   # ✅ pure += no subtract
                }
            )
            last_ts_written = max_ts_written

        if not context.is_replaying:
            logging.info(
                f"[{symbol}] Month done | "
                f"new_rows={rows_added} | total_in_blob={rows_written} | "
                f"up_to={max_ts_written}"
            )

        # Stop if Binance returned nothing
        if rows_written == 0:
            if not context.is_replaying:
                logging.info(f"[{symbol}] ⚠️ No rows fetched from Binance, stopping")
            break

        # ✅ Stop after current month — timer re-triggers tomorrow
        if loop_month_start >= current_month_start:
            if not context.is_replaying:
                logging.info(f"[{symbol}] ✅ Current month processed. Done until next timer run.")
            break

        # Reset after first iteration
        fetch_from          = None
        existing_month_rows = 0

        # Advance to next month
        max_dt = parser.isoparse(max_ts_written)
        if max_dt.tzinfo is None:
            max_dt = max_dt.replace(tzinfo=timezone.utc)

        if max_dt.day == 1 and max_dt.hour == 0 and max_dt.minute == 0:
            last_ts_written = max_ts_written
        else:
            next_month = (
                max_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                + relativedelta(months=1)
            )
            last_ts_written = next_month.isoformat()

    if not context.is_replaying:
        logging.info(f"[{symbol}] Done | Total new rows added={total_rows_added}")

    return {
        "symbol":                symbol,
        "rows_added":            total_rows_added,
        "total_rows":            total_rows_added,
        "last_written_timestamp": last_ts_written
    }