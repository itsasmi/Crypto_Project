import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime, timezone
from dateutil import parser
from dateutil.relativedelta import relativedelta

bp = df.Blueprint()

@bp.orchestration_trigger(context_name="context")
def hybrid_orchestrator(context: df.DurableOrchestrationContext):
    input_data = context.get_input() or {}
    trading_pair = input_data.get("trading_pair")
    manual_start_ts = input_data.get("manual_start_timestamp")

    if not trading_pair:
        raise ValueError("trading_pair missing")

    if not context.is_replaying:
        logging.info(f"🔥 HYBRID ORCHESTRATOR STARTED for {trading_pair} 🔥")

    # 1️⃣ Read tracking table
    tracking = yield context.call_activity(
        "get_last_timestamp_activity",
        trading_pair
    )

    # Default start date
    if not tracking["last_timestamp"]:
        last_processed_dt = datetime(2021, 1, 1, tzinfo=timezone.utc)
    else:
        last_processed_dt = parser.isoparse(tracking["last_timestamp"])
        if last_processed_dt.tzinfo is None:
            last_processed_dt = last_processed_dt.replace(tzinfo=timezone.utc)

    if not context.is_replaying:
        logging.info(f"[{trading_pair}] Last timestamp from DB: {last_processed_dt}")

    now_utc = context.current_utc_datetime.replace(tzinfo=timezone.utc)
    current_month_start = now_utc.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )

    # ✅ CASE 0: Manual start timestamp provided
    # → snap to 1st of that month
    # → check if tracking already has data for that same month to avoid doubling
    if manual_start_ts:
        manual_dt = parser.isoparse(manual_start_ts)
        if manual_dt.tzinfo is None:
            manual_dt = manual_dt.replace(tzinfo=timezone.utc)

        # Always snap to 1st of the given month e.g. 24/6/24 → 2024-06-01
        month_start = manual_dt.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        tracking_month = last_processed_dt.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )

        if tracking["last_timestamp"] and tracking_month == month_start:
            # ✅ Same month already partially loaded
            # → DO NOT subtract — tracking rows is cumulative total not just this month
            # → fetch_from ensures only new rows counted, just add on top
            fetch_from = last_processed_dt
            existing_month_rows = 0             # ✅ never subtract cumulative total
            if not context.is_replaying:
                logging.info(
                    f"[{trading_pair}] CASE 0 — Manual re-run on existing month → "
                    f"counting new rows after {fetch_from}, "
                    f"adding new rows on top of cumulative total"
                )
        else:
            # ✅ Different month or no tracking → fresh fetch, normal accumulation
            fetch_from = None
            existing_month_rows = 0
            if not context.is_replaying:
                logging.info(
                    f"[{trading_pair}] CASE 0 — Manual start → fresh fetch from {month_start}"
                )

    # ✅ CASE 1: Last run was in CURRENT MONTH
    # → re-fetch blob from 1st but only COUNT new rows since last run
    # → DO NOT subtract — tracking rows is cumulative total not just this month
    elif last_processed_dt >= current_month_start:
        month_start = current_month_start
        fetch_from = last_processed_dt
        existing_month_rows = 0             # ✅ never subtract cumulative total
        if not context.is_replaying:
            logging.info(
                f"[{trading_pair}] CASE 1 — Current month re-run → "
                f"blob from {month_start}, counting new rows after {fetch_from}"
            )

    # ✅ CASE 2: last_processed_dt is mid-month (day != 1) in a PAST month
    # → that month was NOT fully fetched (e.g. paused mid-month)
    # → DO NOT subtract — tracking_rows is cumulative total, not just this month
    elif last_processed_dt.day != 1:
        month_start = last_processed_dt.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        fetch_from = last_processed_dt
        existing_month_rows = 0             # ✅ never subtract cumulative total
        if not context.is_replaying:
            logging.info(
                f"[{trading_pair}] CASE 2 — Incomplete past month → "
                f"re-fetching from {month_start}, counting new rows after {fetch_from}"
            )

    # ✅ CASE 3: Clean month boundary (day == 1)
    # → fetch_and_write stored next month's 1st after completing a past month
    # → that month was fully completed, start from this month, normal += accumulation
    else:
        month_start = last_processed_dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        fetch_from = None
        existing_month_rows = 0
        if not context.is_replaying:
            logging.info(
                f"[{trading_pair}] CASE 3 — Clean boundary → "
                f"fetching full month from {month_start}"
            )

    next_month = month_start + relativedelta(months=1)

    # 3️⃣ Stop if fully caught up (> not >=)
    if month_start > current_month_start:
        if not context.is_replaying:
            logging.info(f"[{trading_pair}] ✅ Data is up to date. Stopping ingestion.")
        return "DONE"

    # 4️⃣ Fetch + write
    fetch_result = yield context.call_activity(
        "fetch_and_write_activity",
        {
            "trading_pair":    trading_pair,
            "start_timestamp": month_start.isoformat(),
            "end_timestamp":   next_month.isoformat(),
            "fetch_from":      fetch_from.isoformat() if fetch_from else None
        }
    )

    # 5️⃣ Update tracking always — new_rows can be 0 and that is valid
    yield context.call_activity(
        "update_tracking_activity",
        {
            "trading_pair":             trading_pair,
            "last_processed_timestamp": fetch_result["max_timestamp"],
            "record_count":             fetch_result["new_rows"]  # pure += always
        }
    )

    if not context.is_replaying:
        logging.info(
            f"[{trading_pair}] Month completed: {month_start:%Y-%m} | "
            f"new_rows={fetch_result['new_rows']} | "
            f"total_in_blob={fetch_result['rows_written']}"
        )

    # ✅ If we just processed current month → return DONE, do not continue_as_new
    # Prevents infinite loop — timer re-triggers tomorrow for new rows
    if month_start >= current_month_start:
        if not context.is_replaying:
            logging.info(f"[{trading_pair}] ✅ Current month processed. Done until next timer run.")
        return "DONE"

    # 🔁 Only continue if there are MORE past months to catch up on
    if not context.is_replaying:
        logging.info(f"[{trading_pair}] 🔁 Continuing to next month: {next_month:%Y-%m}")
    context.continue_as_new({
        "trading_pair":          trading_pair,
        "manual_start_timestamp": None
    })