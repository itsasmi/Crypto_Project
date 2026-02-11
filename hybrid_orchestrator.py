import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime, timezone
from dateutil import parser
from dateutil.relativedelta import relativedelta

bp = df.Blueprint()

@bp.orchestration_trigger(context_name="context")
def hybrid_orchestrator(context: df.DurableOrchestrationContext):

    trading_pair = context.get_input().get("trading_pair")
    if not trading_pair:
        raise ValueError("trading_pair missing")

    logging.info(f"🔥 HYBRID ORCHESTRATOR STARTED for {trading_pair} 🔥")

    # 1️⃣ Read tracking table
    tracking = yield context.call_activity(
        "get_last_timestamp_activity",
        trading_pair
    )

    last_processed_dt = parser.isoparse(tracking["last_timestamp"])
    logging.info(f"[{trading_pair}] Last timestamp from DB: {last_processed_dt}")

    # 2️⃣ Compute month window
    month_start = last_processed_dt.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    )
    next_month = month_start + relativedelta(months=1)

    now_utc = context.current_utc_datetime.replace(tzinfo=timezone.utc)

    # 3️⃣ If fully caught up → stop (ONLY place pause is allowed)
    if month_start >= now_utc.replace(day=1):
        logging.info(f"[{trading_pair}] Data is up to date. Stopping ingestion.")
        yield context.call_activity("pause_synapse_activity", None)
        return "DONE"

    # 4️⃣ Fetch + write (incremental logic UNTOUCHED)
    rows_written = yield context.call_activity(
        "fetch_and_write_activity",
        {
            "trading_pair": trading_pair,
            "start_timestamp": month_start.isoformat(),
            "end_timestamp": next_month.isoformat()
        }
    )

    # 5️⃣ Update tracking (UNTOUCHED)
    yield context.call_activity(
        "update_tracking_activity",
        {
            "trading_pair": trading_pair,
            "last_processed_timestamp": next_month.isoformat(),
            "record_count": rows_written
        }
    )

    logging.info(f"[{trading_pair}] Month completed: {month_start:%Y-%m}")

# 🔁 Continue orchestration for next month
    context.continue_as_new({
        "trading_pair": trading_pair
    })

