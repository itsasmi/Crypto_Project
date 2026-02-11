import azure.durable_functions as df
import logging
from function_app import app

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

    # 1️⃣ Decide start timestamp
    if manual_start_ts:
        start_timestamp = manual_start_ts
        tracking_rows = 0
    else:
        tracking = yield context.call_activity(
            "get_last_timestamp_activity",
            symbol
        )

        if not tracking or tracking.get("row_count", 0) == 0:
            start_timestamp = "2021-01-01T00:00:00Z"
            tracking_rows = 0
        else:
            start_timestamp = tracking["last_timestamp"]
            tracking_rows = tracking["row_count"]

    # 2️⃣ Fetch & write (ONE MONTH WINDOW)
    fetch_result = yield context.call_activity(
        "fetch_and_write_activity",
        {
            "trading_pair": symbol,
            "start_timestamp": start_timestamp,
            "is_regeneration": is_regeneration
        }
    )

    rows_added = fetch_result.get("rows_written", 0)
    max_ts_written = fetch_result.get("max_timestamp")

    total_rows = tracking_rows + rows_added

    # 3️⃣ Update tracking ONLY if something was written
    if rows_added > 0 and max_ts_written:
        yield context.call_activity(
            "update_tracking_activity",
            {
                "trading_pair": symbol,
                "last_processed_timestamp": max_ts_written,
                "record_count": total_rows
            }
        )

    if not context.is_replaying:
        logging.info(
            f"[{symbol}] Done | Added={rows_added}, Total={total_rows}"
        )

    return {
        "symbol": symbol,
        "rows_added": rows_added,
        "total_rows": total_rows,
        "last_written_timestamp": max_ts_written
    }