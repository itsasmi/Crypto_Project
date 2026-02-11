import azure.durable_functions as df
import logging
from datetime import timedelta

bp = df.Blueprint()


@bp.orchestration_trigger(context_name="context")
def timer_orchestrator(context: df.DurableOrchestrationContext):

    input_data = context.get_input() or {}

    trading_pairs = input_data.get("trading_pairs", [])
    manual_start_ts = input_data.get("manual_start_timestamp")
    is_regeneration = input_data.get("is_regeneration", False)

    if not trading_pairs:
        return {"status": "NO_SYMBOLS"}

    instance_id = context.instance_id

    if not context.is_replaying:
        logging.info(
            f"[Timer:{instance_id}] 🚀 Starting orchestration for {len(trading_pairs)} symbols"
        )

    # ============================================================
    # 1️⃣ ENSURE SYNAPSE IS RESUMED (STATUS-DRIVEN, NOT DB-DRIVEN)
    # ============================================================
    max_checks = 20  # ~10 minutes (20 × 30s)
    db_ready = False

    for attempt in range(max_checks):

        # 🔁 This activity:
        # - sends RESUME if Paused
        # - returns Resuming / Online
        status = yield context.call_activity("resume_synapse_activity", None)

        if not context.is_replaying:
            logging.info(
                f"[Timer] 🔄 Synapse status: {status} (attempt {attempt + 1}/{max_checks})"
            )

        # Only check DB when pool is ONLINE
        if status == "Online":
            if not context.is_replaying:
                logging.info("[Timer] 🔍 Pool Online → checking DB connectivity")

            is_ready = yield context.call_activity(
                "check_synapse_ready_activity", None
            )

            if is_ready:
                if not context.is_replaying:
                    logging.info(
                        f"[Timer] ✅ Database READY after {attempt + 1} checks"
                    )
                db_ready = True
                break

        # Wait before retry
        yield context.create_timer(
            context.current_utc_datetime + timedelta(seconds=30)
        )

    if not db_ready:
        if not context.is_replaying:
            logging.error(
                "[Timer] ❌ Synapse did not become ready within 10 minutes"
            )
        return {
            "status": "DATABASE_NOT_READY",
            "error": "Synapse pool never reached ONLINE + queryable state"
        }

    # ============================================================
    # 2️⃣ PROCESS ALL SYMBOLS IN PARALLEL
    # ============================================================
    if not context.is_replaying:
        logging.info(
            f"[Timer] 📊 Starting processing for {len(trading_pairs)} symbols"
        )

    try:
        tasks = []
        for symbol in trading_pairs:
            tasks.append(
                context.call_sub_orchestrator(
                    "symbol_orchestrator",
                    {
                        "trading_pair": symbol,
                        "manual_start_timestamp": manual_start_ts,
                        "is_regeneration": is_regeneration
                    }
                )
            )

        results = yield context.task_all(tasks)

    finally:
        # ========================================================
        # 3️⃣ PAUSE SYNAPSE AFTER WORK COMPLETES
        # ========================================================
        if not context.is_replaying:
            logging.info("[Timer] 🔄 Pausing Synapse SQL Pool")

        pause_result = yield context.call_activity(
            "pause_synapse_activity", None
        )

        if not context.is_replaying:
            logging.info(f"[Timer] Synapse pause result: {pause_result}")

    if not context.is_replaying:
        logging.info(
            f"[Timer:{instance_id}] ✅ Orchestration completed successfully"
        )

    return {
        "status": "SUCCESS",
        "symbols_processed": len(results),
        "results": results
    }
