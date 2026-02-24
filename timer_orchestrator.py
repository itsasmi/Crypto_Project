import azure.durable_functions as df
import logging
from datetime import timedelta

bp = df.Blueprint()


@bp.orchestration_trigger(context_name="context")
def timer_orchestrator(context: df.DurableOrchestrationContext):

    input_data = context.get_input() or {}

    trading_pairs    = input_data.get("trading_pairs", [])
    manual_start_ts  = input_data.get("manual_start_timestamp")
    is_regeneration  = input_data.get("is_regeneration", False)

    if not trading_pairs:
        return {"status": "NO_SYMBOLS"}

    instance_id   = context.instance_id
    current_month = context.current_utc_datetime.strftime("%Y-%m")

    if not context.is_replaying:
        logging.info(
            f"[Timer:{instance_id}] 🚀 Starting orchestration for {len(trading_pairs)} symbols"
        )

    # ============================================================
    # 1️⃣ ENSURE SYNAPSE IS RESUMED (STATUS-DRIVEN, NOT DB-DRIVEN)
    # ============================================================
    max_checks = 20  # ~10 minutes (20 × 30s)
    db_ready   = False

    for attempt in range(max_checks):

        status = yield context.call_activity("resume_synapse_activity", None)

        if not context.is_replaying:
            logging.info(
                f"[Timer] 🔄 Synapse status: {status} (attempt {attempt + 1}/{max_checks})"
            )

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

        yield context.create_timer(
            context.current_utc_datetime + timedelta(seconds=30)
        )

    if not db_ready:
        if not context.is_replaying:
            logging.error("[Timer] ❌ Synapse did not become ready within 10 minutes")
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

    results      = []
    all_caught_up = False
    adf_result   = None

    try:
        tasks = []
        for symbol in trading_pairs:
            tasks.append(
                context.call_sub_orchestrator(
                    "symbol_orchestrator",
                    {
                        "trading_pair":          symbol,
                        "manual_start_timestamp": manual_start_ts,
                        "is_regeneration":        is_regeneration
                    }
                )
            )

        results = yield context.task_all(tasks)

        if not context.is_replaying:
            logging.info(f"[Timer] ✅ All {len(trading_pairs)} symbols completed")

    finally:
        # ============================================================
        # 3️⃣ CHECK IF ALL SYMBOLS CAUGHT UP TO CURRENT MONTH
        # ============================================================
        if not context.is_replaying and len(results) > 0:

            logging.info(
                f"[Timer] 🔍 Checking if all symbols are caught up to {current_month}"
            )

            all_caught_up = all(
                isinstance(r, dict)
                and r.get("last_written_timestamp", "")[:7] >= current_month
                for r in results
            )

            if all_caught_up:
                logging.info(
                    f"[Timer] ✅ All {len(trading_pairs)} symbols fully caught up "
                    f"to {current_month}"
                )

                # ============================================================
                # 4️⃣ TRIGGER ADF PIPELINE — raw → silver transformation
                #    Only runs when all symbols are fully caught up
                # ============================================================
                if not context.is_replaying:
                    logging.info("[Timer] 🚀 Triggering ADF pipeline...")

                adf_result = yield context.call_activity(
                    "trigger_adf_pipeline_activity", None
                )

                if not context.is_replaying:
                    logging.info(f"[Timer] 📊 ADF pipeline result: {adf_result}")

                # ============================================================
                # 5️⃣ PAUSE SYNAPSE — only if ADF succeeded
                # ============================================================
                adf_succeeded = (
                    adf_result.startswith("SUCCEEDED") or
                    adf_result.startswith("TRIGGERED")   # simple version fallback
                )

                if adf_succeeded:
                    if not context.is_replaying:
                        logging.info("[Timer] 🔒 ADF succeeded → pausing Synapse...")

                    pause_result = yield context.call_activity(
                        "pause_synapse_activity", None
                    )

                    if not context.is_replaying:
                        logging.info(f"[Timer] 🔒 Synapse pause result: {pause_result}")

                else:
                    # ADF failed — skip pause, leave Synapse running for investigation
                    if not context.is_replaying:
                        logging.warning(
                            f"[Timer] ⚠️ ADF did not succeed ({adf_result}) "
                            f"→ skipping Synapse pause, pool left running for investigation"
                        )

            else:
                not_caught_up = [
                    r.get("symbol", "UNKNOWN")
                    for r in results
                    if isinstance(r, dict)
                    and r.get("last_written_timestamp", "")[:7] < current_month
                ]
                if not context.is_replaying:
                    logging.info(
                        f"[Timer] ⏳ Symbols not yet at {current_month}: {not_caught_up} "
                        f"→ Skipping ADF + Synapse pause"
                    )

    if not context.is_replaying:
        logging.info(
            f"[Timer:{instance_id}] ✅ Orchestration completed successfully"
        )

    return {
        "status":             "SUCCESS",
        "symbols_processed":  len(results),
        "current_month_check": current_month,
        "all_caught_up":      all_caught_up,
        "adf_pipeline":       adf_result,
        "results":            results
    }