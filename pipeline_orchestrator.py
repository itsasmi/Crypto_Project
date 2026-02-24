import azure.durable_functions as df
import logging

bp = df.Blueprint()

# ---- ALL 5 TRADING PAIRS ----
TRADING_PAIRS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT"
]


@bp.orchestration_trigger(context_name="context")
def pipeline_orchestrator(context: df.DurableOrchestrationContext):
    """
    Master orchestrator (TIMER PATH only):
    1. Fans out to all 5 trading pairs in PARALLEL
    2. Waits for ALL 5 to complete (fan-in)
    3. Triggers ADF pipeline once all symbols done
    4. Pauses Synapse SQL pool after ADF succeeds ✅
    """

    if not context.is_replaying:
        logging.info("🚀 PIPELINE ORCHESTRATOR STARTED")
        logging.info(f"📊 Processing {len(TRADING_PAIRS)} symbols: {TRADING_PAIRS}")

    # ---- 1️⃣ FAN OUT — run all 5 symbols in PARALLEL ----
    symbol_tasks = [
        context.call_sub_orchestrator(
            "symbol_orchestrator",
            {
                "trading_pair": symbol,
                "is_regeneration": False
            },
            instance_id=f"{context.instance_id}-{symbol}"
        )
        for symbol in TRADING_PAIRS
    ]

    # ---- 2️⃣ FAN IN — wait for ALL 5 to complete ----
    if not context.is_replaying:
        logging.info("⏳ Waiting for all 5 symbols to complete...")

    results = yield context.task_all(symbol_tasks)

    # ---- 3️⃣ LOG SYMBOL RESULTS ----
    if not context.is_replaying:
        for symbol, result in zip(TRADING_PAIRS, results):
            logging.info(f"[{symbol}] Result: {result}")
        logging.info("✅ All 5 symbols completed — triggering ADF pipeline")

    # ---- 4️⃣ TRIGGER ADF PIPELINE ----
    adf_result = yield context.call_activity(
        "trigger_adf_pipeline_activity",
        None
    )

    if not context.is_replaying:
        logging.info(f"[ADF] Final result: {adf_result}")

    # ---- 5️⃣ PAUSE SYNAPSE — only if ADF succeeded ----
    # ✅ TIMER PATH ONLY — manual ADF trigger does NOT pause Synapse
    # Only pause when ADF actually completed successfully
    synapse_result = None

    adf_succeeded = (
        adf_result.startswith("SUCCEEDED") or
        adf_result.startswith("TRIGGERED")   # simple version fallback
    )

    if adf_succeeded:
        if not context.is_replaying:
            logging.info(
                f"[SYNAPSE] ADF succeeded → pausing Synapse SQL pool..."
            )

        synapse_result = yield context.call_activity(
            "pause_synapse_activity",
            None
        )

        if not context.is_replaying:
            logging.info(f"[SYNAPSE] Pause result: {synapse_result}")

    else:
        # ADF failed/timed out — do NOT pause Synapse
        # Pool may still be needed for investigation
        if not context.is_replaying:
            logging.warning(
                f"[SYNAPSE] ⚠️  ADF did not succeed ({adf_result}) "
                f"— skipping Synapse pause"
            )
        synapse_result = "SKIPPED:adf_not_succeeded"

    # ---- 6️⃣ FINAL SUMMARY ----
    if not context.is_replaying:
        logging.info(
            f"[PIPELINE] {'='*60}\n"
            f"[PIPELINE] ✅ PIPELINE ORCHESTRATOR COMPLETE\n"
            f"[PIPELINE] Symbols   : all 5 done\n"
            f"[PIPELINE] ADF       : {adf_result}\n"
            f"[PIPELINE] Synapse   : {synapse_result}\n"
            f"[PIPELINE] {'='*60}"
        )

    return {
        "symbols": {
            symbol: str(result)
            for symbol, result in zip(TRADING_PAIRS, results)
        },
        "adf_pipeline":    adf_result,
        "synapse_pause":   synapse_result
    }