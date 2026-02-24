import azure.durable_functions as df
import logging

bp = df.Blueprint()


@bp.orchestration_trigger(context_name="context")
def adf_manual_orchestrator(context: df.DurableOrchestrationContext):
    """
    Manual ADF orchestrator — triggered on demand via HTTP.
    No ingestion involved — just fires the ADF pipeline directly.

    Same logging as the automatic timer path.
    Use this to test the pipeline or trigger transformation manually.
    """

    if not context.is_replaying:
        logging.info(
            f"[ADF-MANUAL] {'='*60}\n"
            f"[ADF-MANUAL] 🚀 MANUAL ADF TRIGGER STARTED\n"
            f"[ADF-MANUAL] Instance  : {context.instance_id}\n"
            f"[ADF-MANUAL] This is a MANUAL trigger — no ingestion involved\n"
            f"[ADF-MANUAL] {'='*60}"
        )

    # ---- TRIGGER ADF PIPELINE — same activity as timer path ----
    adf_result = yield context.call_activity(
        "trigger_adf_pipeline_activity",
        None
    )

    if not context.is_replaying:

        # ✅ Handle all possible return values from trigger_adf_pipeline_activity:
        # New polling version  → SUCCEEDED:{run_id}, FAILED:{reason}, TIMEOUT:{run_id}
        # Old simple version   → TRIGGERED:{run_id}
        # Any error            → ERROR:{message}, FAILED:{code}

        succeeded = (
            adf_result.startswith("SUCCEEDED") or  # polling version ✅
            adf_result.startswith("TRIGGERED")      # simple version ✅
        )

        if succeeded:
            logging.info(
                f"[ADF-MANUAL] {'='*60}\n"
                f"[ADF-MANUAL] ✅ MANUAL TRIGGER COMPLETED SUCCESSFULLY\n"
                f"[ADF-MANUAL] Result    : {adf_result}\n"
                f"[ADF-MANUAL] Instance  : {context.instance_id}\n"
                f"[ADF-MANUAL] {'='*60}"
            )
        else:
            logging.error(
                f"[ADF-MANUAL] {'='*60}\n"
                f"[ADF-MANUAL] ❌ MANUAL TRIGGER DID NOT SUCCEED\n"
                f"[ADF-MANUAL] Result    : {adf_result}\n"
                f"[ADF-MANUAL] Instance  : {context.instance_id}\n"
                f"[ADF-MANUAL] {'='*60}"
            )

    return {
        "trigger_type": "manual",
        "instance_id":  context.instance_id,
        "adf_result":   adf_result
    }