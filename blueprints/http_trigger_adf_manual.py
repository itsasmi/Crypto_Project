import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime, timezone

bp = df.Blueprint()


@bp.route(route="trigger-adf-manual", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_trigger_adf_manual(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    """
    Manual HTTP endpoint to trigger ADF pipeline on demand.
    No ingestion — just fires the ADF pipeline directly.

    POST /api/trigger-adf-manual
    Body: {} (no parameters needed)
    """

    now_utc     = datetime.now(timezone.utc)
    instance_id = f"adf-manual-{now_utc.strftime('%Y%m%d-%H%M%S')}"

    logging.info(
        f"[HTTP-ADF] Manual ADF trigger requested | "
        f"Instance: {instance_id} | "
        f"Time: {now_utc.isoformat()}"
    )

    # Check if already running — avoid duplicate pipeline runs
    existing = await client.get_status(instance_id)
    if existing and existing.runtime_status in [
        df.OrchestrationRuntimeStatus.Running,
        df.OrchestrationRuntimeStatus.Pending
    ]:
        logging.warning(f"[HTTP-ADF] Already running: {instance_id}")
        return func.HttpResponse(
            f"ADF trigger already running. Instance: {instance_id}",
            status_code=409
        )

    # Start the adf_manual_orchestrator
    await client.start_new(
        "adf_manual_orchestrator",
        instance_id=instance_id,
        client_input=None   # ✅ no input needed
    )

    logging.info(f"[HTTP-ADF] ✅ Started adf_manual_orchestrator | Instance: {instance_id}")

    return func.HttpResponse(
        f'{{"status": "STARTED", "instance_id": "{instance_id}"}}',
        status_code=202,
        mimetype="application/json"
    )