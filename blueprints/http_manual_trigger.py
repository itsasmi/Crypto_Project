# import azure.functions as func
# import azure.durable_functions as df
# import logging

# bp = df.Blueprint()

# @bp.route(route="start-binance-incremental", methods=["POST"])
# @bp.durable_client_input(client_name="client")
# async def http_starter(
#     req: func.HttpRequest,
#     client: df.DurableOrchestrationClient
# ) -> func.HttpResponse:

#     try:
#         body = req.get_json()
#         trading_pair = body.get("trading_pair")
#     except Exception:
#         return func.HttpResponse(
#             'Invalid JSON. Expected {"trading_pair":"BTCUSDT"}',
#             status_code=400
#         )

#     if not trading_pair:
#         return func.HttpResponse(
#             "trading_pair is required",
#             status_code=400
#         )

#     instance_id = f"manual-{trading_pair}"

#     status = await client.get_status(instance_id)

#     # ✅ CASE 1: Already running → return status (NO ERROR)
#     if status and status.runtime_status in ["Running", "Pending"]:
#         logging.warning(
#             f"[HTTP] Orchestration already running: {instance_id}"
#         )
#         return client.create_check_status_response(req, instance_id)

#     # ✅ CASE 2: Failed / Terminated → clean restart
#     if status and status.runtime_status in ["Failed", "Terminated"]:
#         logging.warning(
#             f"[HTTP] Cleaning up old instance {instance_id}"
#         )
#         await client.terminate(instance_id, "Restart requested")

#     # ✅ CASE 3: Safe to start
#     logging.info(f"[HTTP] Starting orchestration {instance_id}")

#     await client.start_new(
#         "hybrid_orchestrator",
#         instance_id,
#         {"trading_pair": trading_pair}
#     )

#     return client.create_check_status_response(req, instance_id)


# @bp.route(route="terminate-binance-incremental", methods=["POST"])
# @bp.durable_client_input(client_name="client")
# async def http_terminate(
#     req: func.HttpRequest,
#     client: df.DurableOrchestrationClient
# ) -> func.HttpResponse:
#     """
#     Terminates a running Binance ingestion orchestration.
#     Body: { "trading_pair": "BTCUSDT" }
#     """

#     try:
#         body = req.get_json()
#         trading_pair = body.get("trading_pair")
#     except Exception:
#         return func.HttpResponse(
#             'Invalid JSON. Expected {"trading_pair":"BTCUSDT"}',
#             status_code=400
#         )

#     if not trading_pair:
#         return func.HttpResponse(
#             "trading_pair is required",
#             status_code=400
#         )

#     instance_id = f"manual-{trading_pair}"

#     status = await client.get_status(instance_id)

#     if not status:
#         return func.HttpResponse(
#             f"No orchestration found for {instance_id}",
#             status_code=404
#         )

#     if status.runtime_status in ["Completed", "Failed", "Terminated"]:
#         return func.HttpResponse(
#             f"Orchestration already {status.runtime_status}",
#             status_code=409
#         )

#     logging.warning(f"[HTTP] Terminating orchestration {instance_id}")

#     await client.terminate(
#         instance_id,
#         "Manual termination requested"
#     )

#     return func.HttpResponse(
#         f"Orchestration {instance_id} terminated successfully",
#         status_code=200
#     )
# @bp.route(route="test-adf-trigger", methods=["POST"])
# @bp.durable_client_input(client_name="client")
# async def http_test_adf(
#     req: func.HttpRequest,
#     client: df.DurableOrchestrationClient
# ) -> func.HttpResponse:

#     instance_id = "test-adf-trigger"

#     await client.start_new(
#         "test_adf_orchestrator",
#         instance_id,
#         None
#     )

#     logging.info("[ADF-TEST] Started test orchestration")
#     return client.create_check_status_response(req, instance_id)

import azure.functions as func
import azure.durable_functions as df
import logging

bp = df.Blueprint()

@bp.route(route="start-binance-incremental", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_starter(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
) -> func.HttpResponse:

    try:
        body = req.get_json()
        trading_pair = body.get("trading_pair")
    except Exception:
        return func.HttpResponse(
            'Invalid JSON. Expected {"trading_pair":"BTCUSDT"}',
            status_code=400
        )

    if not trading_pair:
        return func.HttpResponse(
            "trading_pair is required",
            status_code=400
        )

    instance_id = f"manual-{trading_pair}"

    status = await client.get_status(instance_id)

    # ✅ CASE 1: Already running → return status (NO ERROR)
    if status and status.runtime_status in ["Running", "Pending"]:
        logging.warning(
            f"[HTTP] Orchestration already running: {instance_id}"
        )
        return client.create_check_status_response(req, instance_id)

    # ✅ CASE 2: Any existing instance → terminate before restarting
    # (Covers Failed, Terminated, Completed AND any unexpected state)
    if status:
        logging.warning(
            f"[HTTP] Existing instance found with status "
            f"'{status.runtime_status}', terminating before restart: {instance_id}"
        )
        try:
            await client.terminate(instance_id, "Restart requested")
        except Exception as e:
            logging.warning(f"[HTTP] Terminate warning (non-fatal): {str(e)}")

    # ✅ CASE 3: Safe to start — wrap in try/except as final safety net
    logging.info(f"[HTTP] Starting orchestration {instance_id}")

    try:
        await client.start_new(
            "hybrid_orchestrator",
            instance_id,
            {"trading_pair": trading_pair}
        )
    except Exception as e:
        if "OrchestrationAlreadyExistsException" in str(e):
            logging.warning(
                f"[HTTP] Race condition caught — instance already exists: {instance_id}"
            )
            return client.create_check_status_response(req, instance_id)
        raise

    return client.create_check_status_response(req, instance_id)


@bp.route(route="terminate-binance-incremental", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_terminate(
    req: func.HttpRequest,
    client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    """
    Terminates a running Binance ingestion orchestration.
    Body: { "trading_pair": "BTCUSDT" }
    """

    try:
        body = req.get_json()
        trading_pair = body.get("trading_pair")
    except Exception:
        return func.HttpResponse(
            'Invalid JSON. Expected {"trading_pair":"BTCUSDT"}',
            status_code=400
        )

    if not trading_pair:
        return func.HttpResponse(
            "trading_pair is required",
            status_code=400
        )

    instance_id = f"manual-{trading_pair}"

    status = await client.get_status(instance_id)

    if not status:
        return func.HttpResponse(
            f"No orchestration found for {instance_id}",
            status_code=404
        )

    if status.runtime_status in ["Completed", "Failed", "Terminated"]:
        return func.HttpResponse(
            f"Orchestration already {status.runtime_status}",
            status_code=409
        )

    logging.warning(f"[HTTP] Terminating orchestration {instance_id}")

    await client.terminate(
        instance_id,
        "Manual termination requested"
    )

    return func.HttpResponse(
        f"Orchestration {instance_id} terminated successfully",
        status_code=200
    )