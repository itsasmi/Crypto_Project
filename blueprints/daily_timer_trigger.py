import os
import azure.functions as func
import azure.durable_functions as df
import logging

bp = df.Blueprint()

# TRADING_PAIRS = ["BTCUSDT", "ETHUSDT", "LINKUSDT", "SOLUSDT", "BNBUSDT"]
TRADING_PAIRS = []
INSTANCE_ID = "daily-timer-orchestrator"


@bp.timer_trigger(
    schedule="0 40 12 * * *",
    arg_name="timer",
    run_on_startup=False
)

@bp.durable_client_input(client_name="client")
async def daily_timer_trigger(
    timer: func.TimerRequest,
    client: df.DurableOrchestrationClient
):
    if timer.past_due:
        logging.warning("⏰ Timer running late")

    # 1️⃣ Check existing orchestration
    status = await client.get_status(INSTANCE_ID)

    if status and status.runtime_status in ["Running", "Pending"]:
        logging.warning(
            f"⏳ Orchestrator '{INSTANCE_ID}' already {status.runtime_status}. Skipping."
        )
        return

    # 2️⃣ Start orchestration
    try:
        await client.start_new(
            "timer_orchestrator",
            INSTANCE_ID,
            {
                "trading_pairs": TRADING_PAIRS
            }
        )
        logging.info("🚀 Timer orchestrator started")

    except Exception as e:
        if "OrchestrationAlreadyExistsException" in str(e):
            logging.warning("⏳ Orchestrator instance already exists. Skipping.")
        else:
            raise
