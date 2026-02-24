import azure.durable_functions as df
import logging
import os
import requests
import time
from datetime import datetime, timezone
from azure.identity import DefaultAzureCredential

bp = df.Blueprint()

# ---- STRUCTURED LOGGER ----
# All logs go to Application Insights automatically via Azure Functions runtime
# Use consistent prefixes so logs are easy to filter/query in Log Analytics
logger = logging.getLogger("adf_pipeline")


def _get_adf_headers():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def _get_base_url():
    sub      = os.environ["AZURE_SUBSCRIPTION_ID"]
    rg       = os.environ["AZURE_RESOURCE_GROUP"]
    factory  = os.environ["ADF_FACTORY_NAME"]
    pipeline = os.environ["ADF_PIPELINE_NAME"]

    return (
        f"https://management.azure.com/subscriptions/{sub}"
        f"/resourceGroups/{rg}"
        f"/providers/Microsoft.DataFactory/factories/{factory}"
        f"/pipelines/{pipeline}"
    )


@bp.activity_trigger(input_name="dummy")
def trigger_adf_pipeline_activity(dummy=None) -> str:
    """
    Triggers the ADF pipeline and polls until completion.

    ADF Pipeline structure:
      1. DF_Transformation_RawCrypto_Silver  — Dataflow (raw CSV → Synapse silver table)
      2. Look_Up_Table                        — Lookup (reads TrackingTable)
      3. ForEach → SP_UpdateTrackingT...      — Stored Procedure per symbol

    Returns:
      SUCCEEDED:{run_id}
      FAILED:{reason}
      CANCELLED:{run_id}
      TIMEOUT:{run_id}
      ERROR:{exception}
    """

    pipeline_name = os.environ.get("ADF_PIPELINE_NAME", "UNKNOWN")
    factory_name  = os.environ.get("ADF_FACTORY_NAME", "UNKNOWN")
    start_time    = datetime.now(timezone.utc)

    logger.info(
        f"[ADF] {'='*60}\n"
        f"[ADF] 🚀 PIPELINE TRIGGER STARTED\n"
        f"[ADF] Factory  : {factory_name}\n"
        f"[ADF] Pipeline : {pipeline_name}\n"
        f"[ADF] StartTime: {start_time.isoformat()}\n"
        f"[ADF] {'='*60}"
    )

    try:
        headers  = _get_adf_headers()
        base_url = _get_base_url()
        sub      = os.environ["AZURE_SUBSCRIPTION_ID"]
        rg       = os.environ["AZURE_RESOURCE_GROUP"]

        # ---- STEP 1: TRIGGER PIPELINE ----
        run_url = f"{base_url}/createRun?api-version=2018-06-01"

        resp = requests.post(run_url, headers=headers, json={}, timeout=30)

        if resp.status_code not in [200, 202]:
            logger.error(
                f"[ADF] ❌ TRIGGER FAILED\n"
                f"[ADF] StatusCode : {resp.status_code}\n"
                f"[ADF] Response   : {resp.text[:300]}"
            )
            return f"FAILED:{resp.status_code}"

        run_id = resp.json().get("runId", "UNKNOWN")

        logger.info(
            f"[ADF] ✅ PIPELINE TRIGGERED SUCCESSFULLY\n"
            f"[ADF] Run ID    : {run_id}\n"
            f"[ADF] Factory   : {factory_name}\n"
            f"[ADF] Pipeline  : {pipeline_name}\n"
            f"[ADF] View in Azure Portal:\n"
            f"[ADF] → Data Factory → Monitor → Pipeline runs → {run_id}"
        )

        # ---- STEP 2: POLL UNTIL COMPLETE ----
        # Dataflow can take time — poll every 30s, max 60 mins
        status_url = (
            f"https://management.azure.com/subscriptions/{sub}"
            f"/resourceGroups/{rg}"
            f"/providers/Microsoft.DataFactory/factories/{factory_name}"
            f"/pipelineruns/{run_id}"
            f"?api-version=2018-06-01"
        )

        max_polls     = 120   # 120 × 30s = 60 minutes max
        poll_interval = 30    # seconds between polls

        logger.info(
            f"[ADF] ⏳ POLLING STARTED\n"
            f"[ADF] Run ID       : {run_id}\n"
            f"[ADF] Poll interval: {poll_interval}s\n"
            f"[ADF] Max wait     : {max_polls * poll_interval // 60} minutes"
        )

        for attempt in range(max_polls):
            time.sleep(poll_interval)

            # Refresh token every 10 polls (Azure tokens expire ~60 min)
            if attempt % 10 == 0:
                headers = _get_adf_headers()

            poll_resp = requests.get(status_url, headers=headers, timeout=30)

            if poll_resp.status_code != 200:
                logger.warning(
                    f"[ADF] ⚠️ Poll {attempt + 1}/{max_polls} — "
                    f"HTTP {poll_resp.status_code} (retrying...)"
                )
                continue

            run_data        = poll_resp.json()
            status          = run_data.get("status", "Unknown")
            duration_so_far = (datetime.now(timezone.utc) - start_time).seconds

            logger.info(
                f"[ADF] 🔄 Poll {attempt + 1}/{max_polls} | "
                f"Status: {status} | "
                f"Elapsed: {duration_so_far}s | "
                f"Run ID: {run_id}"
            )

            # ---- TERMINAL STATES ----
            if status == "Succeeded":
                end_time      = datetime.now(timezone.utc)
                duration_secs = (end_time - start_time).seconds
                duration_mins = duration_secs // 60

                logger.info(
                    f"[ADF] {'='*60}\n"
                    f"[ADF] ✅ PIPELINE SUCCEEDED\n"
                    f"[ADF] Run ID    : {run_id}\n"
                    f"[ADF] Factory   : {factory_name}\n"
                    f"[ADF] Pipeline  : {pipeline_name}\n"
                    f"[ADF] StartTime : {start_time.isoformat()}\n"
                    f"[ADF] EndTime   : {end_time.isoformat()}\n"
                    f"[ADF] Duration  : {duration_mins}m {duration_secs % 60}s\n"
                    f"[ADF] {'='*60}"
                )
                return f"SUCCEEDED:{run_id}"

            elif status == "Failed":
                error    = run_data.get("message", "Unknown error")
                end_time = datetime.now(timezone.utc)

                logger.error(
                    f"[ADF] {'='*60}\n"
                    f"[ADF] ❌ PIPELINE FAILED\n"
                    f"[ADF] Run ID    : {run_id}\n"
                    f"[ADF] Factory   : {factory_name}\n"
                    f"[ADF] Pipeline  : {pipeline_name}\n"
                    f"[ADF] Error     : {error[:300]}\n"
                    f"[ADF] StartTime : {start_time.isoformat()}\n"
                    f"[ADF] EndTime   : {end_time.isoformat()}\n"
                    f"[ADF] {'='*60}"
                )
                return f"FAILED:{error[:200]}"

            elif status == "Cancelled":
                logger.warning(
                    f"[ADF] ⚠️ PIPELINE CANCELLED\n"
                    f"[ADF] Run ID   : {run_id}\n"
                    f"[ADF] Pipeline : {pipeline_name}"
                )
                return f"CANCELLED:{run_id}"

            # Still running: InProgress, Queued, PreparingRun → keep polling

        # ---- TIMEOUT ----
        end_time = datetime.now(timezone.utc)
        logger.warning(
            f"[ADF] {'='*60}\n"
            f"[ADF] ⏰ PIPELINE TIMED OUT\n"
            f"[ADF] Run ID    : {run_id}\n"
            f"[ADF] Pipeline  : {pipeline_name}\n"
            f"[ADF] StartTime : {start_time.isoformat()}\n"
            f"[ADF] EndTime   : {end_time.isoformat()}\n"
            f"[ADF] Waited    : 60 minutes\n"
            f"[ADF] Note      : Pipeline may still be running in ADF\n"
            f"[ADF] {'='*60}"
        )
        return f"TIMEOUT:{run_id}"

    except Exception as e:
        logger.error(
            f"[ADF] {'='*60}\n"
            f"[ADF] ❌ EXCEPTION IN PIPELINE TRIGGER\n"
            f"[ADF] Pipeline  : {pipeline_name}\n"
            f"[ADF] Error     : {str(e)}\n"
            f"[ADF] {'='*60}",
            exc_info=True   # ✅ includes full stack trace in Application Insights
        )
        return f"ERROR:{str(e)}"