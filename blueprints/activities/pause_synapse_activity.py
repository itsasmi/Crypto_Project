import azure.functions as func
import azure.durable_functions as df
import logging
import os
import time
import requests
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.core.exceptions import ClientAuthenticationError

bp = df.Blueprint()


# def _is_running_locally():
#     """Check if running locally vs in Azure"""
#     return os.environ.get("WEBSITE_INSTANCE_ID") is None

def _get_headers():
    """Get authorization headers (works locally & in Azure)"""
    try:
        logging.info("[Synapse] Using DefaultAzureCredential")

        credential = DefaultAzureCredential()
        token = credential.get_token(
            "https://management.azure.com/.default"
        ).token

        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    except Exception as e:
        logging.error(f"[Synapse] Authentication failed: {str(e)}")
        raise


def _get_synapse_urls():
    sub = os.environ["AZURE_SUBSCRIPTION_ID"]
    rg = os.environ["AZURE_RESOURCE_GROUP"]
    ws = os.environ["SYNAPSE_WORKSPACE_NAME"]
    pool = os.environ["SYNAPSE_SQL_POOL_NAME"]

    base = (
        f"https://management.azure.com/subscriptions/{sub}"
        f"/resourceGroups/{rg}"
        f"/providers/Microsoft.Synapse/workspaces/{ws}"
        f"/sqlPools/{pool}"
    )

    return (
        f"{base}?api-version=2021-06-01",
        f"{base}/pause?api-version=2021-06-01"
    )


# def _get_headers():
#     """Get authorization headers - works both locally and in Azure"""
#     try:
#         if _is_running_locally():
#             # Use Azure CLI credential when running locally
#             logging.info("[Synapse] Using Azure CLI credentials (local)")
#             credential = AzureCliCredential()
#         else:
#             # Use DefaultAzureCredential (Managed Identity) when in Azure
#             logging.info("[Synapse] Using Managed Identity (Azure)")
#             credential = DefaultAzureCredential()
        
#         token = credential.get_token("https://management.azure.com/.default").token
#         return {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json"
#         }
#     except ClientAuthenticationError as e:
#         logging.error(f"[Synapse] Authentication failed: {str(e)}")
#         logging.error("[Synapse] If running locally, make sure you've run 'az login' first")
#         raise


@bp.activity_trigger(input_name="dummy")
def pause_synapse_activity(dummy=None) -> str:
    """
    Send pause request to Synapse SQL Pool.
    Works both locally (via Azure CLI) and in Azure (via Managed Identity).
    """
    try:
        status_url, pause_url = _get_synapse_urls()
        headers = _get_headers()

        logging.info("[Synapse] 🔍 Checking current status before pause")

        resp = requests.get(status_url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            logging.warning(f"[Synapse] Status API returned {resp.status_code}")
            return "UNKNOWN"

        resp_data = resp.json()
        logging.info(f"[Synapse] Response: {resp_data}")

        # Safely extract status
        if "properties" not in resp_data:
            logging.error(f"[Synapse] Missing 'properties' in response: {resp_data}")
            return "UNKNOWN"

        status = resp_data["properties"].get("status")

        if not status:
            logging.warning(f"[Synapse] Status missing in properties: {resp_data}")
            return "UNKNOWN"

        logging.info(f"[Synapse] Current status: {status}")

        if status == "Paused":
            logging.info("[Synapse] ✅ Already Paused")
            return "Paused"

        if status == "Online":
            logging.info("[Synapse] 📤 Status is Online, sending PAUSE request...")
            pause_resp = requests.post(pause_url, headers=headers, timeout=30)

            if pause_resp.status_code not in [200, 202, 204]:
                logging.warning(
                    f"[Synapse] ⚠️ Pause failed: {pause_resp.status_code} {pause_resp.text}"
                )
                return "PAUSE_FAILED"
            
            logging.info("[Synapse] ✅ Pause request sent successfully")

        elif status in ["Pausing", "Resuming"]:
            logging.info(f"[Synapse] ⏳ Pool is {status}, waiting...")

        else:
            logging.warning(f"[Synapse] ⚠️ Unexpected state: {status}")
            return "UNKNOWN"

        # Poll until Paused
        for attempt in range(20):  # ~10 minutes max
            time.sleep(30)
            
            poll_resp = requests.get(status_url, headers=headers, timeout=30)
            if poll_resp.status_code != 200:
                continue

            poll_data = poll_resp.json()
            poll_status = poll_data.get("properties", {}).get("status")
            logging.info(f"[Synapse] Poll {attempt + 1}/20: status = {poll_status}")
            
            if poll_status == "Paused":
                logging.info("[Synapse] ✅ Pause completed")
                return "Paused"

        logging.warning("[Synapse] ⏰ Timeout waiting for Paused state")
        return "TIMEOUT"

    except Exception as e:
        logging.error(f"[Synapse] ❌ Pause activity exception: {str(e)}")
        return "ERROR"