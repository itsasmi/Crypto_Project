import azure.functions as func
import azure.durable_functions as df
import logging
import os
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
        f"{base}/resume?api-version=2021-06-01"
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
def resume_synapse_activity(dummy=None) -> str:
    """
    Send resume request to Synapse SQL Pool.
    Works both locally (via Azure CLI) and in Azure (via Managed Identity).
    """
    try:
        status_url, resume_url = _get_synapse_urls()
        headers = _get_headers()

        logging.info("[Synapse] 🔍 Checking current status before resume")

        # Get current status
        resp = requests.get(status_url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            logging.warning(f"[Synapse] Status API returned {resp.status_code}")
            logging.warning(f"[Synapse] Response: {resp.text[:500]}")
            return "STATUS_CHECK_FAILED"

        body = resp.json()
        status = body.get("properties", {}).get("status")

        if not status:
            logging.warning(f"[Synapse] Status missing in response: {body}")
            return "UNKNOWN"

        logging.info(f"[Synapse] Current status: {status}")

        # If already Online, nothing to do
        if status == "Online":
            logging.info("[Synapse] ✅ Already Online, no resume needed")
            return "Online"

        # If Paused, send resume request
        if status == "Paused":
            logging.info("[Synapse] 📤 Status is Paused, sending RESUME request...")
            resume_resp = requests.post(resume_url, headers=headers, timeout=30)

            if resume_resp.status_code not in [200, 202, 204]:
                logging.warning(
                    f"[Synapse] ⚠️ Resume API returned {resume_resp.status_code}"
                )
                logging.warning(f"[Synapse] Response: {resume_resp.text[:500]}")
                return "RESUME_FAILED"
            
            logging.info("[Synapse] ✅ Resume request sent successfully")
            return "Resuming"

        # If already Resuming, just acknowledge it
        if status == "Resuming":
            logging.info("[Synapse] ⏳ Already Resuming (likely from previous run)")
            return "Resuming"

        # If Pausing, wait for it to finish then resume
        if status == "Pausing":
            logging.info("[Synapse] ⏸️ Pool is Pausing, cannot resume yet")
            return "Pausing"

        # Unexpected status
        logging.warning(f"[Synapse] ⚠️ Unexpected status: {status}")
        return status

    except Exception as e:
        logging.error(f"[Synapse] ❌ Resume activity exception: {str(e)}")
        return "ERROR"