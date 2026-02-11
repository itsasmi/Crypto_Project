import azure.functions as func
import azure.durable_functions as df
import logging
import os
import pyodbc

bp = df.Blueprint()


def _get_connection():
    conn_str = os.environ.get("SYNAPSE_CONNECTION_STRING")
    return pyodbc.connect(conn_str, timeout=10)


@bp.activity_trigger(input_name="dummy")
def check_synapse_ready_activity(dummy=None) -> bool:
    """
    Check if Synapse database is accessible by running a simple query.
    Works both locally and in Azure.
    """
    try:
        with _get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        
        logging.info("[SynapseCheck] ✅ Database is READY and queryable")
        return True
        
    except Exception as e:
        error_msg = str(e)
        
        # Check if it's a "database not available" error
        if "not currently available" in error_msg or "40613" in error_msg or "40892" in error_msg:
            logging.warning("[SynapseCheck] ⏳ Database still resuming...")
        else:
            logging.warning(f"[SynapseCheck] ⚠️ Database check failed: {error_msg[:200]}")
        
        return False