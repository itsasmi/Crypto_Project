import azure.functions as func
import azure.durable_functions as df
import logging
import os
import pyodbc
from datetime import datetime, timezone

bp = df.Blueprint()


def _get_connection():
    conn_str = os.environ.get("SYNAPSE_CONNECTION_STRING")
    return pyodbc.connect(conn_str)


@bp.activity_trigger(input_name="trading_pair")
def get_last_timestamp_activity(trading_pair: str) -> dict:
    
    logging.info(f"[DB] Fetching last timestamp for {trading_pair}")

    try:
        with _get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                        Last_Processed_Timestamp,
                        Record_Count
                    FROM logging.TrackingTable
                    WHERE Table_Name = 'RAW_INGESTION'
                      AND Trading_Pair = ?
                    """,
                    (trading_pair,)
                )

                row = cursor.fetchone()

                if row:
                    last_ts = row[0]
                    row_count = row[1] or 0

                    # Convert to ISO string if datetime
                    if isinstance(last_ts, datetime):
                        if last_ts.tzinfo is None:
                            last_ts = last_ts.replace(tzinfo=timezone.utc)
                        last_ts_str = last_ts.isoformat()
                    else:
                        last_ts_str = str(last_ts)

                    logging.info(
                        f"[DB] {trading_pair}: last_ts={last_ts_str}, rows={row_count}"
                    )

                    return {
                        "last_timestamp": last_ts_str,
                        "row_count": row_count
                    }
                else:
                    logging.info(f"[DB] No record found for {trading_pair}")
                    return {
                        "last_timestamp": None,
                        "row_count": 0
                    }

    except Exception as e:
        logging.error(f"[DB] Failed to get timestamp for {trading_pair}: {str(e)}")
        raise