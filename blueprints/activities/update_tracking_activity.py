import azure.functions as func
import azure.durable_functions as df
import logging
import os
import pyodbc
from datetime import datetime, timezone
from dateutil import parser

bp = df.Blueprint()


def _get_connection():
    return pyodbc.connect(
        os.environ["SYNAPSE_CONNECTION_STRING"]
    )


@bp.activity_trigger(input_name="params")
def update_tracking_activity(params: dict) -> bool:

    logging.info(f"[TrackingUpdate] Payload received: {params}")

    trading_pair = params.get("trading_pair")
    last_processed_ts_raw = params.get("last_processed_timestamp")
    total_row_count = params.get("record_count")

    # 🔒 FIX: dict-safe handling
    if isinstance(total_row_count, dict):
        total_row_count = total_row_count.get("rows_written", 0)

    # ✅ Pure += always — never subtract
    # tracking["row_count"] is always CUMULATIVE total across ALL months
    # subtract would wipe out all prior months' counts
    # fetch_from in orchestrators already ensures only NEW rows are counted
    # so we just always add new_rows on top — safe for all cases:
    # CASE 0 manual same month  → += new rows only (fetch_from set)    ✅
    # CASE 1 current month      → += new rows only (fetch_from set)    ✅
    # CASE 2 past incomplete    → += new rows only (fetch_from set)    ✅
    # CASE 3 clean boundary     → += all rows (fetch_from=None)        ✅

    if not trading_pair or not last_processed_ts_raw or total_row_count is None:
        raise ValueError(
            f"Invalid payload for update_tracking_activity: {params}"
        )

    last_processed_ts = parser.isoparse(last_processed_ts_raw)
    system_update_time = datetime.now(timezone.utc)

    try:
        with _get_connection() as conn:
            with conn.cursor() as cursor:

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM logging.TrackingTable
                    WHERE Table_Name = 'RAW_INGESTION'
                      AND Trading_Pair = ?
                    """,
                    (trading_pair,)
                )

                exists = cursor.fetchone()[0] > 0

                if exists:
                    cursor.execute(
                        """
                        UPDATE logging.TrackingTable
                        SET
                            Last_Processed_Timestamp = ?,
                            Record_Count = ISNULL(Record_Count, 0) + ?,
                            System_Update_Time = ?
                        WHERE Table_Name = 'RAW_INGESTION'
                          AND Trading_Pair = ?
                        """,
                        (
                            last_processed_ts,
                            int(total_row_count), # new rows only — always safe to add
                            system_update_time,
                            trading_pair
                        )
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO logging.TrackingTable
                        (
                            Table_Name,
                            Trading_Pair,
                            Last_Processed_Timestamp,
                            Record_Count,
                            System_Update_Time
                        )
                        VALUES
                        (
                            'RAW_INGESTION',
                            ?, ?, ?, ?
                        )
                        """,
                        (
                            trading_pair,
                            last_processed_ts,
                            int(total_row_count),
                            system_update_time
                        )
                    )

                conn.commit()

        logging.info(
            f"[TrackingUpdate] Successfully updated {trading_pair} | "
            f"added: {int(total_row_count)}"
        )
        return True

    except Exception as e:
        logging.error(
            f"[TrackingUpdate] Failed for {trading_pair}: {str(e)}"
        )
        raise