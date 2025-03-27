import dagster as dg
from .. import resources
from ..assets.transcode_retry import reencode_failed_asset
import datetime


@dg.sensor(
    name="failed_encoding_retry_sensor",
    job_name="backfill_failed_encodings_job",
    minimum_interval_seconds=900,
    required_resource_keys={'database', 'process_pool'}
)
def failed_encoding_retry_sensor(
    context: dg.SensorEvaluationContext,
):
    """Detects failed encodings that need to be retried based on database records"""

    last_check_time = context.cursor or datetime.datetime.now().isoformat()
    last_check = datetime.datetime.fromisoformat(last_check_time)
    context.log.info(f"Last check time for retries: {last_check}")

    search = f"SELECT * FROM encoding_status WHERE status='RAWcook failed'"
    failed_encodings = context.resource.database.retrieve_seq_id_row(context, search, 'fetchall')
    if not failed_encodings:
        return dg.SkipReason("No failed encodings to retry")
    context.log.info(f"{len(failed_encodings)} rows retrieved: {failed_encodings}")

    if len(failed_encodings) == 0:
        return dg.SensorResult(
            skip_reason="No failed encodings to retry",
            cursor=datetime.datetime.now().isoformat()
        )

    # Group by batch size if needed
    for seq in failed_encodings:
        seq_id = seq["seq_id"]
        spath = seq["folder_path"]
        retry_count = seq.get("encoding_retry", 0)

        if int(retry_count) > 3:
            context.log.warning(f"Attemped encodings exceeded 3 attempts. Manual attention needed.")
            arguments = (
                ['status', 'Sequence failed repeatedly'],
                ['error_message', 'Manual review needed, maximum retries met.']
                ['encoding_retry', int(retry_count)]
            )
            entry = context.resource.database.append_to_database(context, seq_id, arguments)
            context.log.info(f"Skipping this sequence. Row updated: {entry}")
            continue

        # Update retry count in database
        arguments = (
            ['status', 'Pending retry'],
            ['encoding_retry', int(retry_count) + 1]
        )
        entry = context.resource.database.append_to_database(context, seq_id, arguments)
        context.log.info(f"Row updated: {entry}")

        # Create a run request for this sequence
        yield dg.RunRequest(
            run_key=f"retry_{seq_id}_{int(retry_count) + 1}",
            run_config={
                "ops": {
                    "reencode_failed_asset": {
                        "config": {"sequence": spath}
                    }
                }
            },
            tags={"retry_attempt": str(retry_count + 1)}
        )
