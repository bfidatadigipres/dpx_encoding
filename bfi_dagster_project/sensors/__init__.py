import dagster as dg
from typing import Optional, List, Callable
from ..assets.transcode_retry import reencode_failed_asset, build_transcode_retry_asset
import datetime


def build_failed_encoding_retry_sensor(key_prefix: Optional[str] = None):
    '''
    Factory function that creates a sensor with optional key prefix.
    '''

    # Get the appropriate asset based on prefix
    asset = build_transcode_retry_asset(key_prefix)

    # Define the job with the appropriate asset / prefix
    job_name = f"{key_prefix}_backfill_failed_encodings_job"
    job = dg.define_asset_job(name=job_name, selection=[asset])

    # Determine the asset key for run config
    asset_key_str = f"{key_prefix}/reencode_failed_asset"

    # Set sensor name with optional prefix
    sensor_name = f"{key_prefix}_failed_encoding_retry_sensor"

    @dg.sensor(
        name=sensor_name,
        job=job,
        minimum_interval_seconds=3600,
        required_resource_keys={'database', 'process_pool'}
    )
    def failed_encoding_retry_sensor(
        context: dg.SensorEvaluationContext,
    ):
        '''
        Detects failed encodings that need to be retried based on database records
        '''

        # Add prefix to logging for clarity in multi-project setups
        log_prefix = f"[{key_prefix}] "

        last_check_time = context.cursor or datetime.datetime.now().isoformat()
        last_check = datetime.datetime.fromisoformat(last_check_time)
        context.log.info(f"{log_prefix}Last check time for retries: {last_check}")

        search = "SELECT * FROM encoding_status WHERE status = 'RAWcook failed'"
        failed_encodings = context.resources.database.retrieve_seq_id_row(context, search, 'fetchall')
        context.log.info(f"{log_prefix}Found failed encodings: {failed_encodings}")

        if not failed_encodings:
            return dg.SensorResult(
                skip_reason=f"{log_prefix}No failed encodings to retry",
                cursor=datetime.datetime.now().isoformat()
            )
        context.log.info(f"{log_prefix}{len(failed_encodings)} rows retrieved: {failed_encodings}")

        # Group by batch size if needed
        for seq in failed_encodings:
            context.log.info(f"{log_prefix}Processing sequence {seq}")
            seq_id = seq[1]
            spath = seq[3]
            key = seq[28]
            if key_prefix != key:
                context.log.info(f"Key prefix {key_prefix} does not match sequence {key}")
                continue
            retry_count = seq[17]
            if not retry_count.isnumeric():
                retry_count = 0
            else:
                retry_count = int(retry_count)

            if retry_count > 3:
                context.log.warning(f"{log_prefix}Attempted encodings exceeded 3 attempts. Manual attention needed.")
                arguments = (
                    ['status', 'Sequence failed repeatedly'],
                    ['error_message', 'Manual review needed, maximum retries met.'],  # Fixed missing comma
                    ['encoding_retry', retry_count]
                )
                entry = context.resources.database.append_to_database(context, seq_id, arguments)
                context.log.info(f"{log_prefix}Skipping this sequence. Row updated: {entry}")
                continue

            # Update retry count in database
            arguments = (
                ['status', 'Pending retry'],
                ['encoding_retry', retry_count + 1]
            )
            entry = context.resources.database.append_to_database(context, seq_id, arguments)
            context.log.info(f"{log_prefix}Row updated: {entry}")

            # Create a run request for this sequence with proper asset key
            run_key = f"{key_prefix}_retry_{seq_id}_{retry_count + 1}"

            yield dg.RunRequest(
                run_key=run_key,
                run_config={
                    "ops": {
                        asset_key_str: {  # Use the correct asset key based on prefix
                            "config": {"sequence": spath}
                        }
                    }
                },
                tags={
                    "retry_attempt": str(retry_count + 1),
                    "project": key_prefix
                }
            )

    return failed_encoding_retry_sensor


# Create the default sensor (no prefix) for backward compatibility
failed_encoding_retry_sensor = build_failed_encoding_retry_sensor()
