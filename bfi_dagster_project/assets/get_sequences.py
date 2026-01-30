import os
from typing import List, Optional

import dagster as dg


def build_target_sequences_asset(key_prefix: Optional[str] = None):
    """Factory function that returns the asset with optional key prefix."""

    # Build the asset key with optional prefix
    asset_key = (
        [f"{key_prefix}", "target_sequences"] if key_prefix else "target_sequences"
    )

    @dg.asset(key=asset_key, required_resource_keys={"database", "source_path"})
    def target_sequences(
        context: dg.AssetExecutionContext,
    ) -> List[str]:
        """
        Look for new sequences in watch folder and update to database,
        and hand list of folderpaths to assessment asset.
        """
        log_prefix = f"[{key_prefix}] " if key_prefix else ""
        target_automation = context.resources.source_path
        target = os.path.join(target_automation, "image_sequence_processing/")
        if not os.path.exists(target):
            context.log.info(f"{log_prefix}Unable to access target_path: %s", target)
            return [""]
        seq_supply = os.path.join(target, "processing")

        context.resources.database.initialise_db(context)
        directories = [
            x
            for x in os.listdir(seq_supply)
            if os.path.isdir(os.path.join(seq_supply, x))
        ]
        directories.sort()
        context.log.info(f"{log_prefix}Directories located:\n%s", directories)

        count = 1
        current_files = []
        for dr in directories:
            if "for_deletion" in dr:
                continue
            if count > 2:
                break
            dpath = os.path.join(seq_supply, dr)
            context.log.info(f"{log_prefix}Directory path: %s", dpath)

            search = "SELECT * FROM encoding_status WHERE seq_id=?"
            result = context.resources.database.retrieve_seq_id_row(
                context, search, "fetchall", (dr,)
            )
            context.log.info(f"{log_prefix}{result}")

            # Review database entries
            if len(result) == 0:
                current_files.append(dpath)
                entry = context.resources.database.start_process(
                    context, dr, dpath, "Triggered assessment", str(key_prefix)
                )
                context.log.info(
                    f"{log_prefix}New entry made in database: %s - %s", entry, dpath
                )
            elif len(result) > 0 and "Triggered assessment" not in str(result):
                context.log.info(
                    f"{log_prefix}Skipping: Sequence already listed in process/processed: %s",
                    result,
                )
                continue
            elif len(result) > 0 and "Accept gaps" in str(result):
                if "Triggered assessment" in str(result):
                    context.log.info(
                        f"{log_prefix}Picking up sequence a second time. Passing for processing with accept gaps: %s",
                        result,
                    )
                    current_files.append(f"GAPS_{dpath}")
            elif len(result) > 0 and "Triggered assessment" in str(result) and " FPS" in str(result):
                context.log.info(
                    f"{log_prefix}FPS adjustment sequence found. Passing for processing: %s",
                    result,
                )
                current_files.append(dpath)
            elif len(result) > 0 and "Triggered assessment" in str(result):
                context.log.info(
                    f"{log_prefix}Picking up sequence a second time. Passing for processing: %s",
                    result,
                )
                current_files.append(dpath)
            else:
                current_files.append(dpath)
                entry = context.resources.database.start_process(
                    context, dr, dpath, "Triggered assessment"
                )
                context.log.info(
                    f"{log_prefix}New entry made in database: %s - %s", entry, dpath
                )
            count += 1

        context.log.info(
            f"{log_prefix}Files being handed to assessment:\n%s", current_files
        )
        return current_files

    return target_sequences


# Default asset without prefix for backward compatibility
target_sequences = build_target_sequences_asset()
