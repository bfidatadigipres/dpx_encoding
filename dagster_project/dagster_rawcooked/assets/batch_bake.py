'''
dagster_rawcooked/assets/batch_bake.py
Example code to help with batch processing
of DPX assets. Requires further development.
'''

from dagster import asset, DynamicOutput, DynamicPartitionsDefinition, AssetExecutionContext
import os
from datetime import datetime

# Define dynamic partitions for concurrent processing
dpx_partitions = DynamicPartitionsDefinition(name="dpx_sequences")


@asset
def scan_source_directory(context: AssetExecutionContext):
    '''
    Scans the source directory for DPX sequences and registers them in the database.
    Returns mapping of partition keys to DPX paths for concurrent processing.
    '''
    root = os.getenv("ENCODE_PATH")
    conn = context.resources.cookbook.get_connection()
    cursor = conn.cursor()
    new_sequences = {}
    
    # Recursively scan for DPX sequences
    directories = [ x for x in os.listdir(root) if os.path.isdir(os.path.join(root, x))]
    for directory in directories:
        dpath = os.path.join(root, directory)
            
        # Check if sequence is already registered
        cursor.execute(
            "SELECT status FROM encoding_status WHERE dpx_id = ?", 
            (directory),
        )
        result = cursor.fetchone()
        
        if not result:
            # Register new sequence
            cursor.execute(f"""
                INSERT INTO encoding_status 
                (dpx_id, folder_path, status, current_stage, last_updated) 
                VALUES (?, ?, 'pending', 'discovered', str({datetime.today()[:19]}))
            """, (directory, dpath))
            new_sequences[directory] = root
        elif result[0] == 'failed':
            # Re-queue failed sequences
            cursor.execute("""
                UPDATE encoding_status 
                SET status = 'pending', 
                    current_stage = 'rediscovered',
                    error_message = NULL 
                WHERE dpx_id = ?
            """, (directory,))
            new_sequences[directory] = dpath
    
    conn.commit()
    return new_sequences


@asset
def process_dpx_sequences(
    context: AssetExecutionContext,
    scanned_sequences: Dict[str, List[str]]
) -> None:
    '''
    Creates dynamic output for each DPX sequence to enable concurrent processing.
    '''
    for dpx_id, path in scanned_sequences.items():
        yield DynamicOutput(
            value={"dpx_id": dpx_id, "path": path},
            partition_key=dpx_id,
            mapping_key=dpx_id
        )

