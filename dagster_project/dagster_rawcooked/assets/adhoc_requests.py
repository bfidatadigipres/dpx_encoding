from dagster import asset, AssetIn, Config

class RequestConfig(Config):
    filename: str
    size: int


@asset(
    ins={
      "db": AssetIn("encoding_database")
    }
)
def adhoc_requests(config: RequestConfig) -> list:
    '''
    Retrieve filename/status of files that have
    been assessed for RAWcooking
    '''
    print(config.filename)
    print(config.status)

    cursor = db.cursor()
            
    # Check if sequence is already registered
    cursor.execute(
        "SELECT status FROM encoding_status WHERE dpx_id = * AND assessment_pass = ? AND status = ? ", 
        ('True', 'Assessment complete'),
    )
    rawcooked = cursor.fetchall()

    cursor.execute(
        "SELECT status FROM encoding_status WHERE dpx_id = * AND assessment_pass = ? AND status = ? ", 
        ('True', 'Output version 2'),
    )
    rawcooked_v2 = cursor.fetchall()

    rawcook_first = rawcook_retry = []

    for entry in rawcooked:
        # search config hear for fetchall()
        # Build dpx_id list of items not already RAWcooked encoded

    for entry in rawcooked_v2:
        # search config hear for fetchall()
        # Build dpx_id list of items not already RAWcooked encoded

    return rawcook_first, rawcook_retry
