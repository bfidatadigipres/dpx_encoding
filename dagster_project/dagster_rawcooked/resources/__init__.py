import os
from dagster import resource


path_resource = resource(
    dpx_path=os.path.join(os.getenv("STORAGE"), os.getenv("ENCODING_PATH")),
)

database_resource = resource(
    database=os.getenv("RAWCOOK_DB"),
)