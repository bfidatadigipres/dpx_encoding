from dagster_duckdb import DuckDBResource
from dagster import EnvVar
from dagster import resource

# Improvised
path_resource = resource(
    dpx_path=EnvVar("DPX_PATH"),
)

database_resource = DuckDBResource(
    database=EnvVar("RAWCOOK_DUCKDB"),
)