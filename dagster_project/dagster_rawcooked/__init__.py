from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from dagster import EnvVar
from .assets import batch_bake, cookbook, make_ready, rawcook, taste_test, wipe_up

all_assets = load_assets_from_modules(
    [
        batch_bake,
        cookbook,
        make_ready,
        rawcook,
        taste_test,
        wipe_up
    ]
)

defs = Definitions(
    assets=all_assets,
)

database_resource = DuckDBResource(
    database=EnvVar("RAWCOOK_DB")
)
