from dagster import Definitions, load_assets_from_modules

from .assets import adhoc_requests, cookbook, make_ready, rawcook, taste_test, wipe_up
from .resources import database_resource
from .jobs import rawcooked_start_jobs
from .schedules import rawcooked_schedule


all_assets = load_assets_from_modules(
    [
        adhoc_requests,
        cookbook,
        make_ready,
        rawcook,
        taste_test,
        wipe_up
    ]
)

all_jobs = [rawcooked_start_jobs]
all_schedules = [rawcooked_schedule]

defs = Definitions(
    assets=all_assets,
    resources = {
        "database": database_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules,
)

