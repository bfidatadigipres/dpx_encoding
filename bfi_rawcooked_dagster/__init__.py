from dagster import (
    load_assets_from_package_module,
    Definitions,
    define_asset_job,
    ScheduleDefinition
)

from bfi_rawcooked_dagster import assets
import os


defs = Definitions(
    assets=load_assets_from_package_module(assets),
    schedules=[
        ScheduleDefinition(
            job=define_asset_job(name="daily_refresh", selection="*"),
            cron_schedule="@daily",
        )
    ],
    resources={
        "qnap_film_pth": os.environ[''],
        "qnap_filmops_pth": os.environ[''],
        "qnap_filmops_pth2": os.environ[''],
        "isilon_filmops": os.environ[''],
        "isilon_digiops": os.environ[''],
        "qnap_11_dms": os.environ[''],
        "qnap_dms": os.environ['']
    },
)
