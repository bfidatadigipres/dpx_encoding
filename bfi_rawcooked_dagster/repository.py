from dagster import (
    load_assets_from_packing_module,
    repository,
    define_asset_job,
    ScheduleDefinition,
)

from rawcooked_dagster import assets

daily_job = define_asset_job(name="daily_refresh", selection="*")
daily_schedule = ScheduleDefinition(
    job=daily_job,
    cron_schedule="@daily",
)


@repository
def my_dagster_project():
    return [
        daily_job,
        daily_schedule,
        load_assets_from_package_module(assets),
]
