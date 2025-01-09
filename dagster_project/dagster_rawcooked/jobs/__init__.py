from dagster import AssetSelection, define_asset_job

rawcooked_start_jobs = define_asset_job(
    name='rawcooked_start_jobs',
    selection=AssetSelection.all()
)
