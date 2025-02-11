from dagster import AssetSelection, define_asset_job

adhoc_request = AssetSelection.assets['adhoc_request']

rawcooked_start_jobs = define_asset_job(
    name='rawcooked_start_jobs',
    selection=AssetSelection.all() - adhoc_request,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 3,
                },
            }
        }
    }
)

adhoc_request = define_asset_job(
    name='adhoc_request_job',
    selection=adhoc_request,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 3,
                },
            }
        }
    }
)