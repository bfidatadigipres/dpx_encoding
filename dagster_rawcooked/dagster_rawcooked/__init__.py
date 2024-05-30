from dagster import Definitions, load_assets_from_modules

from . import assets_assess, assets_rawcook

all_assets = load_assets_from_modules(
    [
        assets_rawcook
    ]
)

defs = Definitions(
    assets=all_assets,
)
