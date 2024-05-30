from dagster import Definitions, load_assets_from_modules

from . import assets_assess

all_assets = load_assets_from_modules([assets_assess])

defs = Definitions(
    assets=all_assets,
)
