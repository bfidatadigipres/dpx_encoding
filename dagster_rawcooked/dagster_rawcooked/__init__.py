from dagster import Definitions, load_assets_from_modules

from . import assess

all_assets = load_assets_from_modules([assess])

defs = Definitions(
    assets=all_assets,
)
