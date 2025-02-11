'''
Check the FFV1 Matroskas
match Policy validation and
logs are healthy.

Output results to dB.
Move successful files to
autoingest.
'''

# Imports
import os
from dagster import asset, AssetIn, DynamicOutput, AssetExecutionContext
from .dpx_rawcook import encoder
from .config import QNAP_FILM, DPX_COOK, MKV_ENCODED
