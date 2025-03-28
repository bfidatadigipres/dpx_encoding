from typing import List
import dagster as dg

from .get_sequences import build_get_sequences_asset
from .assessment import build_assess_sequence_asset
from .transcoding import build_transcode_ffv1_asset
from .validation import build_validation_asset
from .archiving import build_archiving_asset
from .transcode_retry import build_transcode_retry_asset


def create_project_assets(project_id: str) -> List[dg.AssetsDefinition]:
    ''''
    Create a set of assets with project-specific prefixes.
    '''

    return [
        build_get_sequences_asset(project_id),
        build_assess_sequence_asset(project_id),
        build_transcode_ffv1_asset(project_id),
        build_validation_asset(project_id),
        build_archiving_asset(project_id),
        build_transcode_retry_asset(project_id)
    ]
