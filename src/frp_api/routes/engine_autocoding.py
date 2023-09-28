from typing import Dict

from fastapi import APIRouter

from frp_api.models.table import Table
from frp_api.services.delta_table_service import DeltaTableService
from frp_api.settings.api_settings import APISettings
from frp_api.utils.utils import get_logger_for_class

router = APIRouter()
logger = get_logger_for_class(__name__, "EngineAutoCodingRouter")
api_settings = APISettings()

delta_table_service = DeltaTableService(table_path=api_settings.EAC_TABLE_PATH, azure_storage_account_name=api_settings.AZURE_STORAGE_ACCOUNT_NAME, azure_storage_access_key=api_settings.AZURE_STORAGE_ACCESS_KEY)


@router.get(
    "/table",
    response_model=Table,
    summary="Get table from Engine Autocoding",
    response_model_exclude_none=False,
)
def get_engine_autocoding_table(
    sort_key: str = "Date_Reported",
    sort_dir: str = "descending",
    page_size: int = 20,
    offset: int = 0,
    filter_column: str = "TOC",
    filter_value: str = "SRT"
) -> Table:
    return delta_table_service.get_table(sort_key, sort_dir, page_size, offset, filter_column, filter_value)
