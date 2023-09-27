from typing import Dict

from fastapi import APIRouter

from frp_api.models.table import Table
from frp_api.services.delta_table_service import DeltaTableService
from frp_api.settings.api_settings import APISettings
from frp_api.utils.utils import get_logger_for_class

router = APIRouter()
logger = get_logger_for_class(__name__, "Jurisprudentie")
api_settings = APISettings()

delta_table_service = DeltaTableService(table_path=api_settings.EAC_TABLE_PATH)


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
) -> Table:
    return delta_table_service.get_table(sort_key, sort_dir, page_size, offset)
