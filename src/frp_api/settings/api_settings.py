from typing import Optional
from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    DEBUG: bool = False
    EAC_TABLE_PATH: Optional[str]=None
    AZURE_STORAGE_ACCOUNT_NAME:Optional[str]=None
    AZURE_STORAGE_ACCESS_KEY:Optional[str]=None