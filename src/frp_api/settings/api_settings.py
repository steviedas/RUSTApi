from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    DEBUG: bool = False
    EAC_TABLE_PATH: str
