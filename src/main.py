import logging
from logging.config import dictConfig
from urllib.request import Request

import uvicorn
from fastapi import FastAPI
from starlette.responses import Response

from frp_api.routes import engine_autocoding
from frp_api.settings.api_settings import APISettings
from frp_api.settings.logging import LOGGING_CONFIG

app = FastAPI(title="frp-api", description="API that serves content from the Engine Autocoding delta table", version="0.0.0")

api_settings = APISettings()
dictConfig(LOGGING_CONFIG(logging.INFO))


app.include_router(engine_autocoding.router, tags=["EAC"])


if api_settings.DEBUG:
    dictConfig(LOGGING_CONFIG(logging.DEBUG))

    @app.exception_handler(Exception)
    async def debug_exception_handler(request: Request, exc: Exception):
        import traceback

        return Response(
            content="".join(
                traceback.format_exception(
                    etype=type(exc), value=exc, tb=exc.__traceback__
                )
            )
        )


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
