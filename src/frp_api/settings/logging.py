import sys

LOGGING_CONFIG = lambda log_level: {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": sys.stderr,
        },
    },
    "loggers": {
        "": {  # root logger
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
        "api": {
            "handlers": ["default"],
            "level": log_level,
            "propagate": False,
        },
        "__main__": {"handlers": ["default"], "level": log_level, "propagate": False},
    },
}

