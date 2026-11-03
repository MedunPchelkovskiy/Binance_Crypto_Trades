# monitoring/logging_config.py
import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def setup_logging(service_name, level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "time", "levelname": "level"},
        static_fields={"service": service_name},
    ))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)