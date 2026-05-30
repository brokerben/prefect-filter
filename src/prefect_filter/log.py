"""Structured logging setup."""

import logging
import sys
from typing import Any

from pythonjsonlogger import core as jsonlogger_core
from pythonjsonlogger import jsonlogger

_LOGGING_KWARGS = {"exc_info", "stack_info", "stacklevel", "extra"}
_EXTRA_KEY_PREFIX = "kw__"
_RESERVED_EXTRA_KEYS = set(jsonlogger_core.RESERVED_ATTRS) | {"message", "asctime"}


class _PrefectRunContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "flow_run_id", None) or getattr(record, "task_run_id", None):
            return True
        try:
            from prefect.context import get_run_context

            get_run_context()
            return True
        except Exception:
            return False


class _JsonFormatter(jsonlogger.JsonFormatter):
    def process_log_record(self, log_data: dict[str, Any]) -> dict[str, Any]:
        for key in list(log_data.keys()):
            if not key.startswith(_EXTRA_KEY_PREFIX):
                continue
            original_key = key[len(_EXTRA_KEY_PREFIX) :]
            log_data.setdefault(original_key, log_data[key])
            del log_data[key]
        return log_data


class _StructuredAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        passthrough: dict[str, Any] = {}
        extra: dict[str, Any] = {}

        existing_extra = kwargs.get("extra")
        if isinstance(existing_extra, dict):
            extra.update(existing_extra)

        for key, value in kwargs.items():
            if key in _LOGGING_KWARGS:
                if key != "extra":
                    passthrough[key] = value
                continue
            if key in _RESERVED_EXTRA_KEYS:
                extra[f"{_EXTRA_KEY_PREFIX}{key}"] = value
            else:
                extra[key] = value

        if self.extra:
            extra = {**self.extra, **extra}

        try:
            from prefect.context import get_run_context

            ctx = get_run_context()
            task_run = getattr(ctx, "task_run", None)
            flow_run = getattr(ctx, "flow_run", None)
            if task_run is not None:
                extra.setdefault("task_run_id", str(task_run.id))
                extra.setdefault("flow_run_id", str(task_run.flow_run_id))
            elif flow_run is not None:
                extra.setdefault("flow_run_id", str(flow_run.id))
        except Exception:
            pass

        if extra:
            passthrough["extra"] = extra

        return msg, passthrough


def setup_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = _JsonFormatter(
        "%(message)s %(levelname)s %(name)s",
        rename_fields={"levelname": "level", "name": "logger"},
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(level)

    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)

    for name in ("prefect", "prefect.flow_runs", "prefect.task_runs"):
        logging.getLogger(name).propagate = True

    try:
        from prefect.logging.handlers import APILogHandler
    except Exception:
        APILogHandler = None  # type: ignore[assignment]

    if APILogHandler is not None:
        for name in ("prefect.flow_runs", "prefect.task_runs"):
            lg = logging.getLogger(name)
            if not any(isinstance(h, APILogHandler) for h in lg.handlers):
                lg.addHandler(APILogHandler())

        app_logger = logging.getLogger("prefect_filter")
        app_api_handler = next(
            (h for h in app_logger.handlers if isinstance(h, APILogHandler)),
            None,
        )
        if app_api_handler is None:
            app_api_handler = APILogHandler()
            app_logger.addHandler(app_api_handler)
        app_api_handler.setFormatter(
            _JsonFormatter(
                "%(message)s %(levelname)s %(name)s",
                rename_fields={"levelname": "level", "name": "logger"},
            )
        )
        if not any(isinstance(f, _PrefectRunContextFilter) for f in app_api_handler.filters):
            app_api_handler.addFilter(_PrefectRunContextFilter())
        app_logger.propagate = True

    for name in ("prefect.flow_runs", "prefect.task_runs"):
        logging.getLogger(name).setLevel(logging.INFO)


def get_logger(name: str) -> logging.LoggerAdapter:
    return _StructuredAdapter(logging.getLogger(name), {})
