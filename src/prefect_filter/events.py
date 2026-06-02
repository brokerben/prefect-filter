"""Backend-agnostic event capture.

Swap the analytics provider by changing ``_create_backend`` — the rest
of the codebase depends only on the ``EventBackend`` protocol.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol, runtime_checkable

from prefect_filter.config import settings

logger = logging.getLogger(__name__)


@runtime_checkable
class EventBackend(Protocol):
    def capture(self, distinct_id: str, event: str, properties: dict[str, Any]) -> None: ...
    def flush(self) -> None: ...
    def shutdown(self) -> None: ...


class NullBackend:
    def capture(self, distinct_id: str, event: str, properties: dict[str, Any]) -> None:
        pass

    def flush(self) -> None:
        pass

    def shutdown(self) -> None:
        pass


def _create_backend() -> EventBackend:
    if settings.posthog_api_key:
        from prefect_filter.posthog_backend import PostHogBackend

        return PostHogBackend(settings.posthog_api_key, settings.posthog_host)
    return NullBackend()


def _prefect_run_ids() -> dict[str, str]:
    ids: dict[str, str] = {}
    try:
        from prefect.context import get_run_context

        ctx = get_run_context()
        task_run = getattr(ctx, "task_run", None)
        flow_run = getattr(ctx, "flow_run", None)
        if task_run is not None:
            ids["task_run_id"] = str(task_run.id)
            ids["flow_run_id"] = str(task_run.flow_run_id)
        elif flow_run is not None:
            ids["flow_run_id"] = str(flow_run.id)
    except Exception:
        pass
    return ids


_backend: EventBackend | None = None


def _get_backend() -> EventBackend:
    global _backend
    if _backend is None:
        _backend = _create_backend()
    return _backend


def capture(distinct_id: str, event: str, properties: dict[str, Any] | None = None) -> None:
    try:
        props = {**(properties or {}), **_prefect_run_ids()}
        _get_backend().capture(distinct_id, event, props)
    except Exception:
        logger.warning("events.capture.failed", exc_info=True)


def flush() -> None:
    try:
        _get_backend().flush()
    except Exception:
        logger.warning("events.flush.failed", exc_info=True)


def shutdown() -> None:
    try:
        _get_backend().shutdown()
    except Exception:
        logger.warning("events.shutdown.failed", exc_info=True)
