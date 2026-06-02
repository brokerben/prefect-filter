"""PostHog event tracking. Fire-and-forget; failures never propagate."""

from __future__ import annotations

import logging
from typing import Any

from prefect_filter.config import settings

logger = logging.getLogger(__name__)

_client: Any = None
_initialised = False


def _get_client() -> Any:
    global _client, _initialised
    if _initialised:
        return _client
    _initialised = True
    if not settings.posthog_api_key:
        return None
    from posthog import Posthog

    _client = Posthog(settings.posthog_api_key, host=settings.posthog_host)
    return _client


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


def capture(distinct_id: str, event: str, properties: dict[str, Any] | None = None) -> None:
    try:
        ph = _get_client()
        if ph is None:
            return
        props = {**(properties or {}), **_prefect_run_ids()}
        ph.capture(distinct_id, event, props)
    except Exception:
        logger.warning("posthog.capture.failed", exc_info=True)


def flush() -> None:
    try:
        if _client is not None:
            _client.flush()
    except Exception:
        logger.warning("posthog.flush.failed", exc_info=True)


def shutdown() -> None:
    try:
        if _client is not None:
            _client.shutdown()
    except Exception:
        logger.warning("posthog.shutdown.failed", exc_info=True)
