"""PostHog implementation of EventBackend."""

from __future__ import annotations

from typing import Any

from posthog import Posthog


class PostHogBackend:
    def __init__(self, api_key: str, host: str) -> None:
        self._client = Posthog(api_key, host=host)

    def capture(self, distinct_id: str, event: str, properties: dict[str, Any]) -> None:
        self._client.capture(distinct_id, event, properties)

    def flush(self) -> None:
        self._client.flush()

    def shutdown(self) -> None:
        self._client.shutdown()
