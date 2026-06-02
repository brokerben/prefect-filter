"""PostHog implementation of EventBackend.

Maps the generic ``capture(event, data)`` call to PostHog's
``capture(distinct_id, event, properties)`` format.  The ``id`` key in
*data* becomes PostHog's ``distinct_id``; everything else is sent as
event properties.
"""

from __future__ import annotations

from typing import Any

from posthog import Posthog


class PostHogBackend:
    def __init__(self, api_key: str, host: str) -> None:
        self._client = Posthog(api_key, host=host)

    def capture(self, event: str, data: dict[str, Any]) -> None:
        properties = {k: v for k, v in data.items() if k != "id"}
        distinct_id = str(data.get("id", "unknown"))
        self._client.capture(distinct_id, event, properties)

    def flush(self) -> None:
        self._client.flush()

    def shutdown(self) -> None:
        self._client.shutdown()
