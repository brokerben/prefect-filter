"""Shared test fixtures and configuration."""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock

import pytest
from prefect.testing.utilities import prefect_test_harness

@pytest.fixture(autouse=True, scope="session")
def prefect_test_env():
    """Run all tests against an ephemeral in-process Prefect API."""
    with prefect_test_harness():
        yield


# Register a fake equimatch_agent module so tests run without the private package.
# Individual tests patch equimatch_agent.build_agent with the desired behavior.
if "equimatch_agent" not in sys.modules:
    _fake = MagicMock()
    sys.modules["equimatch_agent"] = _fake


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": ["Authorization"],
        # "none" → fail loudly if a cassette is missing (safe default for CI).
        # Use --vcr-record=new_episodes or --vcr-record=all to record cassettes
        # against a live API.
        "record_mode": "none",
    }


@pytest.fixture(autouse=True)
def mock_settings(monkeypatch):
    """Patch settings in every module that caches it at import time."""
    from prefect_filter import config

    test_settings = config.Settings(
        backend_base_uri="https://api.equimatch.example",
        equimatch_api_key="test-key",
        openrouter_api_key="test-key",
    )
    monkeypatch.setattr("prefect_filter.config.settings", test_settings)
    monkeypatch.setattr("prefect_filter.client.settings", test_settings)
    monkeypatch.setattr("prefect_filter.flow.settings", test_settings)


# ---------------------------------------------------------------------------
# LLM agent fixtures for flow tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_agent_yes_high():
    """build_agent returns YES with High confidence."""
    agent = MagicMock()
    agent.run = AsyncMock(
        return_value=MagicMock(
            output=(
                '{"answer":"YES","confidence":"High","company_id":"co1",'
                '"outreach_message":"Your B2B SaaS expertise aligns with our client.",'
                '"reasoning":"Strong explicit match to software criteria."}'
            )
        )
    )
    return AsyncMock(return_value=agent)


@pytest.fixture
def mock_agent_yes_medium():
    """build_agent returns YES with Medium confidence."""
    agent = MagicMock()
    agent.run = AsyncMock(
        return_value=MagicMock(
            output=(
                '{"answer":"YES","confidence":"Medium","company_id":"co1",'
                '"outreach_message":"Your services partially match the criteria.",'
                '"reasoning":"Some elements align with investment focus."}'
            )
        )
    )
    return AsyncMock(return_value=agent)


@pytest.fixture
def mock_agent_no():
    """build_agent returns NO with High confidence."""
    agent = MagicMock()
    agent.run = AsyncMock(
        return_value=MagicMock(
            output=(
                '{"answer":"NO","confidence":"High","company_id":"co1",'
                '"outreach_message":"",'
                '"reasoning":"Company operates in an entirely unrelated sector."}'
            )
        )
    )
    return AsyncMock(return_value=agent)


@pytest.fixture
def mock_agent_yes_low():
    """build_agent returns YES with Low confidence."""
    agent = MagicMock()
    agent.run = AsyncMock(
        return_value=MagicMock(
            output=(
                '{"answer":"YES","confidence":"Low","company_id":"co1",'
                '"outreach_message":"Speculative match.",'
                '"reasoning":"Very indirect alignment only."}'
            )
        )
    )
    return AsyncMock(return_value=agent)
