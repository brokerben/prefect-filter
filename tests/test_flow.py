"""Tests for flow logic.

Two layers:
1. Pure-function tests for helpers (_parse_filter_result, _build_evaluation_prompt)
   — no Prefect, no HTTP needed.
2. Integration tests for filter_single_company that mock the HTTP client layer
   and the LLM agent, validating the decision routing end-to-end.
"""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, patch

import pytest

from prefect_filter.flow import _build_evaluation_prompt, _parse_filter_result
from prefect_filter.models import Confidence, FilterResult


# ---------------------------------------------------------------------------
# Pure-function tests: _parse_filter_result
# ---------------------------------------------------------------------------


def test_parse_filter_result_yes_high():
    raw = (
        '{"answer":"YES","confidence":"High","company_id":"c1",'
        '"outreach_message":"Great fit.","reasoning":"Strong match."}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.answer == "YES"
    assert result.confidence == Confidence.HIGH
    assert result.outreach_message == "Great fit."


def test_parse_filter_result_no_high():
    raw = (
        '{"answer":"NO","confidence":"High","company_id":"c1",'
        '"outreach_message":"","reasoning":"Unrelated sector."}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.answer == "NO"
    assert result.confidence == Confidence.HIGH


def test_parse_filter_result_yes_medium():
    raw = (
        '{"answer":"YES","confidence":"Medium","company_id":"c1",'
        '"outreach_message":"Partial fit.","reasoning":"Some alignment."}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.confidence == Confidence.MEDIUM


def test_parse_filter_result_yes_low():
    raw = (
        '{"answer":"YES","confidence":"Low","company_id":"c1",'
        '"outreach_message":"","reasoning":"Speculative."}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.confidence == Confidence.LOW


def test_parse_filter_result_no_json_defaults_yes_low():
    result = _parse_filter_result("not valid json at all", "c1")
    assert result.answer == "YES"
    assert result.confidence == Confidence.LOW
    assert result.company_id == "c1"


def test_parse_filter_result_broken_json_defaults_yes_low():
    result = _parse_filter_result("{broken: json}", "c1")
    assert result.answer == "YES"
    assert result.confidence == Confidence.LOW


def test_parse_filter_result_json_wrapped_in_markdown():
    raw = '```json\n{"answer":"YES","confidence":"High","company_id":"c1","outreach_message":"Hi","reasoning":"Good"}\n```'
    result = _parse_filter_result(raw, "c1")
    assert result.answer == "YES"
    assert result.confidence == Confidence.HIGH


def test_parse_filter_result_unknown_confidence_defaults_low():
    raw = (
        '{"answer":"YES","confidence":"VeryHigh","company_id":"c1",'
        '"outreach_message":"","reasoning":""}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.confidence == Confidence.LOW


def test_parse_filter_result_invalid_answer_defaults_yes():
    raw = (
        '{"answer":"MAYBE","confidence":"High","company_id":"c1",'
        '"outreach_message":"","reasoning":""}'
    )
    result = _parse_filter_result(raw, "c1")
    assert result.answer == "YES"


def test_parse_filter_result_uses_fallback_company_id():
    raw = '{"answer":"YES","confidence":"High","outreach_message":"","reasoning":""}'
    result = _parse_filter_result(raw, "fallback-id")
    assert result.company_id == "fallback-id"


# ---------------------------------------------------------------------------
# Pure-function tests: _build_evaluation_prompt
# ---------------------------------------------------------------------------


def test_build_evaluation_prompt_contains_description():
    prompt = _build_evaluation_prompt("A fintech startup", "SaaS companies", "c1")
    assert "A fintech startup" in prompt


def test_build_evaluation_prompt_contains_criteria():
    prompt = _build_evaluation_prompt("Company X", "B2B software criteria", "c1")
    assert "B2B software criteria" in prompt


def test_build_evaluation_prompt_contains_company_id():
    prompt = _build_evaluation_prompt("Company X", "Criteria Y", "company-abc")
    assert "company-abc" in prompt


def test_build_evaluation_prompt_requests_json_output():
    prompt = _build_evaluation_prompt("desc", "criteria", "c1")
    assert "answer" in prompt
    assert "confidence" in prompt
    assert "outreach_message" in prompt


# ---------------------------------------------------------------------------
# Flow integration tests: filter_single_company
# ---------------------------------------------------------------------------

PIPELINE_DATA = {
    "pipeline": {
        "id": "pipeline123",
        "searchCriteria": "B2B software companies with revenue above $5M.",
    }
}
COMPANY_DATA = {
    "company": {
        "id": "co1",
        "name": "TechCorp Inc",
        "websites": [{"websiteId": "web1", "url": "https://techcorp.example"}],
    }
}
WEBSITE_DATA = {
    "website": {
        "id": "web1",
        "description": (
            "TechCorp is a leading B2B SaaS platform that helps enterprise finance "
            "teams automate their month-end close processes. Trusted by 500+ CFOs."
        ),
    }
}


@pytest.fixture
def mock_client(monkeypatch):
    """Patch all Equimatch API calls so no HTTP is made."""
    monkeypatch.setattr(
        "prefect_filter.client.fetch_pipeline", AsyncMock(return_value=PIPELINE_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.fetch_company", AsyncMock(return_value=COMPANY_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.fetch_website", AsyncMock(return_value=WEBSITE_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_company_status", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_flow_status", AsyncMock(return_value=None)
    )


async def test_filter_single_company_active_high(mock_client, mock_agent_yes_high):
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_yes_high):
        result = await filter_single_company("pipeline123", "co1")

    assert result.answer == "YES"
    assert result.confidence == Confidence.HIGH


async def test_filter_single_company_active_medium(mock_client, mock_agent_yes_medium):
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_yes_medium):
        result = await filter_single_company("pipeline123", "co1")

    assert result.answer == "YES"
    assert result.confidence == Confidence.MEDIUM


async def test_filter_single_company_excluded_answer_no(mock_client, mock_agent_no):
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_no):
        result = await filter_single_company("pipeline123", "co1")

    assert result.answer == "NO"


async def test_filter_single_company_excluded_low_confidence(
    mock_client, mock_agent_yes_low
):
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_yes_low):
        result = await filter_single_company("pipeline123", "co1")

    # YES + Low confidence → excluded (flow_status Z2)
    assert result.confidence == Confidence.LOW


async def test_filter_single_company_no_websites(monkeypatch):
    """Company with no websites → excluded X2, result.confidence == LOW."""
    from prefect_filter.flow import filter_single_company

    monkeypatch.setattr(
        "prefect_filter.client.fetch_pipeline", AsyncMock(return_value=PIPELINE_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.fetch_company",
        AsyncMock(return_value={"company": {"id": "co1", "websites": []}}),
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_company_status", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_flow_status", AsyncMock(return_value=None)
    )

    result = await filter_single_company("pipeline123", "co1")
    assert result.confidence == Confidence.LOW
    assert "No website" in result.reasoning


async def test_filter_single_company_empty_description(monkeypatch):
    """Company with empty website description → excluded X2."""
    from prefect_filter.flow import filter_single_company

    empty_website = {"website": {"id": "web1", "description": ""}}

    monkeypatch.setattr(
        "prefect_filter.client.fetch_pipeline", AsyncMock(return_value=PIPELINE_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.fetch_company", AsyncMock(return_value=COMPANY_DATA)
    )
    monkeypatch.setattr(
        "prefect_filter.client.fetch_website", AsyncMock(return_value=empty_website)
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_company_status", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        "prefect_filter.client.update_flow_status", AsyncMock(return_value=None)
    )

    result = await filter_single_company("pipeline123", "co1")
    assert result.confidence == Confidence.LOW
    assert "empty" in result.reasoning.lower()


async def test_filter_single_company_update_calls_on_active(
    mock_client, mock_agent_yes_high
):
    """Active decision: update_company_status('active') and update_flow_status('C3.1')."""
    from prefect_filter import client as client_module
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_yes_high):
        await filter_single_company("pipeline123", "co1")

    update_status: AsyncMock = client_module.update_company_status  # type: ignore[assignment]
    update_flow: AsyncMock = client_module.update_flow_status  # type: ignore[assignment]

    # Final status call should be "active"
    final_status_calls = [
        c for c in update_status.call_args_list if c.args[2] == "active"
    ]
    assert len(final_status_calls) >= 1

    # Final flow status should be "C3.1"
    final_flow_calls = [
        c for c in update_flow.call_args_list if c.args[2] == "C3.1"
    ]
    assert len(final_flow_calls) >= 1


async def test_filter_single_company_update_calls_on_excluded_no(
    mock_client, mock_agent_no
):
    """Excluded (NO): update_flow_status('Z2') should be called."""
    from prefect_filter import client as client_module
    from prefect_filter.flow import filter_single_company

    with patch("equimatch_agent.build_agent", mock_agent_no):
        await filter_single_company("pipeline123", "co1")

    update_flow: AsyncMock = client_module.update_flow_status  # type: ignore[assignment]
    z2_calls = [c for c in update_flow.call_args_list if c.args[2] == "Z2"]
    assert len(z2_calls) >= 1
