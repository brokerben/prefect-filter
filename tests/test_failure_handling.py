"""Tests for fail-on-external-dependency-failure behavior.

These verify that external failures (unparseable LLM output, LLM exceptions,
missing company data) raise instead of defaulting the company to included.
They exercise the underlying functions (``.fn``) so no Prefect server is needed.
"""

from __future__ import annotations

import pytest

from prefect_filter import flow
from prefect_filter.models import Confidence, FilterResult


# ---------------------------------------------------------------------------
# _parse_filter_result
# ---------------------------------------------------------------------------


def test_parse_no_json_raises():
    with pytest.raises(ValueError, match="parse_error"):
        flow._parse_filter_result("there is no json here", "c1")


def test_parse_invalid_json_raises():
    with pytest.raises(ValueError, match="parse_error"):
        flow._parse_filter_result("prefix {not valid json", "c1")


def test_parse_valid_json_returns_result():
    raw = (
        'Here you go: {"answer": "NO", "confidence": "High", '
        '"company_id": "c1", "outreach_message": "msg", "reasoning": "because"}'
    )
    result = flow._parse_filter_result(raw, "c1")
    assert isinstance(result, FilterResult)
    assert result.answer == "NO"
    assert result.confidence == Confidence.HIGH


def test_parse_odd_confidence_coerces_to_low():
    # A well-formed JSON with an unknown confidence is still a usable answer.
    raw = '{"answer": "YES", "confidence": "banana", "company_id": "c1"}'
    result = flow._parse_filter_result(raw, "c1")
    assert result.answer == "YES"
    assert result.confidence == Confidence.LOW


# ---------------------------------------------------------------------------
# evaluate_company_task
# ---------------------------------------------------------------------------


async def test_evaluate_company_propagates_llm_exception(monkeypatch):
    async def _boom(*args, **kwargs):
        raise RuntimeError("LLM down")

    # build_agent is imported inside the task function from equimatch_agent.
    import sys
    import types

    fake_module = types.ModuleType("equimatch_agent")
    fake_module.build_agent = _boom
    monkeypatch.setitem(sys.modules, "equimatch_agent", fake_module)

    with pytest.raises(RuntimeError, match="LLM down"):
        await flow.evaluate_company_task.fn("desc", "criteria", "c1")


# ---------------------------------------------------------------------------
# filter_single_company — data-missing cases raise (no default-to-included)
# ---------------------------------------------------------------------------


class _Recorder:
    def __init__(self):
        self.statuses: list[str] = []

    async def update_status(self, pipeline_id, company_id, status, clause=None):
        self.statuses.append(status)

    async def update_flow_status(self, pipeline_id, company_id, flow_status):
        pass


def _patch_common(monkeypatch, recorder, company):
    # Replace the task OBJECTS with plain async stubs so the flow body runs
    # without the Prefect engine / a backend.
    async def _pipeline(pipeline_id):
        return {"pipeline": {"searchCriteria": "criteria"}}

    async def _company(company_id):
        return {"company": company}

    monkeypatch.setattr(flow, "fetch_pipeline_task", _pipeline)
    monkeypatch.setattr(flow, "fetch_company_task", _company)
    monkeypatch.setattr(flow, "update_status_task", recorder.update_status)
    monkeypatch.setattr(flow, "update_flow_status_task", recorder.update_flow_status)


async def test_no_websites_raises_and_never_decides(monkeypatch):
    recorder = _Recorder()
    _patch_common(monkeypatch, recorder, {"websites": []})

    with pytest.raises(ValueError, match="has no websites"):
        await flow.filter_single_company.fn("p1", "c1")

    # Only "processing" should have been written — never active/excluded.
    assert recorder.statuses == ["processing"]
    assert "active" not in recorder.statuses
    assert "excluded" not in recorder.statuses


async def test_empty_description_raises(monkeypatch):
    recorder = _Recorder()
    _patch_common(
        monkeypatch, recorder, {"websites": [{"websiteId": "w1"}], "description": ""}
    )

    async def _website(website_id):
        return {"website": {"description": ""}}

    monkeypatch.setattr(flow, "fetch_website_task", _website)

    with pytest.raises(ValueError, match="description is empty"):
        await flow.filter_single_company.fn("p1", "c1")

    assert "active" not in recorder.statuses
    assert "excluded" not in recorder.statuses
