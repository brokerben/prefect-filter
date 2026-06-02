"""Unit tests for Pydantic models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from prefect_filter.models import Confidence, FilterResult


def test_confidence_enum_values():
    assert Confidence.HIGH == "High"
    assert Confidence.MEDIUM == "Medium"
    assert Confidence.LOW == "Low"


def test_confidence_enum_value():
    assert Confidence.HIGH.value == "High"
    assert Confidence.MEDIUM.value == "Medium"
    assert Confidence.LOW.value == "Low"


def test_filter_result_valid():
    r = FilterResult(
        answer="YES",
        confidence=Confidence.HIGH,
        company_id="c1",
        outreach_message="Great fit.",
        reasoning="Strong match to criteria.",
    )
    assert r.answer == "YES"
    assert r.confidence == Confidence.HIGH
    assert r.company_id == "c1"


def test_filter_result_no_answer():
    r = FilterResult(
        answer="NO",
        confidence=Confidence.HIGH,
        company_id="c2",
        outreach_message="",
        reasoning="Unrelated sector.",
    )
    assert r.answer == "NO"


def test_filter_result_requires_all_fields():
    with pytest.raises(ValidationError):
        FilterResult(answer="YES")  # type: ignore[call-arg]


def test_filter_result_invalid_confidence():
    with pytest.raises(ValidationError):
        FilterResult(
            answer="YES",
            confidence="VeryHigh",  # not a valid Confidence value
            company_id="c1",
            outreach_message="",
            reasoning="",
        )


def test_filter_result_low_confidence():
    r = FilterResult(
        answer="YES",
        confidence=Confidence.LOW,
        company_id="c3",
        outreach_message="",
        reasoning="Speculative.",
    )
    assert r.confidence == Confidence.LOW
    # Decision rule: YES + LOW → excluded (tested in test_flow.py)
