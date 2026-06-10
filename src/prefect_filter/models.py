"""Pydantic models for API responses and LLM evaluation output."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel


class Confidence(str, Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"


class FilterResult(BaseModel):
    answer: str
    confidence: Confidence
    company_id: str
    outreach_message: str
    reasoning: str
    failure_reason: str | None = None  # None means no failure; set when process couldn't complete normally
