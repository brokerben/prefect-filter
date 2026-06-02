"""VCR-based tests for the Equimatch HTTP client.

Cassettes live in tests/cassettes/test_client/ and are replayed on every run.
To record fresh cassettes against a live API:

    BACKEND_BASE_URI=https://<real-api> EQUIMATCH_API_KEY=<key> \\
        uv run pytest tests/test_client.py --vcr-record=all
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from prefect_filter import client


@pytest.fixture(scope="module")
def vcr_cassette_dir(request):
    return str(Path(__file__).parent / "cassettes" / "test_client")


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@pytest.mark.vcr
async def test_fetch_pipeline():
    data = await client.fetch_pipeline("pipeline123")
    assert "pipeline" in data
    assert "searchCriteria" in data["pipeline"]
    assert len(data["pipeline"]["searchCriteria"]) > 0


@pytest.mark.vcr
async def test_fetch_companies_single_page():
    companies = await client.fetch_companies("pipeline123")
    assert isinstance(companies, list)
    assert len(companies) == 2
    assert all("id" in c for c in companies)


@pytest.mark.vcr
async def test_fetch_companies_multi_page():
    """Pagination: client must loop until page == totalPages."""
    companies = await client.fetch_companies("pipeline123")
    assert len(companies) == 2
    ids = [c["id"] for c in companies]
    assert "co1" in ids
    assert "co2" in ids


@pytest.mark.vcr
async def test_fetch_company():
    data = await client.fetch_company("company456")
    company = data["company"]
    assert company["id"] == "company456"
    assert "websites" in company
    assert len(company["websites"]) > 0


@pytest.mark.vcr
async def test_fetch_website():
    data = await client.fetch_website("website789")
    website = data["website"]
    assert "description" in website
    assert len(website["description"]) > 0


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@pytest.mark.vcr
async def test_update_company_status():
    # Should not raise
    await client.update_company_status("pipeline123", "company456", "active")


@pytest.mark.vcr
async def test_update_company_status_with_clause():
    await client.update_company_status(
        "pipeline123",
        "company456",
        "active",
        clause="Your B2B SaaS expertise aligns with our client's investment criteria.",
    )


@pytest.mark.vcr
async def test_update_flow_status():
    await client.update_flow_status("pipeline123", "company456", "C3.1")
