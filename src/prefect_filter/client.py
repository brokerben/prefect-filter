"""Async HTTP client for the Equimatch API."""

from __future__ import annotations

from typing import Any

import httpx

from prefect_filter.config import settings
from prefect_filter.log import get_logger

logger = get_logger(__name__)

_TIMEOUT = 30.0


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {settings.equimatch_api_key}"}


async def fetch_pipeline(pipeline_id: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(
            f"{settings.backend_base_uri}/pipelines/{pipeline_id}",
            headers=_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_companies(pipeline_id: str) -> list[dict[str, Any]]:
    all_companies: list[dict[str, Any]] = []
    page = 1
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        while True:
            resp = await client.get(
                f"{settings.backend_base_uri}/companies",
                params={"pipelineId": pipeline_id, "page": page},
                headers=_headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            all_companies.extend(data.get("companies", []))
            if data.get("page", 1) >= data.get("totalPages", 1):
                break
            page += 1
    return all_companies


async def fetch_company(company_id: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(
            f"{settings.backend_base_uri}/companies/find/{company_id}",
            headers=_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def fetch_website(website_id: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.get(
            f"{settings.backend_base_uri}/websites/{website_id}",
            headers=_headers(),
        )
        resp.raise_for_status()
        return resp.json()


async def update_company_status(
    pipeline_id: str,
    company_id: str,
    status: str,
    clause: str | None = None,
) -> None:
    body: dict[str, Any] = {
        "pipelineId": pipeline_id,
        "companyId": company_id,
        "status": status,
    }
    if clause is not None:
        body["clause"] = clause

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.patch(
            f"{settings.backend_base_uri}/companies/pipeline/status",
            json=body,
            headers=_headers(),
        )
        resp.raise_for_status()


async def update_flow_status(
    pipeline_id: str, company_id: str, flow_status: str
) -> None:
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.patch(
            f"{settings.backend_base_uri}/companies/pipeline/flow-status",
            json={
                "pipelineId": pipeline_id,
                "companyId": company_id,
                "flowStatus": flow_status,
            },
            headers=_headers(),
        )
        resp.raise_for_status()
