"""CLI entry points."""

import asyncio
import os

from prefect import get_client, serve as prefect_serve

from prefect_filter.config import settings
from prefect_filter.flow import filter_pipeline, filter_single_company

RUN_LIMIT = int(os.environ.get("RUN_LIMIT", 5))


def main() -> None:
    pipeline_id = os.environ.get("PIPELINE_ID", "")
    if not pipeline_id:
        raise SystemExit("PIPELINE_ID environment variable is required")
    flow_status = os.environ.get("FLOW_STATUS") or "C2.2"
    asyncio.run(filter_pipeline(pipeline_id, flow_status=flow_status))


async def _create_concurrency_limits() -> None:
    # Create the named global concurrency limits enforced via concurrency(...) in
    # flow.py and surfaced in the Prefect UI. Idempotent (upsert) — safe to call
    # on every start.
    async with get_client() as client:
        # Caps how many companies are evaluated concurrently across all runs.
        await client.upsert_global_concurrency_limit_by_name(
            name="filter-company", limit=settings.max_concurrency
        )
        # Caps how many pipeline runs process concurrently.
        await client.upsert_global_concurrency_limit_by_name(
            name="filter-pipeline", limit=RUN_LIMIT
        )


def serve() -> None:
    asyncio.run(_create_concurrency_limits())

    pipeline_filter = filter_pipeline.to_deployment(
        name="pipeline-filter",
        tags=["filter", "pipeline"],
        description="Filter pipeline companies against investment criteria",
    )
    filter_company = filter_single_company.to_deployment(
        name="filter-company",
        tags=["filter", "single-company"],
        description="Filter company against investment criteria",
    )
    prefect_serve(pipeline_filter, filter_company)
