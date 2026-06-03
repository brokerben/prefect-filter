"""CLI entry points."""

import asyncio
import os
from prefect import serve as prefect_serve

from prefect_filter.flow import filter_companies, filter_pipeline, filter_single_company

RUN_LIMIT = int(os.environ.get("RUN_LIMIT", 5))


def main() -> None:
    pipeline_id = os.environ.get("PIPELINE_ID", "")
    if not pipeline_id:
        raise SystemExit("PIPELINE_ID environment variable is required")
    flow_status = os.environ.get("FLOW_STATUS") or "C2.2"
    asyncio.run(filter_pipeline(pipeline_id, flow_status=flow_status))


def serve() -> None:
    pipeline_filter = filter_pipeline.to_deployment(
        name="pipeline-filter",
        tags=["filter", "pipeline"],
        description="Filter pipeline companies against investment criteria",
        concurrency_limit=RUN_LIMIT,
    )
    filter_company = filter_single_company.to_deployment(
        name="filter-company",
        tags=["filter", "single-company"],
        description="Filter company against investment criteria",
    )
    prefect_serve(pipeline_filter, filter_company)
