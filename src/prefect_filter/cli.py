"""CLI entry points."""

import asyncio
import os

from prefect_filter.flow import filter_companies, filter_pipeline, filter_single_company

RUN_LIMIT = int(os.environ.get("RUN_LIMIT", 5))


def main() -> None:
    pipeline_id = os.environ.get("PIPELINE_ID", "")
    if not pipeline_id:
        raise SystemExit("PIPELINE_ID environment variable is required")
    flow_status = os.environ.get("FLOW_STATUS") or None
    asyncio.run(filter_pipeline(pipeline_id, flow_status=flow_status))


def serve() -> None:
    filter_pipeline.serve(
        name="pipeline-filter",
        tags=["filter"],
        description="Filter pipeline companies against investment criteria",
        limit=RUN_LIMIT,
    )
