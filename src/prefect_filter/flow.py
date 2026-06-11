"""Prefect flow: filter pipeline companies against investment criteria via LLM."""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.deployments import run_deployment
from prefect.tasks import exponential_backoff, task_input_hash

from prefect_filter import client, events
from prefect_filter.config import settings
from prefect_filter.log import get_logger, setup_logging
from prefect_filter.models import Confidence, FilterResult

logger = get_logger(__name__)

_MODEL_OVERRIDE = "google/gemma-4-31b-it"

# Deployment slug (flow name / deployment name) used to launch each company as an
# independent, individually-retryable flow run. Must match cli.py:serve().
_FILTER_COMPANY_DEPLOYMENT = "filter_single_company/filter-company"


@dataclass
class ChunkOutcome:
    """Outcome of filtering a chunk of companies.

    ``results`` holds the FilterResult of every company that completed (whether
    accepted or excluded). ``failures`` holds ``(company_id, error)`` for every
    company whose run failed — these are NOT defaulted to included; they remain
    Failed runs that are auto-retried and retryable from the Prefect UI.
    """

    results: list[FilterResult] = field(default_factory=list)
    failures: list[tuple[str, str]] = field(default_factory=list)


def _build_evaluation_prompt(
    description: str, criteria: str, company_id: str
) -> str:
    return (
        "# Company Relevance Evaluation Prompt\n\n"
        "## Your Task\n"
        "Decide if a company matches the given Client's Investment Criteria "
        "based on its company description. We are an origination outreach service "
        "and your role is to decide if the target company below fits the investment "
        "criteria of our client. Default to YES unless the company is clearly "
        "irrelevant.\n\n"
        "## Input Format\n\n"
        f"**Company Description:**\n{description}\n\n"
        f"**Client's Investment Criteria:**\n{criteria}\n\n"
        f"**Company ID**: {company_id}\n\n"
        "## Evaluation Guidelines\n\n"
        "Include (YES):\n"
        "- The company's description shows an explicit or potential match to the "
        "investment criteria.\n"
        "- The company operates in an adjacent sector or industry that could "
        "reasonably relate to the criteria.\n"
        "- Borderline or uncertain cases -> still YES (to avoid false negatives)\n\n"
        "Exclude (NO):\n"
        "- The company's description has no connection at all to the investment "
        "criteria.\n"
        "- The industry, products, and services are entirely unrelated.\n\n\n"
        "Confidence Guidelines\n\n"
        "- High: Strong alignment (or strong evidence of no alignment) explicitly "
        "stated in the description.\n"
        "- Medium: Partial or indirect alignment; some elements match, others unclear.\n"
        "- Low: Very speculative alignment; weak or indirect signals only.\n\n\n"
        "## Outreach Message Rules\n"
        "Generate a single-sentence outreach message following these **STRICT** "
        "constraints:\n"
        '1. **No Greetings**: Do not use "Hi," "Hello," "Dear," or any pleasantries.\n'
        "2. **No Company Names**: Never mention the name of the company.\n"
        '3. **Direct Address**: Use "your" or "you" to refer to their expertise/services.\n'
        "4. **Alignment**: Address exactly how the company fits the **investment "
        "criteria of our client**.\n"
        "5. **Style**: Direct, professional, and complimentary regarding their specific "
        "capabilities.\n"
        "6. **Content**: Cite specific characteristics from the description that show "
        "alignment.\n\n"
        "**Good Examples:**\n"
        '- "Your expertise in designing and manufacturing modular facade panels for '
        "institutional projects aligns perfectly with our client's investment criteria "
        'in the sustainable construction sector."\n'
        '- "We are impressed by your expertise in streamlining financial reporting for '
        "CFOs, which directly fits our client's investment focus on specialized B2B "
        'financial services."\n\n\n'
        "## Required Response Format\n"
        "Respond only with valid JSON in this exact structure:\n\n"
        "```json\n"
        "{\n"
        '  "answer": "YES" | "NO",\n'
        '  "confidence": "High" | "Medium" | "Low",\n'
        '  "company_id": "CompanyId",\n'
        '  "outreach_message": "one sentence addressing the company",\n'
        '  "reasoning": "multi-sentence reasoning according to guidelines above."\n'
        "}\n"
        "```\n\n"
        'Respond with { "answer": "YES"} even if you don\'t come to a result. '
        "The default answer is always yes."
    )


def _parse_filter_result(raw: str, company_id: str) -> FilterResult:
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        raise ValueError(
            f"parse_error: no JSON object found in LLM output for company {company_id}"
        )
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        raise ValueError(
            f"parse_error: invalid JSON in LLM output for company {company_id}"
        ) from e

    answer = str(data.get("answer", "YES")).upper()
    if answer not in ("YES", "NO"):
        answer = "YES"

    confidence_raw = str(data.get("confidence", "Low")).capitalize()
    try:
        confidence = Confidence(confidence_raw)
    except ValueError:
        confidence = Confidence.LOW

    return FilterResult(
        answer=answer,
        confidence=confidence,
        company_id=str(data.get("company_id", company_id)),
        outreach_message=str(data.get("outreach_message", "")),
        reasoning=str(data.get("reasoning", "")),
    )


async def _create_result_artifact(result: FilterResult) -> None:
    decision = (
        "Included (active)"
        if result.answer == "YES" and result.confidence != Confidence.LOW
        else "Excluded"
    )
    failure_row = (
        f"| Process Failure | {result.failure_reason} |\n"
        if result.failure_reason
        else ""
    )
    markdown = (
        f"## Filter Result: {result.company_id}\n\n"
        f"| Field | Value |\n|-------|-------|\n"
        f"| Answer | {result.answer} |\n"
        f"| Confidence | {result.confidence.value} |\n"
        f"| Decision | {decision} |\n"
        f"{failure_row}\n"
        f"### Outreach Message\n{result.outreach_message or 'N/A'}\n\n"
        f"### Reasoning\n{result.reasoning}\n"
    )
    try:
        await create_markdown_artifact(
            key=f"filter-result-{result.company_id}",
            markdown=markdown,
            description=f"Filter result for company {result.company_id}",
        )
    except Exception:
        logger.warning(
            "create_result_artifact.failed", company_id=result.company_id
        )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(
    name="evaluate_company",
    task_run_name="evaluate_company {company_id}",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    timeout_seconds=120,
)
async def evaluate_company_task(
    description: str, criteria: str, company_id: str
) -> FilterResult:
    from equimatch_agent import build_agent

    prompt = _build_evaluation_prompt(description, criteria, company_id)

    try:
        agent = await build_agent("plain@v1", model_override=_MODEL_OVERRIDE)
        result = await agent.run(prompt)
        raw = result.output or ""
    except Exception as e:
        logger.warning(
            "evaluate_company.llm_failed", company_id=company_id, error=str(e)
        )
        raise

    parsed = _parse_filter_result(raw, company_id)
    logger.info(
        "evaluate_company.done",
        company_id=company_id,
        answer=parsed.answer,
        confidence=parsed.confidence.value,
    )
    return parsed


@task(
    name="fetch_pipeline",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=5),
    retries=2,
    retry_delay_seconds=5,
)
async def fetch_pipeline_task(pipeline_id: str) -> dict[str, Any]:
    data = await client.fetch_pipeline(pipeline_id)
    logger.info("fetch_pipeline.done", pipeline_id=pipeline_id)
    return data


@task(
    name="fetch_companies",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
)
async def fetch_companies_task(
    pipeline_id: str, flow_status: str | None = None
) -> list[dict[str, Any]]:
    companies = await client.fetch_companies(pipeline_id, flow_status=flow_status)
    logger.info("fetch_companies.done", pipeline_id=pipeline_id, count=len(companies))
    return companies


@task(
    name="fetch_company",
    task_run_name="fetch_company {company_id}",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
)
async def fetch_company_task(company_id: str) -> dict[str, Any]:
    data = await client.fetch_company(company_id)
    logger.info("fetch_company.done", company_id=company_id)
    return data


@task(
    name="fetch_website",
    task_run_name="fetch_website {website_id}",
    retries=2,
    retry_delay_seconds=5,
)
async def fetch_website_task(website_id: str) -> dict[str, Any]:
    return await client.fetch_website(website_id)


@task(
    name="update_status",
    task_run_name="update_status {company_id} -> {status}",
    retries=2,
    retry_delay_seconds=5,
)
async def update_status_task(
    pipeline_id: str,
    company_id: str,
    status: str,
    clause: str | None = None,
) -> None:
    await client.update_company_status(pipeline_id, company_id, status, clause)
    logger.info(
        "update_status.done",
        pipeline_id=pipeline_id,
        company_id=company_id,
        status=status,
    )


@task(
    name="update_flow_status",
    task_run_name="update_flow_status {company_id} -> {flow_status}",
    retries=2,
    retry_delay_seconds=5,
)
async def update_flow_status_task(
    pipeline_id: str, company_id: str, flow_status: str
) -> None:
    await client.update_flow_status(pipeline_id, company_id, flow_status)
    logger.info(
        "update_flow_status.done",
        pipeline_id=pipeline_id,
        company_id=company_id,
        flow_status=flow_status,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@task(name="apply_filter_decision", task_run_name="apply_filter_decision {result.company_id}")
async def apply_filter_decision_task(
    result: FilterResult,
    pipeline_id: str,
) -> None:
    company_id = result.company_id

    if result.answer == "NO":
        logger.info(
            "filter_decision.excluded",
            company_id=company_id,
            reason="answer_no",
            confidence=result.confidence.value,
        )
        events.capture("company-excluded-from-pipeline", {
            "id": company_id,
            "pipeline_id": pipeline_id,
            "reasoning": result.outreach_message,
            "confidence": result.confidence.value,
            "type": "company",
            "explanation": result.reasoning,
        })
        await asyncio.gather(
            update_status_task(pipeline_id, company_id, "excluded"),
            update_flow_status_task(pipeline_id, company_id, "Z2"),
        )
        return

    if result.confidence == Confidence.LOW:
        logger.info(
            "filter_decision.excluded_low_confidence",
            company_id=company_id,
            confidence=result.confidence.value,
        )
        events.capture("company-excluded-from-pipeline", {
            "id": company_id,
            "pipeline_id": pipeline_id,
            "reasoning": result.outreach_message,
            "confidence": result.confidence.value,
            "type": "company",
            "reason": "confidence-low-included",
            "explanation": result.reasoning,
        })
        await asyncio.gather(
            update_status_task(pipeline_id, company_id, "excluded"),
            update_flow_status_task(pipeline_id, company_id, "Z2"),
        )
        return

    logger.info(
        "filter_decision.active",
        company_id=company_id,
        confidence=result.confidence.value,
    )
    events.capture("company-fit-message", {
        "id": company_id,
        "message": result.outreach_message,
        "pipeline_id": pipeline_id,
        "confidence": result.confidence.value,
        "type": "company",
        "reasoning": result.reasoning,
    })
    await asyncio.gather(
        update_status_task(pipeline_id, company_id, "active", clause=result.outreach_message),
        update_flow_status_task(pipeline_id, company_id, "C3.1"),
    )


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


@flow(
    name="filter_single_company",
    flow_run_name="filter company {company_id}",
    log_prints=True,
    retries=2,
    retry_delay_seconds=30,
)
async def filter_single_company(
    pipeline_id: str, company_id: str
) -> FilterResult:
    """Evaluate a single company against a pipeline's investment criteria.

    Core processing unit: fetches criteria from the pipeline, evaluates the
    company, and updates pipeline status/flow_status accordingly.
    """
    setup_logging()
    logger.info("filter_single_company.start", pipeline_id=pipeline_id, company_id=company_id)

    pipeline_data = await fetch_pipeline_task(pipeline_id)
    search_criteria = pipeline_data["pipeline"]["searchCriteria"]

    await update_status_task(pipeline_id, company_id, "processing")

    company_data = await fetch_company_task(company_id)
    company = company_data.get("company", {})
    websites = company.get("websites") or []

    if not websites:
        logger.warning(
            "filter_single_company.no_websites", company_id=company_id
        )
        raise ValueError(f"data_missing: company {company_id} has no websites")

    website_id = websites[0].get("websiteId") or websites[0].get("id")
    if not website_id:
        logger.warning(
            "filter_single_company.no_website_id", company_id=company_id
        )
        raise ValueError(
            f"data_missing: website entry for company {company_id} has no ID"
        )

    logger.info("filter_single_company.fetching_website", company_id=company_id, website_id=str(website_id))
    website_data = await fetch_website_task(str(website_id))
    description = website_data.get("website", {}).get("description") or company.get("description","")

    if not description:
        logger.info(
            "filter_single_company.empty_description",
            company_id=company_id,
            website_id=str(website_id),
        )
        events.capture("website-description-empty", {
            "id": str(website_id),
            "company_id": company_id,
            "type": "website",
        })
        raise ValueError(
            f"data_missing: website description is empty for company {company_id}"
        )

    logger.info("filter_single_company.evaluating", company_id=company_id)
    result = await evaluate_company_task(
        description=description,
        criteria=search_criteria,
        company_id=company_id,
    )
    await apply_filter_decision_task(result, pipeline_id)
    await _create_result_artifact(result)
    return result


@flow(
    name="filter_companies",
    flow_run_name="filter_companies chunk {chunk_index} ({len_companies} companies)",
    log_prints=True,
)
async def filter_companies(
    pipeline_id: str,
    company_ids: list[str],
    chunk_index: int = 0,
    len_companies: int = 0,
) -> ChunkOutcome:
    """Filter a chunk of companies against a pipeline's investment criteria.

    Launches each company as an independent ``filter-company`` deployment run via
    run_deployment, so a single company's failure is isolated (the rest of the
    chunk continues) and that company's run is auto-retried and retryable from the
    Prefect UI. Concurrency is gated by the deployment's own concurrency limit.
    """
    setup_logging()
    logger.info(
        "filter_companies.start",
        pipeline_id=pipeline_id,
        chunk_index=chunk_index,
        count=len(company_ids),
    )

    async def _run(cid: str) -> FilterResult | Exception:
        flow_run = await run_deployment(
            name=_FILTER_COMPANY_DEPLOYMENT,
            parameters={"pipeline_id": pipeline_id, "company_id": cid},
        )
        state = flow_run.state
        if state is None or not state.is_completed():
            message = state.message if state is not None else "no state"
            raise RuntimeError(
                f"company {cid} run did not complete: "
                f"{state.type if state is not None else 'UNKNOWN'} ({message})"
            )
        # Completed — recover the FilterResult for summary purposes. Status
        # updates and artifacts already happened inside the company's own run, so
        # failure to fetch the result here is non-fatal.
        result = state.result(raise_on_failure=False)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, FilterResult):
            raise RuntimeError(
                f"company {cid} run completed without a FilterResult"
            )
        return result

    gathered = await asyncio.gather(
        *[_run(cid) for cid in company_ids],
        return_exceptions=True,
    )

    outcome = ChunkOutcome()
    for cid, item in zip(company_ids, gathered):
        if isinstance(item, FilterResult):
            outcome.results.append(item)
        else:
            error = str(item)
            outcome.failures.append((cid, error))
            logger.error(
                "filter_companies.company_failed",
                company_id=cid,
                error=error,
            )

    logger.info(
        "filter_companies.done",
        pipeline_id=pipeline_id,
        processed=len(outcome.results),
        failed=len(outcome.failures),
    )

    try:
        table_data = [
            {
                "company_id": r.company_id,
                "answer": r.answer,
                "confidence": r.confidence.value,
                "decision": (
                    "active"
                    if r.answer == "YES" and r.confidence != Confidence.LOW
                    else "excluded"
                ),
                "status": "completed",
                "detail": (
                    r.reasoning[:120] + "..."
                    if len(r.reasoning) > 120
                    else r.reasoning
                ),
            }
            for r in outcome.results
        ] + [
            {
                "company_id": cid,
                "answer": "",
                "confidence": "",
                "decision": "failed",
                "status": "failed",
                "detail": error[:120] + "..." if len(error) > 120 else error,
            }
            for cid, error in outcome.failures
        ]
        await create_table_artifact(
            key=f"filter-batch-{pipeline_id}",
            table=table_data,
            description=f"Batch filter results for pipeline {pipeline_id} — processed: {len(outcome.results)}, failed: {len(outcome.failures)}",
        )
    except Exception:
        logger.warning("create_batch_artifact.failed", pipeline_id=pipeline_id)

    return outcome


@flow(name="filter_pipeline", log_prints=True)
async def filter_pipeline(
    pipeline_id: str, flow_status: str | None = "C2.2"
) -> None:
    """Filter all companies in a pipeline against its investment criteria.

    Fetches companies, optionally filters by flowStatus, then delegates
    to filter_companies as a subflow.
    """
    setup_logging()

    events.capture("filter-pipeline-search-criteria-webhook", {
        "id": pipeline_id,
        "parameters": json.dumps({"pipeline_id": pipeline_id, "flow_status": flow_status}),
        "type": "pipeline",
    })

    companies = await fetch_companies_task(pipeline_id, flow_status=flow_status)
    if not companies:
        logger.info("filter_pipeline.no_companies", pipeline_id=pipeline_id)
        return

    company_ids = [str(c["id"]) for c in companies]

    if not company_ids:
        logger.info(
            "filter_pipeline.no_matching_companies",
            pipeline_id=pipeline_id,
            flow_status=flow_status,
        )
        return

    chunk_size = settings.chunk_size
    chunks = [company_ids[i:i + chunk_size] for i in range(0, len(company_ids), chunk_size)]
    logger.info(
        "filter_pipeline.start",
        pipeline_id=pipeline_id,
        count=len(company_ids),
        chunks=len(chunks),
        chunk_size=chunk_size,
        flow_status=flow_status,
    )

    results: list[FilterResult] = []
    failures: list[tuple[str, str]] = []
    for i, chunk in enumerate(chunks):
        chunk_outcome = await filter_companies(
            pipeline_id,
            chunk,
            chunk_index=i,
            len_companies=len(chunk),
        )
        results.extend(chunk_outcome.results)
        failures.extend(chunk_outcome.failures)

    try:
        accepted = sum(
            1
            for r in results
            if r.answer == "YES" and r.confidence != Confidence.LOW
        )
        excluded = len(results) - accepted
        failed = len(failures)
        high = sum(1 for r in results if r.confidence == Confidence.HIGH)
        medium = sum(1 for r in results if r.confidence == Confidence.MEDIUM)
        low = sum(1 for r in results if r.confidence == Confidence.LOW)

        # Companies whose run FAILED (external dependency failure). These are not
        # defaulted to included/excluded — they remain Failed runs to be retried.
        failed_rows = "".join(
            f"| {cid} | {error} |\n" for cid, error in failures
        )
        process_failures_section = (
            f"### Failed Runs ({len(failures)})\n\n"
            + (
                f"| Company ID | Failure Reason |\n|------------|----------------|\n{failed_rows}"
                if failures
                else "_No failed runs._\n"
            )
            + "\n"
        )

        summary = (
            f"## Pipeline Filter Summary\n\n"
            f"**Pipeline ID:** {pipeline_id}\n\n"
            f"**Flow Status Filter:** {flow_status or 'all'}\n\n"
            f"**Total Companies:** {len(company_ids)}\n\n"
            f"### Results\n\n"
            f"| Metric | Count |\n|--------|-------|\n"
            f"| Accepted (active) | {accepted} |\n"
            f"| Excluded | {excluded} |\n"
            f"| Failed (no result) | {failed} |\n\n"
            f"### Confidence Distribution\n\n"
            f"| Confidence | Count |\n|------------|-------|\n"
            f"| High | {high} |\n"
            f"| Medium | {medium} |\n"
            f"| Low | {low} |\n\n"
            f"{process_failures_section}"
        )
        await create_markdown_artifact(
            key=f"filter-pipeline-{pipeline_id}",
            markdown=summary,
            description=f"Pipeline filter summary for {pipeline_id}",
        )
    except Exception:
        logger.warning("create_pipeline_artifact.failed", pipeline_id=pipeline_id)

    events.flush()
    logger.info("filter_pipeline.done", pipeline_id=pipeline_id)
