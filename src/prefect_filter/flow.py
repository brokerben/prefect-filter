"""Prefect flow: filter pipeline companies against investment criteria via LLM."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import timedelta
from typing import Any

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.tasks import task_input_hash

from prefect_filter import client, posthog
from prefect_filter.config import settings
from prefect_filter.log import get_logger, setup_logging
from prefect_filter.models import Confidence, FilterResult

logger = get_logger(__name__)

_MODEL_OVERRIDE = "google/gemma-4-31b-it"


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
        return FilterResult(
            answer="YES",
            confidence=Confidence.LOW,
            company_id=company_id,
            outreach_message="",
            reasoning="Failed to parse LLM output; defaulting to YES.",
        )
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return FilterResult(
            answer="YES",
            confidence=Confidence.LOW,
            company_id=company_id,
            outreach_message="",
            reasoning="Failed to parse LLM JSON output; defaulting to YES.",
        )

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
    markdown = (
        f"## Filter Result: {result.company_id}\n\n"
        f"| Field | Value |\n|-------|-------|\n"
        f"| Answer | {result.answer} |\n"
        f"| Confidence | {result.confidence.value} |\n"
        f"| Decision | {decision} |\n\n"
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


@task(name="evaluate_company", task_run_name="evaluate_company {company_id}")
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
        return FilterResult(
            answer="YES",
            confidence=Confidence.LOW,
            company_id=company_id,
            outreach_message="",
            reasoning=f"LLM call failed: {e}; defaulting to YES.",
        )

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
)
async def fetch_pipeline_task(pipeline_id: str) -> dict[str, Any]:
    data = await client.fetch_pipeline(pipeline_id)
    logger.info("fetch_pipeline.done", pipeline_id=pipeline_id)
    return data


@task(name="fetch_companies")
async def fetch_companies_task(
    pipeline_id: str, flow_status: str | None = None
) -> list[dict[str, Any]]:
    companies = await client.fetch_companies(pipeline_id, flow_status=flow_status)
    logger.info("fetch_companies.done", pipeline_id=pipeline_id, count=len(companies))
    return companies


@task(name="fetch_company", task_run_name="fetch_company {company_id}")
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


async def _apply_filter_decision(
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
        posthog.capture(
            distinct_id=company_id,
            event="company-excluded-from-pipeline",
            properties={
                "pipeline_id": pipeline_id,
                "reasoning": result.outreach_message,
                "confidence": result.confidence.value,
                "type": "company",
                "explanation": result.reasoning,
            },
        )
        await update_status_task(pipeline_id, company_id, "excluded")
        await update_flow_status_task(pipeline_id, company_id, "Z2")
        return

    if result.confidence == Confidence.LOW:
        logger.info(
            "filter_decision.excluded_low_confidence",
            company_id=company_id,
            confidence=result.confidence.value,
        )
        posthog.capture(
            distinct_id=company_id,
            event="company-excluded-from-pipeline",
            properties={
                "pipeline_id": pipeline_id,
                "reasoning": result.outreach_message,
                "confidence": result.confidence.value,
                "type": "company",
                "reason": "confidence-low-included",
                "explanation": result.reasoning,
            },
        )
        await update_flow_status_task(pipeline_id, company_id, "Z2")
        await update_status_task(pipeline_id, company_id, "excluded")
        return

    logger.info(
        "filter_decision.active",
        company_id=company_id,
        confidence=result.confidence.value,
    )
    posthog.capture(
        distinct_id=company_id,
        event="company-fit-message",
        properties={
            "message": result.outreach_message,
            "pipeline_id": pipeline_id,
            "confidence": result.confidence.value,
            "type": "company",
            "reasoning": result.reasoning,
        },
    )
    await update_status_task(
        pipeline_id, company_id, "active", clause=result.outreach_message
    )
    await update_flow_status_task(pipeline_id, company_id, "C3.1")


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


@flow(name="filter_single_company", log_prints=True)
async def filter_single_company(
    pipeline_id: str, company_id: str
) -> FilterResult:
    """Evaluate a single company against a pipeline's investment criteria.

    Core processing unit: fetches criteria from the pipeline, evaluates the
    company, and updates pipeline status/flow_status accordingly.
    """
    setup_logging()

    pipeline_data = await fetch_pipeline_task(pipeline_id)
    search_criteria = pipeline_data["pipeline"]["searchCriteria"]

    try:
        await update_status_task(pipeline_id, company_id, "processing")

        company_data = await fetch_company_task(company_id)
        company = company_data.get("company", {})
        websites = company.get("websites") or []

        if not websites:
            logger.warning(
                "filter_single_company.no_websites", company_id=company_id
            )
            await update_status_task(pipeline_id, company_id, "excluded")
            await update_flow_status_task(pipeline_id, company_id, "X2")
            result = FilterResult(
                answer="YES",
                confidence=Confidence.LOW,
                company_id=company_id,
                outreach_message="",
                reasoning="No website found for company; defaulting to YES.",
            )
            await _create_result_artifact(result)
            return result

        website_id = websites[0].get("websiteId") or websites[0].get("id")
        if not website_id:
            logger.warning(
                "filter_single_company.no_website_id", company_id=company_id
            )
            await update_status_task(pipeline_id, company_id, "excluded")
            await update_flow_status_task(pipeline_id, company_id, "X2")
            result = FilterResult(
                answer="YES",
                confidence=Confidence.LOW,
                company_id=company_id,
                outreach_message="",
                reasoning="No website ID found for company; defaulting to YES.",
            )
            await _create_result_artifact(result)
            return result

        website_data = await fetch_website_task(str(website_id))
        description = website_data.get("website", {}).get("description") or ""

        if not description:
            logger.info(
                "filter_single_company.empty_description",
                company_id=company_id,
                website_id=str(website_id),
            )
            posthog.capture(
                distinct_id=str(website_id),
                event="website-description-empty",
                properties={"company_id": company_id, "type": "website"},
            )
            await update_status_task(pipeline_id, company_id, "excluded")
            await update_flow_status_task(pipeline_id, company_id, "X2")
            result = FilterResult(
                answer="YES",
                confidence=Confidence.LOW,
                company_id=company_id,
                outreach_message="",
                reasoning="Website description is empty; defaulting to YES.",
            )
            await _create_result_artifact(result)
            return result

        result = await evaluate_company_task(
            description=description,
            criteria=search_criteria,
            company_id=company_id,
        )
        await _apply_filter_decision(result, pipeline_id)
        await _create_result_artifact(result)
        return result

    except Exception as e:
        logger.error(
            "filter_single_company.failed",
            company_id=company_id,
            error=str(e),
        )
        try:
            await update_status_task(pipeline_id, company_id, "excluded")
            await update_flow_status_task(pipeline_id, company_id, "X2")
        except Exception:
            pass
        result = FilterResult(
            answer="YES",
            confidence=Confidence.LOW,
            company_id=company_id,
            outreach_message="",
            reasoning=f"Processing failed: {e}; defaulting to YES.",
        )
        await _create_result_artifact(result)
        return result


@flow(name="filter_companies", log_prints=True)
async def filter_companies(
    pipeline_id: str, company_ids: list[str]
) -> list[FilterResult]:
    """Filter specific companies against a pipeline's investment criteria.

    Invokes filter_single_company as a subflow for each company, with
    concurrency limited by settings.max_concurrency.
    """
    setup_logging()

    logger.info(
        "filter_companies.start",
        pipeline_id=pipeline_id,
        count=len(company_ids),
    )
    semaphore = asyncio.Semaphore(settings.max_concurrency)

    async def _run(cid: str) -> FilterResult | None:
        async with semaphore:
            try:
                return await filter_single_company(pipeline_id, cid)
            except Exception as e:
                logger.error(
                    "filter_companies.subflow_failed",
                    company_id=cid,
                    error=str(e),
                )
                return None

    results = await asyncio.gather(
        *[_run(cid) for cid in company_ids],
        return_exceptions=True,
    )

    valid_results = [r for r in results if isinstance(r, FilterResult)]
    failed = len(company_ids) - len(valid_results)
    logger.info(
        "filter_companies.done",
        pipeline_id=pipeline_id,
        processed=len(valid_results),
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
                "reasoning": (
                    r.reasoning[:120] + "..."
                    if len(r.reasoning) > 120
                    else r.reasoning
                ),
            }
            for r in valid_results
        ]
        await create_table_artifact(
            key=f"filter-batch-{pipeline_id}",
            table=table_data,
            description=f"Batch filter results for pipeline {pipeline_id}",
        )
    except Exception:
        logger.warning("create_batch_artifact.failed", pipeline_id=pipeline_id)

    return valid_results


@flow(name="filter_pipeline", log_prints=True)
async def filter_pipeline(
    pipeline_id: str, flow_status: str | None = "C2.2"
) -> None:
    """Filter all companies in a pipeline against its investment criteria.

    Fetches companies, optionally filters by flowStatus, then delegates
    to filter_companies as a subflow.
    """
    setup_logging()

    posthog.capture(
        distinct_id=pipeline_id,
        event="filter-pipeline-search-criteria-webhook",
        properties={
            "parameters": json.dumps({"pipeline_id": pipeline_id, "flow_status": flow_status}),
            "type": "pipeline",
        },
    )

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

    logger.info(
        "filter_pipeline.start",
        pipeline_id=pipeline_id,
        count=len(company_ids),
        flow_status=flow_status,
    )

    results = await filter_companies(pipeline_id, company_ids)

    try:
        accepted = sum(
            1
            for r in results
            if r.answer == "YES" and r.confidence != Confidence.LOW
        )
        excluded = len(results) - accepted
        failed = len(company_ids) - len(results)
        high = sum(1 for r in results if r.confidence == Confidence.HIGH)
        medium = sum(1 for r in results if r.confidence == Confidence.MEDIUM)
        low = sum(1 for r in results if r.confidence == Confidence.LOW)

        summary = (
            f"## Pipeline Filter Summary\n\n"
            f"**Pipeline ID:** {pipeline_id}\n\n"
            f"**Flow Status Filter:** {flow_status or 'all'}\n\n"
            f"**Total Companies:** {len(company_ids)}\n\n"
            f"### Results\n\n"
            f"| Metric | Count |\n|--------|-------|\n"
            f"| Accepted (active) | {accepted} |\n"
            f"| Excluded | {excluded} |\n"
            f"| Failed | {failed} |\n\n"
            f"### Confidence Distribution\n\n"
            f"| Confidence | Count |\n|------------|-------|\n"
            f"| High | {high} |\n"
            f"| Medium | {medium} |\n"
            f"| Low | {low} |\n"
        )
        await create_markdown_artifact(
            key=f"filter-pipeline-{pipeline_id}",
            markdown=summary,
            description=f"Pipeline filter summary for {pipeline_id}",
        )
    except Exception:
        logger.warning("create_pipeline_artifact.failed", pipeline_id=pipeline_id)

    posthog.flush()
    logger.info("filter_pipeline.done", pipeline_id=pipeline_id)
