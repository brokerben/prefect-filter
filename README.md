# prefect-filter

LLM-powered company filtering pipeline for Equimatch. Evaluates pipeline companies against a client's investment criteria and routes them to active or excluded status.

## Overview

Uses Prefect 3.x to orchestrate an async pipeline that:

1. Fetches companies from the Equimatch backend API
2. Extracts each company's website description
3. Evaluates the description against investment criteria using an LLM (via `equimatch-agent`)
4. Updates company status (`active` / `excluded`) and flow status in the backend

## Flow Architecture

Three nested async flows form a processing hierarchy:

```
filter_pipeline
 └─ filter_companies          (concurrent batch processor)
     └─ filter_single_company  (per-company evaluation)
```

### filter_pipeline

Top-level entry point. Fetches all companies in a pipeline, optionally filters by `flowStatus`, then delegates to `filter_companies`.

### filter_companies

Batch processor. Runs `filter_single_company` concurrently for each company ID, bounded by `MAX_CONCURRENCY`.

### filter_single_company

Core processing unit. For a single company:

1. Fetches pipeline criteria and company data
2. Retrieves the company's website description
3. Sends an evaluation prompt to the LLM
4. Applies the accept/exclude decision and updates the backend

### Decision Logic

| Condition                         | Status     | Flow Status |
|-----------------------------------|------------|-------------|
| Answer = YES, Confidence = High   | `active`   | `C3.1`      |
| Answer = YES, Confidence = Medium | `active`   | `C3.1`      |
| Answer = YES, Confidence = Low    | `excluded` | `Z2`        |
| Answer = NO                       | `excluded` | `Z2`        |
| No website / empty description    | `excluded` | `X2`        |
| Processing error                  | `excluded` | `X2`        |

## Artifacts

Each flow run produces Prefect artifacts visible in the Prefect UI:

- **Per-company result** (key: `filter-result-{company_id}`) — Markdown showing answer, confidence, decision, outreach message, and reasoning.
- **Batch summary** (key: `filter-batch-{pipeline_id}`) — Table of all company results with answer, confidence, decision, and truncated reasoning.
- **Pipeline summary** (key: `filter-pipeline-{pipeline_id}`) — Markdown with overall stats: accepted/excluded/failed counts and confidence distribution.

## Configuration

All configuration is via environment variables (or a `.env` file):

| Variable                      | Required | Default | Description                          |
|-------------------------------|----------|---------|--------------------------------------|
| `BACKEND_BASE_URI`            | Yes      |         | Equimatch API base URL               |
| `EQUIMATCH_API_KEY`           | Yes      |         | API authentication key               |
| `OPENROUTER_API_KEY`          | Yes      |         | LLM provider API key                 |
| `PIPELINE_ID`                 | Yes*     |         | Pipeline to process (*CLI only)      |
| `FLOW_STATUS`                 | No       |         | Filter companies by flow status      |
| `MAX_CONCURRENCY`             | No       | `5`     | Max parallel company evaluations     |
| `RUN_LIMIT`                   | No       | `5`     | Prefect serve run limit              |
| `PHOENIX_COLLECTOR_ENDPOINT`  | No       |         | Observability collector endpoint     |
| `PHOENIX_API_KEY`             | No       |         | Phoenix authentication key           |

## Usage

### One-shot run

```bash
PIPELINE_ID=<uuid> filter-pipeline
```

### With flow status filter

```bash
PIPELINE_ID=<uuid> FLOW_STATUS=B2 filter-pipeline
```

### As a Prefect deployment (long-running)

```bash
serve
```

## Development

Requires Python 3.12+.

```bash
pip install -e ".[dev]"
pytest
```
