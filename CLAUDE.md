# CLAUDE.md

This file preserves architectural decisions and project conventions across Claude Code sessions.
In agentic workflows, context is lost between conversations — decisions recorded here prevent
the same design questions from being relitigated and keep the model consistent across sessions.

---

## Project Overview

This is a dbt project modelling a synthetic contractor marketplace platform. Raw data is
provided as static parquet files (users, jobs, services, contractor profiles, payouts,
adjustments) and transformed through a three-layer dbt architecture — staging, intermediate,
and marts — into fact tables, dimension tables, and business summary models. The project was
built for agentic data modelling evaluation using DuckDB as the query engine.

---

## Architectural Decisions

### Normalization belongs in a dedicated CTE, not inline
All string normalization (lowercasing, trimming, value mapping) must be isolated in a CTE
named `normalized` at the top of the intermediate model. Do not embed `lower()`, `trim()`, or
`case` normalization logic inline within downstream CTEs — this makes auditing transformations
harder and scatters the normalization boundary across the model.

### No `current_timestamp` or runtime-derived metrics in models
Models must be deterministic and reproducible. Do not use `current_timestamp`, `now()`,
`current_date`, or any function that returns a different value on each run. Derived metrics
such as "days since signup" or "age of record" must not appear in mart models — they belong
in BI tooling or ad-hoc queries where the reference date is explicit.

### Null semantics for inactive records are intentional — do not coalesce to zero
Summary models (e.g. `contractor_performance_summary`, `customer_health_summary`) leave
rate and average columns NULL for entities with no activity (e.g. a contractor with no
completed jobs has NULL for `avg_job_revenue_usd`). This is intentional: NULL means "not
applicable" and is semantically distinct from zero. Do not coalesce these columns to 0.
Volume and total columns (counts, sums) are coalesced to 0 because zero activity is a
valid and meaningful value.

### Mart models reference the intermediate layer, not staging directly
Marts must always `ref()` intermediate models (`int_*`), never staging models (`stg_*`).
Staging models are a thin ingestion boundary with minimal transformation. All normalisation,
type casting, and business logic lives in the intermediate layer and must be present before
data reaches the marts.

---

## Layer Conventions

### Staging (`models/staging/`)
- One model per source table, named `stg_<source_table>`.
- Thin pass-throughs: select all columns, rename where necessary, cast types if needed.
- No business logic, no joins, no derived columns.
- Tests: `unique` + `not_null` on primary keys; `not_null` on columns that must never be
  missing at source. Enum values are NOT tested here — normalisation happens in intermediate.
- Schema documented in `staging/schema.yml`.

### Intermediate (`models/intermediate/`)
- One model per staging model, named `int_<entity>_enriched`.
- Responsibilities: normalise strings, cast to final types, derive boolean flags, classify
  enums, apply business rules.
- Use a `normalized` CTE as the first transformation step for any string cleaning.
- No aggregation. No joins across domains (e.g. do not join jobs to users here).
- Tests: `accepted_values` on all normalised enum columns; `unique` + `not_null` on PKs.
- Schema documented in `intermediate/schema.yml`.

### Marts (`models/marts/`)
- Fact tables (`fct_*`): one row per business event (job, payout, adjustment). Join
  intermediate models to denormalise snapshot attributes (category, tier, city) onto the
  fact. Include pre-aggregated adjustment and payout rollups on `fct_jobs`.
- Dimension tables (`dim_*`): one row per entity (user, contractor, service). Derived from
  intermediate models; include all descriptive attributes needed for slicing and filtering.
- Summary tables (no prefix): pre-aggregated grain (e.g. per contractor, per customer, per
  category). Join facts to dimensions; include volume counts, revenue totals, and rate metrics.
- Tests: relationship integrity (`relationships`) on all FK columns; `accepted_values` on
  denormalised enum columns; `unique` + `not_null` on grain keys.
- Schema documented in `marts/schema.yml`.

---

## Test Conventions

| Layer        | Test types applied                                                      |
|--------------|-------------------------------------------------------------------------|
| Staging      | `unique`, `not_null` on PKs; `not_null` on required source columns      |
| Intermediate | Above + `accepted_values` on all normalised enums                       |
| Marts        | Above + `relationships` on FK columns; 4 singular tests for invariants  |

### Singular tests (`tests/`)
Singular tests assert cross-column or cross-row invariants that generic schema tests cannot
express. Each file must:
- Be named `assert_<what_must_be_true>.sql`
- Open with a comment block explaining the invariant and why it matters if violated
- Return rows only on failure (dbt convention: test passes when 0 rows returned)

Current singular tests:
- `assert_jobs_status_mutually_exclusive.sql` — a job cannot be both completed and canceled
- `assert_completed_at_after_requested_at.sql` — completed_at must not precede requested_at
- `assert_refund_amounts_non_negative.sql` — total_refund_usd must be a positive magnitude
- `assert_adjustment_amounts_positive.sql` — adjustment_amount_usd must be a positive magnitude
