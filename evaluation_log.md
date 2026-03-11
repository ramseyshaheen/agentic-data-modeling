### Claude Code Evaluation Log


## Part 1 - Intermediary Data Models


# Claude Code Evaluation Log
## Project: Agentic Data Modeling with dbt + DuckDB


Tracks prompt quality, output strengths, and gaps for each Claude Code task. Forms the evidence base for the project README thesis.


---


## Part 1 - Intermediary Modeling


# Task 1.1 — Build intermediate model for jobs data


**Prompt:**
> Read stg_jobs.sql, sample the underlying data, create int_jobs_enriched.sql in a models/intermediate/ folder with clean types, renamed columns, and null handling. Run dbt run to confirm it builds.


**Successes:**
- Explored schema and nulls before writing any SQL
- Used `ref('stg_jobs')` correctly
- Explicit type casting on all columns
- Derived boolean flags (`is_completed`, `is_canceled`, `canceled_by`)
- Null semantics matched business context (e.g. actual_hours NULL for canceled jobs)


**Needs Improvement:**
- Status normalization bug — compared boolean flags against raw `job_status` instead of the normalized value, breaking on mixed-case or padded strings
- `coalesce(gross_amount, 0.00)` collapses "unknown revenue" and "no revenue" into the same value
- `date_diff('hour', ...)` truncates to whole hours — precision loss risk for billing or SLA metrics


**Summary:**
Claude Code built a passing intermediate model with explicit type casting, null handling, and derived boolean flags. Two business logic defects were present that did not cause build failures and were only caught through external review.


---


# Task 1.2 — Build intermediate models for remaining 5 tables


**Prompt:**
> Following the same pattern as int_jobs_enriched.sql, sample each parquet file and create intermediate models for stg_users, stg_contractor_profiles, stg_services, stg_payouts, and stg_adjustments. Use ref(), apply explicit type casting, null handling, and meaningful derived columns. Run dbt run --select intermediate to confirm all pass.


**Successes:**
- Status normalization bug from Task 1 did not repeat — `lower(trim())` applied consistently in both column definitions and boolean flags
- `int_payouts` derived `implied_gross_usd` and `platform_fee_usd` from payout math, using `nullif()` to guard against division by zero
- `int_contractor_profiles` derived `is_eligible_to_work` by combining two boolean eligibility checks
- `int_services` bucketed hourly rates into tiers unprompted, adding analytical value
- All 5 models passed `dbt run` successfully


**Needs Improvement:**
- Repeated normalization logic — `lower(trim())` is written out multiple times per model rather than centralized in a CTE, creating a maintenance and drift risk (recurring pattern across all models)
- `days_since_signup` in `int_users` uses `current_timestamp`, making it non-deterministic — breaks snapshot reproducibility and complicates backfills
- `implied_gross_usd` in `int_payouts` computes the same division expression twice instead of reusing a CTE alias — risk of values diverging if one is edited
- Boolean expressions like `is_eligible_to_work` don't guard against NULL inputs — could silently return NULL instead of FALSE
- Rate tier thresholds (`75`, `125`) hardcoded in intermediate layer — business segmentation logic arguably belongs in marts


**Summary:**
All 5 models passed dbt run. Normalization logic was repeated inline across models rather than centralized, creating a recurring maintenance risk. One non-deterministic metric and a double computation in payouts were the most actionable issues identified.


---


# Task 1.3 — Refactor all 6 intermediate models


**Prompt:**
> Refactor all 6 int models to address three recurring issues: centralize lower(trim()) normalization into a single CTE per model, remove the non-deterministic days_since_signup metric from int_users, and fix financial calculation fragility in int_jobs and int_payouts. Stop and ask for input if business context is needed.


**Successes:**
- Introduced a consistent `normalized` CTE pattern across all 6 models — normalization logic now written once, eliminating drift risk
- Removed `days_since_signup` from `int_users` with a clear inline comment explaining the decision
- Refactored `int_payouts` to compute `implied_gross` once in a dedicated CTE, eliminating double computation
- Removed `coalesce(gross_amount, 0.00)` from `int_jobs` — null preserved, marts decide
- Paused twice mid-task to ask clarifying business questions rather than making assumptions


**Needs Improvement:**
- `is_eligible_to_work` in `int_contractor_profiles` still doesn't guard against NULL inputs — can silently return NULL instead of FALSE


**Summary:**
All flagged issues were addressed across all 6 models in a single pass. A consistent `normalized` CTE pattern was introduced, `days_since_signup` was removed, and financial calculation fragility was resolved. Claude Code paused twice to ask clarifying questions before proceeding on ambiguous decisions.


---


## Part 2 - Downstream Mart and Open-Ended Modeling


# Task 2.1 — Build first fact model (fct_jobs)


**Prompt:**
> Review the 6 intermediate models and build a single fact model for the most central table in the dataset. Place it in models/marts/, use ref() only, and run dbt run to confirm it builds. Check in if business logic input is needed.


**Successes:**
- Correctly identified jobs as the central fact in a gig economy dataset
- Aggregated payouts and adjustments to job grain before joining — avoids fan-out issues
- Used left joins throughout, preserving all jobs regardless of payout or adjustment status
- `net_revenue_usd` correctly preserves null for canceled jobs rather than defaulting to zero
- Denormalized service, contractor, and customer attributes directly onto the fact — appropriate at the mart layer
- Section comments make the model readable and self-documenting


**Needs Improvement:**
- `bool_or(is_pending)` in the payouts aggregation returns true if any single payout is pending — reasonable interpretation but an undocumented business logic assumption


**Summary:**
fct_jobs was built correctly at job grain with payouts and adjustments pre-aggregated before joining. Null semantics were preserved for canceled jobs and service, contractor, and customer attributes were denormalized onto the fact. The model passed dbt run on the first attempt.


---


**Note — Data Quality Misdiagnosis:**
During a QA pass on fct_jobs, Claude Code identified what it believed were 724 corrupted payout records with amounts mismatching the expected formula. It diagnosed two failure modes (zero-fill records and over-generated amounts) and recommended adding a `derived_payout_usd` column and a `has_payout_gross_mismatch` flag to the fact model.


After being directed to inspect the source generation script, Claude Code ran a full population verification and found that all 20,139 payout records reconciled perfectly — the original formula accounts for pre-payout adjustments that Claude Code had not considered. The "corrupted" records were legitimate: zero payouts resulted from full refunds issued before payout processing, and above-gross payouts resulted from goodwill bonuses. No model changes were needed.


The misdiagnosis was caught before any incorrect columns were shipped, but it required additional investigation to resolve. This is a realistic example of how agentic tools can misread data patterns without full business context — and why human review of proposed fixes matters.


---




# Task 2.2 — Build remaining 5 mart models


**Prompt:**
> Following the same pattern as fct_jobs, build appropriate mart models for the remaining 5 intermediate models: int_users_enriched, int_contractor_profiles_enriched, int_services_enriched, int_payouts_enriched, and int_adjustments_enriched. Place all models in models/marts/, use ref() only, and run dbt run --select marts to confirm all pass.


**Successes:**
- Correctly classified all 5 models as either facts or dimensions — jobs, payouts, and adjustments as facts; users, contractors, and services as dimensions
- `fct_adjustments` added `signed_amount_usd` unprompted — applies direction to the raw amount, making downstream aggregations cleaner
- `fct_payouts` included an inline comment explicitly referencing the QA investigation finding about implied gross — good self-documentation
- `dim_contractors` correctly joined user attributes to avoid requiring consumers to join two tables
- All 5 models passed `dbt run` successfully


**Needs Improvement:**
- `days_since_signup` reappeared in both `dim_users` and `dim_contractors` using `current_timestamp` — the same non-deterministic issue removed in Task 1.3. Claude Code fixed it when instructed but reintroduced it independently when given creative freedom
- `dim_services` joins contractor attributes onto a services dimension — services and contractors are separate entities, creating a mixed-grain dimension


**Summary:**
All 5 models were correctly classified as facts or dimensions and passed dbt run. `days_since_signup` reappeared in `dim_users` and `dim_contractors` — likely due to a new Claude Code session with no memory of the Task 1.3 decision to remove it. This highlights a key limitation: architectural decisions made in one session do not persist into the next without being documented in a `CLAUDE.md` file or re-stated in the prompt.


---


# Task 2.3 — Free-choice business value mart model


**Prompt:**
> You have a complete star schema in models/marts/. Build one additional mart model that demonstrates real business value for a gig economy platform. You have full creative freedom. Briefly explain your reasoning before starting, then build it and run dbt run to confirm it passes.


**Successes:**
- Chose a contractor performance scorecard unprompted — a genuinely useful operational model covering volume, quality, earnings, and service breadth in a single table
- Correctly referenced `fct_jobs` and `dim_contractors` rather than going back to intermediate models — avoided duplicating logic already assembled upstream
- Metrics were well-chosen and business-relevant: completion rate, contractor cancellation rate, refund exposure, hours variance, and service category breadth
- `contractor_cancel_rate_pct` is a particularly valuable risk signal — flags contractors who frequently abandon jobs
- Coalesce applied correctly — contractors with no jobs default to 0, making the scorecard complete for all contractors
- Avoided `days_since_signup` and used `first_job_at` and `last_completed_at` instead, letting consumers calculate recency themselves


**Needs Improvement:**
- No significant issues — the model is analytically sound and production-ready


**Summary:**
Claude Code chose a contractor performance scorecard and built it correctly on the first attempt, referencing mart models rather than intermediates and applying coalesce consistently for contractors with no job history.


---


# Task 2.4 — Free-choice business value mart model (2nd)


**Prompt:**
> Build one additional mart model that delivers business value for a gig economy platform. Choose something different from contractor performance. Briefly explain your reasoning before starting, then build it in models/marts/ using ref() only and run dbt run to confirm it builds. Full creative freedom.


**Successes:**
- Built a customer health scorecard using RFM framing (Recency, Frequency, Monetary) — a recognised CRM methodology
- `health_segment` classifies customers into six buckets (suspended, never_ordered, at_risk, low_engagement, high_value, healthy) with clearly documented priority ordering and a note that it could be replaced with an ML score
- Refund rate calculated against completed jobs only — a subtle but correct business logic choice, with an inline comment explaining the reasoning
- Referenced `fct_jobs` and `dim_users` — correct mart-to-mart reference pattern
- When flagged about `current_timestamp` in the health segment, fixed it by introducing a `max_job_date` CTE that anchors the reference date to the dataset itself — more reproducible than `current_timestamp` and explicitly noted as consistent with the pattern from `int_users_enriched`
- Within-session context was retained — Claude Code referenced a decision made earlier in the same session when applying the fix


**Needs Improvement:**
- Initial output used `current_timestamp` for the 90-day recency check before being corrected — required a follow-up prompt to fix


**Summary:**
Claude Code built a customer health scorecard with RFM framing, a rule-based `health_segment` column, and correct null handling throughout. The initial `current_timestamp` issue was fixed within the same session by introducing a `max_job_date` CTE, with Claude Code explicitly referencing its own earlier decision when applying the correction.


---


# Task 2.5 — Free-choice business value mart model (3rd)


**Prompt:**
> Build one additional mart model that delivers business value for a gig economy platform. Choose something different from contractor performance. Briefly explain your reasoning before starting, then build it in models/marts/ using ref() only and run dbt run to confirm it builds. Full creative freedom.


**Successes:**
- Built a service category health report — a different analytical angle from the contractor and customer scorecards, completing a three-sided view of the marketplace
- Split demand and supply into separate CTEs before joining — clean architecture that keeps logic independently readable and modifiable
- `completed_jobs_per_active_contractor` is a standout supply-demand tension metric — flags categories where a small contractor pool is absorbing disproportionate demand
- Used `FULL OUTER JOIN` correctly — preserves categories with listings but no jobs, and categories with jobs but no active listings
- `count(distinct contractor_id)` on the supply side explicitly avoids double-counting contractors with multiple listings in the same category, with an inline comment explaining the decision
- Rate tier distribution percentages add pricing context without requiring a separate join
- `where service_category is not null` guard prevents null categories from polluting aggregations


**Needs Improvement:**
- `order by total_gross_revenue_usd desc` at the end of the model — ordering belongs in the BI layer, not in dbt models


**Summary:**
Claude Code built a service category health report combining demand and supply metrics at category grain. Demand and supply were split into separate CTEs before joining, a `FULL OUTER JOIN` was used to preserve all categories, and a supply-demand tension metric was derived unprompted.


---


## Part 3 - dbt Testing and Self Audit


# Task 3.1 — Add dbt tests to fct_jobs


**Prompt:**
> Review fct_jobs and the underlying data to identify what should be tested. Add appropriate generic tests (unique, not_null, accepted_values, relationships) to a schema.yml file in models/marts/. Run dbt test --select fct_jobs to confirm all tests pass.


**Successes:**
- Did not apply `not_null` tests blindly — deliberately omitted them from columns that are intentionally null for canceled jobs (`completed_at`, `actual_hours`, `gross_amount_usd`), with inline comments explaining each decision
- Added `relationships` tests on all three foreign keys — validates referential integrity across the full star schema
- `accepted_values` tests on all categorical columns (`job_status`, `canceled_by`, `rate_tier`, `contractor_tier`, `customer_account_status`) — catches unexpected upstream changes
- Noted that `canceled_by` accepted_values ignores nulls by default — shows awareness of dbt test behavior
- All 30 tests passed on first run in 1.68 seconds


**Needs Improvement:**
- Initial `relationships` test syntax used an incorrect `arguments:` wrapper — required a fix before tests would run without deprecation warnings


**Summary:**
30 tests were added to fct_jobs covering unique, not_null, accepted_values, and relationships. Not_null tests were omitted from intentionally nullable columns with inline comments explaining each omission. All 30 tests passed after a minor syntax fix on the relationships tests.


---


# Task 3.2 — Add dbt tests to remaining 8 mart models


**Prompt:**
> I have a schema.yml in models/marts/ with tests already added for fct_jobs. Please add dbt tests to the remaining mart models. Review each model and the underlying data to determine appropriate tests — use your judgment on what to test and why. Run dbt test --select marts to confirm all tests pass.


**Successes:**
- Correctly identified primary keys across all 8 models and applied `unique` + `not_null` without prompting
- Maintained null semantics discipline from Task 3.1 — omitted `not_null` tests on rate/average/timestamp columns that are intentionally null for entities with no job history, with inline comments explaining each decision
- `relationships` tests added to `fct_payouts` and `fct_adjustments` linking back to `fct_jobs` — validates the full fact-to-fact referential chain
- `accepted_values` applied consistently across models for shared categorical columns (`service_category`, `contractor_tier`, `account_status`, `health_segment`)
- All 180 tests across 9 models passed in 8.63 seconds with zero warnings or errors


**Needs Improvement:**
- No significant issues — test coverage was thorough and well-reasoned throughout


**Summary:**
180 tests were added across 9 mart models with consistent null handling, accepted_values coverage on all categorical columns, and relationships tests linking facts to dimensions. All tests passed on the first run with no errors or warnings.


---


# Task 3.3 — Self-audit test coverage gaps


**Prompt:**
> I have a complete dbt project with 180 tests across all mart models. Please review the full project — staging, intermediate, and mart layers — and identify any gaps in test coverage. Implement any additional tests you think are genuinely valuable, then run dbt test to confirm everything passes.


**Successes:**
- Identified three distinct layers of missing coverage unprompted: staging, intermediate, and singular tests — each with a clear rationale
- Staging tests justified as catching schema drift before it propagates to 21 downstream models
- Intermediate `accepted_values` tests correctly placed to validate normalization logic at the layer responsible for it
- 4 singular tests added for cross-column logical invariants: mutual exclusivity of `is_completed` and `is_canceled`, timeline integrity, non-negative refunds, and positive adjustment magnitudes
- 107 new tests added bringing the total to 287 — all passed on first run
- Every addition was explained with a specific reason, not added for coverage metrics alone


**Needs Improvement:**
- No significant issues


**Summary:**
107 additional tests were added across staging, intermediate, and a new singular tests folder, bringing the total to 287. Coverage gaps were identified independently across all three layers, with each addition accompanied by a specific rationale. All 287 tests passed on the first run.


---


## Part 4 - Documentation and Final Verification 


# Task 4.1 — Add descriptions to all models and columns
Prompt:


Add descriptions to all models and columns across the entire project that don't already have one. Review each model and write meaningful, business-friendly descriptions — not just restatements of the column name. Work through all three layers: staging, intermediate, and marts. Do not change any existing tests or descriptions — only add where missing.


Successes:


Descriptions added across all three layers — staging, intermediate, and marts — without touching any existing tests
Descriptions go beyond column name restatements — implied_gross_usd explains why it may differ from fct_jobs.gross_amount_usd, completed_jobs_per_active_contractor explains what a high value signals for the business, avg_hours_variance clarifies the interpretation of positive vs negative values
health_segment description covers all 6 buckets and the priority logic behind them
287/287 tests remained green after all additions — YAML edits were purely additive


Needs Improvement:


No significant issues


Summary:
Descriptions were added across all three dbt layers with business context rather than mechanical column name restatements. All existing tests passed unchanged confirming no unintended edits were made.


# Task 4.2 — Documentation Verification
Prompt:


Run dbt docs generated from the project root. After it completes, confirm it succeeded without errors, report how many nodes were documented, and confirm that target/catalog.json and target/manifest.json both exist and are non-empty.


Successes:


dbt docs generate completed cleanly — 21 models, 6 sources, 287 tests documented, 474 macros registered
catalog.json written to target/ confirming DuckDB introspection succeeded
manifest.json present and readable by the docs site
Docs served via dbt docs serve at localhost:8080 without errors
Column descriptions verified in UI — all columns populated with business-meaningful context (e.g. customer_health_summary grain, RFM framing, and null semantics for customers with no job history all visible in the docs site)
Lineage graph confirmed — full source → staging → intermediate → marts chain navigable using the expand controls
Test coverage indicators present on all documented models


Needs Improvement:


dbt docs serve is required to load the docs site correctly — opening target/index.html directly in a browser fails due to browser security restrictions on local file fetching. This is expected dbt behavior but worth noting for anyone running the project for the first time.


Summary:
Documentation compiled and served cleanly. All 21 models, 6 sources, and 287 tests are reflected in the docs site with business-meaningful descriptions and a navigable lineage graph. Task 4.1 descriptions were confirmed visible in the UI.


# Task 4.3 — Project Self-Audit
Prompt:


Perform a final project audit before the README is written and the project is pushed to GitHub. Review all 21 models for description coverage, test coverage, and schema.yml completeness. Check whether CLAUDE.md exists. Review singular tests for meaningful names and comments. Check sources.yml for descriptions and freshness configuration. Report findings as blockers, nice-to-haves, and skips.


Successes:


Identified three genuine gaps independently without prompting: missing source group and table descriptions in sources.yml, missing CLAUDE.md, and no rationale for omitting freshness checks
Correctly classified all three as actionable rather than skips — source descriptions are visible in the docs site, CLAUDE.md is a real operational need for agentic workflows
All three gaps resolved in the same session: source descriptions added, CLAUDE.md created with architectural conventions derived from the project's own decision history, freshness omission comment added to sources.yml
CLAUDE.md content was well-structured — covered layer conventions, null semantics policy, non-deterministic metrics ban, and a note explaining the file's purpose for future Claude Code sessions
No blockers remained after remediation


Needs Improvement:


No significant issues


Summary:
The self-audit identified three gaps that would have been visible to any reviewer examining the GitHub repo: undocumented sources, a missing CLAUDE.md, and no explanation for omitted freshness checks. All three were resolved. The project is now audit-clean and ready for README authoring and GitHub push.