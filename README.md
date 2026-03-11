# Agentic Data Modeling with dbt 

---

### 1. Overview

This project constructs a full dbt project (DAG) for a synthetic contractor marketplace (think TaskRabbit or Fiverr) using AI agents. It serves as a structured evaluation of Claude Code Opus 4.6 as the primary agent across four different development stages: intermediate modeling, mart modeling, dbt testing, and documentation. Each of the 14 key data modeling tasks within these stages were evaluated and logged to capture the agent’s behavior and ability to autonomously conduct production level data modeling work.

---

### 2. Dataset
Synthetic custom marketplace dataset generated in Python with:

- 6 source tables
- 38 raw data fields
- ~24,000 jobs
- ~20,000 payouts
- ~3,000 users
- Realistic scenarios including canceled jobs and partial refunds
- Simulated distribution of contract values

All data is stored as Parquet and queried via DuckDB.

---

### 3. Architecture

4-layer dbt project (sources → staging → intermediate → marts)

models/ 

├── staging/ # 6 models — direct reads from source parquet files. │ No business logic.
 
├── intermediate/ # 6 models — enrichment, normalization, and derived flags. │ The transformation layer. Marts never read staging directly. 

└── marts/ # 9 models — facts, dimensions, and business scorecards. Consumption-ready. One row per business entity.

<img width="2257" height="1085" alt="image" src="https://github.com/user-attachments/assets/4e1574ef-0d48-4ae2-81ed-7652601b71a6" />

---

### 4. What Was Built

**Models (21 total)**

- 6 Staging Models

- 6 Intermediate Models

- 9 Mart Models

- 3 Fact (FCT)

- 3 Dimension (DIM)

 - 3 Scorecard (Agent’s Open-Ended Models)

**Tests (287 total)**

- Staging: 37 tests — PK integrity and source schema drift detection
  
- Intermediate: 66 tests — normalization validation and derived boolean checks
  
- Marts: 180 tests — referential integrity, null semantics, accepted values
  
- Singular: 4 tests — cross-column logical invariants (e.g. is_completed and is_canceled are mutually exclusive)
 
---

### 5. Evaluation Methodology
The primary agent (Claude Code Opus 4.6) was given 14 tasks across 4 stages of dbt development:

- Stage 1 (Tasks 1.1–1.3): Intermediate modeling and refactoring
- Stage 2 (Tasks 2.1–2.5): Mart construction across facts, dimensions, and scorecards
- Stage 3 (Tasks 3.1–3.3): Test coverage across all three layers
- Stage 4 (Tasks 4.1–4.3): Schema documentation, verification, and self-audit

Each task followed the same structure: a written prompt → independent review of the output, and a logged entry documenting what worked and what needed correction.
Initial written prompt and back and forth with the primary agent to produce output
All requested permissions within the project were granted to the agent
Review process with simulated multi-agent workflow

1) Manual Review

2) Agent #2 Review → External 2nd Claude with separate contex window

3) Agent #3 Review → External GPT-5.2 with separate context window

4) Agent #2 Feedback → External 2nd Claude reviews notes from Agent #3 

5) Manual Review of overall agentic feedback

6) Selected feedback is passed back to the primary project agent and implemented

Task output and feedback logged in evaluation_log.md

---

### 6. Key Findings
**Successes**

- Followed dbt conventions correctly throughout: ref(), layering, naming, and test structure required no correction across 14 tasks
- Claude successfully explored data before modeling. The agent ran null counts and schema inspection before writing SQL in most tasks, unprompted 
- Within a session, it retained context and referenced prior decisions when applying fixes
- Generated comprehensive tests across staging, int, and mart models including 4 relevant singular tests. 
- Given full creative freedom, Claude Code produced three effective open-ended business value models unprompted: contractor_performance_summary (contractor-grain volume and revenue scorecard), customer_health_summary (RFM-style segmentation), and service_category_performance (supply/demand tension by category). All three passed on the first run, required minimal correction, and could provide genuine business value. 

**Issues Identified** 
- Task 1.1 - Defects not caught by the primary agent: int_jobs_enriched passed dbt run with two business logic defects: a status normalization bug that would silently break boolean flags on certain strings, and a coalesce collapsing "no revenue" and "unknown revenue" into the same value. Neither caused a build failure. Both were caught only by GPT-5.2 during the simulated multi-agent review step.
 - Task 2.1 - Data misdiagnosis: Claude Code flagged 724 payout records as corrupted and recommended adding diagnostic columns to the fact model. All records were valid because the diagnosis failed to account for pre-payout adjustments. My manual review caught before anything progressed, but it required additional investigation to resolve. 
- Cross-session memory loss: Patterns explicitly removed in one session reappeared in the next. Architectural decisions must be documented externally to persist which is the purpose of CLAUDE.md in this repo.


---
### 7. Conclusion
Overall, Claude Code Opus 4.6 proved quite capable when faced with the range of tasks required to build  production level dbt data models. The agent completed 12 out of 14 tasks correctly on its first dbt output following the initial prompt and conversation. It was particularly impressive when independently determining which downstream models to build, creating open-ended business valuable mart models, and generating comprehensive dbt tests. 

However, in two specific tasks, the agent did hallucinate raw data corruption and incorrectly normalize fields showing the potential for significant errors in independent agentic outputs. A simulated multi-agent QA workflow did show potential in reducing these types of errors which could augment and extend the capability of agentic data modeling work. 

---
### 8. Tech Stack

- dbt (data modeling platform)
- Python (data generation, evaluation framework)
- DuckDB (SQL execution)
- Anthropic Claude Opus 4.6 (Primary AI agent)
- GPT-5.2 (External model for additional verification)
---

### 9. How to Run the Project
1) Clone the repo
git clone https://github.com/yourusername/agentic-data-modeling.git
cd agentic-data-modeling

2) Create and activate virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1   # Windows
source venv/bin/activate       # Mac/Linux

3) Install dependencies
pip install dbt-duckdb

4) Move into the dbt project
cd agentic_data_modeling

5) Run the project
dbt run
dbt test

6) View documentation
dbt docs generate
dbt docs serve

---

