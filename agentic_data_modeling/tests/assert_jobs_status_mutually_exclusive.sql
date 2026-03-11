-- Assert: a job cannot be both completed and canceled simultaneously.
-- is_completed and is_canceled are derived from job_status, so this
-- invariant should always hold — but tests guard against future logic
-- changes that break the mutual exclusivity.
-- Returns rows on failure (test passes when 0 rows returned).

select job_id
from {{ ref('fct_jobs') }}
where is_completed and is_canceled
