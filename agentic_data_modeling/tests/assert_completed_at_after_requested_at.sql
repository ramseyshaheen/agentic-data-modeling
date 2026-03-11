-- Assert: for completed jobs, completed_at must not precede requested_at.
-- A job finishing before it was requested is a timestamp integrity violation
-- that would silently corrupt job_duration_hours and any time-series analysis.
-- Returns rows on failure (test passes when 0 rows returned).

select job_id, requested_at, completed_at
from {{ ref('fct_jobs') }}
where is_completed
  and completed_at < requested_at
