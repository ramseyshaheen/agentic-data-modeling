-- Assert: refund amounts are stored as positive magnitudes; a negative
-- total_refund_usd means the sign convention has been inverted somewhere
-- in the aggregation logic and net_revenue_usd would be incorrect.
-- Returns rows on failure (test passes when 0 rows returned).

select job_id, total_refund_usd
from {{ ref('fct_jobs') }}
where total_refund_usd < 0
