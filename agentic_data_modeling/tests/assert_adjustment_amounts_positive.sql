-- Assert: raw adjustment amounts are always stored as positive magnitudes.
-- Direction (debit/credit) is encoded in adjustment_direction and
-- signed_amount_usd. A negative adjustment_amount_usd means the source
-- sign convention has changed and signed_amount_usd would be wrong.
-- Returns rows on failure (test passes when 0 rows returned).

select adjustment_id, adjustment_amount_usd
from {{ ref('fct_adjustments') }}
where adjustment_amount_usd <= 0
