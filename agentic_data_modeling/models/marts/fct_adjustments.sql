-- Grain: one row per adjustment.
-- Adjustments are post-completion events on a job: refunds back to the customer
-- or goodwill bonuses paid to the contractor.

with adjustments as (
    select * from {{ ref('int_adjustments_enriched') }}
),

jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

services as (
    select * from {{ ref('int_services_enriched') }}
),

contractors as (
    select * from {{ ref('int_contractor_profiles_enriched') }}
),

final as (
    select
        -- Primary and foreign keys
        a.adjustment_id,
        a.job_id,

        -- Timestamps
        a.adjusted_at,

        -- Adjustment type & direction
        a.adjustment_type,
        a.adjustment_direction,
        a.is_refund,
        a.is_full_refund,
        a.is_partial_refund,
        a.is_goodwill_bonus,

        -- Amount
        a.adjustment_amount_usd,

        -- Signed amount: negative for refunds (revenue out), positive for bonuses
        case
            when a.adjustment_direction = 'debit'  then -a.adjustment_amount_usd
            when a.adjustment_direction = 'credit' then  a.adjustment_amount_usd
        end as signed_amount_usd,

        -- Job context: who was involved and how much the job was worth
        j.service_id,
        j.contractor_id,
        j.customer_id,
        j.gross_amount_usd      as job_gross_amount_usd,
        j.requested_at          as job_requested_at,
        j.completed_at          as job_completed_at,

        -- Service and contractor context
        s.service_category,
        c.contractor_tier

    from adjustments        a
    left join jobs          j on a.job_id        = j.job_id
    left join services      s on j.service_id    = s.service_id
    left join contractors   c on j.contractor_id = c.contractor_id
)

select * from final
