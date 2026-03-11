-- Grain: one row per payout.
--
-- Note on implied_gross_usd / platform_fee_usd:
--   These are back-calculated from payout_amount, not from the job's gross_amount.
--   payout_amount already reflects pre-payout adjustments (refunds deducted,
--   bonuses added), so implied_gross will differ from job gross_amount_usd
--   whenever adjustments occurred before the payout was issued. This is
--   expected and correct — see QA investigation for full explanation.

with payouts as (
    select * from {{ ref('int_payouts_enriched') }}
),

jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

contractors as (
    select * from {{ ref('int_contractor_profiles_enriched') }}
),

final as (
    select
        -- Primary and foreign keys
        p.payout_id,
        p.contractor_id,
        p.job_id,

        -- Timestamps
        p.paid_at,

        -- Payout financials
        p.commission_rate,
        p.payout_amount_usd,
        p.implied_gross_usd,
        p.platform_fee_usd,

        -- Status
        p.payout_status,
        p.is_pending,

        -- Job context
        j.service_id,
        j.customer_id,
        j.requested_at          as job_requested_at,
        j.gross_amount_usd      as job_gross_amount_usd,

        -- Contractor context
        c.contractor_tier,
        c.is_premium_tier

    from payouts            p
    left join jobs          j on p.job_id        = j.job_id
    left join contractors   c on p.contractor_id = c.contractor_id
)

select * from final
