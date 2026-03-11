with source as (
    select * from {{ ref('stg_payouts') }}
),

-- Normalize status once; was duplicated for payout_status and is_pending.
normalized as (
    select
        * exclude (payout_status),
        lower(trim(payout_status)) as payout_status
    from source
),

-- Compute implied_gross once so that platform_fee_usd can reference it
-- directly rather than repeating the full division expression.
-- Rounding deferred to marts; intermediate layer preserves full precision.
computed as (
    select
        *,
        payout_amount / nullif(1.0 - commission_rate, 0) as implied_gross
    from normalized
),

enriched as (
    select
        -- Primary and foreign keys
        cast(payout_id     as bigint) as payout_id,
        cast(contractor_id as bigint) as contractor_id,
        cast(job_id        as bigint) as job_id,

        -- Timestamps
        cast(payout_date as timestamp) as paid_at,

        -- Commission rate: precise decimal (e.g. 0.2000 = 20%)
        cast(commission_rate as decimal(5, 4)) as commission_rate,

        -- Payout amount: what the contractor receives
        cast(payout_amount as decimal(10, 2)) as payout_amount_usd,

        -- Derived: implied gross and platform fee computed from a single division
        implied_gross                 as implied_gross_usd,
        implied_gross - payout_amount as platform_fee_usd,

        -- Status: normalized in CTE above
        payout_status,
        payout_status = 'pending' as is_pending

    from computed
)

select * from enriched
