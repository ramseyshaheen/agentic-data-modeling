with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

services as (
    select * from {{ ref('int_services_enriched') }}
),

contractors as (
    select * from {{ ref('int_contractor_profiles_enriched') }}
),

customers as (
    select * from {{ ref('int_users_enriched') }}
),

-- Aggregate payouts to job grain (a job may have more than one payout record)
payouts_by_job as (
    select
        job_id,
        sum(payout_amount_usd)  as total_payout_amount_usd,
        sum(platform_fee_usd)   as total_platform_fee_usd,
        count(*)                as payout_count,
        bool_or(is_pending)     as has_pending_payout
    from {{ ref('int_payouts_enriched') }}
    group by job_id
),

-- Aggregate adjustments to job grain (a job may have multiple refunds / bonuses)
adjustments_by_job as (
    select
        job_id,
        sum(case when is_refund         then adjustment_amount_usd else 0 end) as total_refund_usd,
        sum(case when is_goodwill_bonus  then adjustment_amount_usd else 0 end) as total_bonus_usd,
        count(*)                                                                as adjustment_count,
        bool_or(is_full_refund)                                                as has_full_refund
    from {{ ref('int_adjustments_enriched') }}
    group by job_id
),

final as (
    select
        -- ----------------------------------------------------------------
        -- Keys
        -- ----------------------------------------------------------------
        j.job_id,
        j.service_id,
        j.contractor_id,
        j.customer_id,

        -- ----------------------------------------------------------------
        -- Timestamps
        -- ----------------------------------------------------------------
        j.requested_at,
        j.completed_at,

        -- ----------------------------------------------------------------
        -- Job status
        -- ----------------------------------------------------------------
        j.job_status,
        j.is_completed,
        j.is_canceled,
        j.canceled_by,

        -- ----------------------------------------------------------------
        -- Time / effort metrics
        -- ----------------------------------------------------------------
        j.estimated_hours,
        j.actual_hours,
        j.hours_variance,
        j.job_duration_hours,

        -- ----------------------------------------------------------------
        -- Revenue (from job record)
        -- ----------------------------------------------------------------
        j.gross_amount_usd,

        -- ----------------------------------------------------------------
        -- Payout financials (aggregated; NULL when no payout exists yet)
        -- ----------------------------------------------------------------
        p.total_payout_amount_usd,
        p.total_platform_fee_usd,
        p.payout_count,
        p.has_pending_payout,

        -- ----------------------------------------------------------------
        -- Adjustments (aggregated; default to 0 / false when none exist)
        -- ----------------------------------------------------------------
        coalesce(a.total_refund_usd,  0)     as total_refund_usd,
        coalesce(a.total_bonus_usd,   0)     as total_bonus_usd,
        coalesce(a.adjustment_count,  0)     as adjustment_count,
        coalesce(a.has_full_refund,   false) as has_full_refund,

        -- ----------------------------------------------------------------
        -- Derived net revenue: gross minus any refunds
        -- NULL preserved when gross_amount_usd is NULL (e.g. canceled jobs)
        -- ----------------------------------------------------------------
        j.gross_amount_usd - coalesce(a.total_refund_usd, 0) as net_revenue_usd,

        -- ----------------------------------------------------------------
        -- Service attributes
        -- ----------------------------------------------------------------
        s.service_category,
        s.hourly_rate_usd,
        s.rate_tier,

        -- ----------------------------------------------------------------
        -- Contractor attributes
        -- ----------------------------------------------------------------
        c.contractor_tier,
        c.is_premium_tier,
        c.commission_rate as contractor_commission_rate,

        -- ----------------------------------------------------------------
        -- Customer attributes
        -- ----------------------------------------------------------------
        cu.city           as customer_city,
        cu.account_status as customer_account_status

    from jobs               j
    left join services          s  on j.service_id    = s.service_id
    left join contractors       c  on j.contractor_id = c.contractor_id
    left join customers         cu on j.customer_id   = cu.user_id
    left join payouts_by_job    p  on j.job_id        = p.job_id
    left join adjustments_by_job a on j.job_id        = a.job_id
)

select * from final