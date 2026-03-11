-- Grain: one row per customer (user).
--
-- Purpose: RFM-style health scorecard covering spend, activity recency,
-- refund exposure, and cancellation behaviour. Designed to support churn
-- risk flagging, LTV segmentation, and CRM targeting.
--
-- All financial figures are in USD. Recency / tenure fields are in days.
-- Timestamps are intentionally left as-is so consumers can rebase to any
-- reference date without rerunning this model.

with customers as (
    select * from {{ ref('dim_users') }}
),

jobs as (
    select * from {{ ref('fct_jobs') }}
),

-- Deterministic reference date: the latest job request in the dataset.
-- Avoids current_timestamp so results are reproducible across runs and
-- backfills (consistent with the pattern established in int_users_enriched).
max_job_date as (
    select max(requested_at) as ref_date from jobs
),

job_stats as (
    select
        customer_id,

        -- ----------------------------------------------------------------
        -- Frequency (F in RFM)
        -- ----------------------------------------------------------------
        count(*)                                                        as total_jobs,
        count(*) filter (where is_completed)                            as completed_jobs,
        count(*) filter (where is_canceled)                             as canceled_jobs,

        -- Customer-initiated cancels: a signal of intent / dissatisfaction
        count(*) filter (where canceled_by = 'customer')               as customer_cancellations,

        round(
            100.0 * count(*) filter (where is_completed)
            / nullif(count(*), 0),
        2)                                                              as completion_rate_pct,

        round(
            100.0 * count(*) filter (where canceled_by = 'customer')
            / nullif(count(*), 0),
        2)                                                              as customer_cancel_rate_pct,

        -- ----------------------------------------------------------------
        -- Monetary (M in RFM)
        -- ----------------------------------------------------------------
        round(sum(gross_amount_usd), 2)                                 as total_gross_spend_usd,
        round(sum(net_revenue_usd), 2)                                  as total_net_spend_usd,
        round(avg(gross_amount_usd) filter (where is_completed), 2)    as avg_job_gross_usd,

        -- ----------------------------------------------------------------
        -- Refund / dispute exposure
        -- ----------------------------------------------------------------
        count(*) filter (where total_refund_usd > 0)                   as jobs_with_refunds,
        round(sum(total_refund_usd), 2)                                 as total_refunded_usd,

        -- Refund rate vs. completed jobs (not all jobs, to avoid penalising
        -- customers who cancel before work starts)
        round(
            100.0 * count(*) filter (where total_refund_usd > 0)
            / nullif(count(*) filter (where is_completed), 0),
        2)                                                              as refund_rate_pct,

        count(*) filter (where has_full_refund)                        as full_refund_jobs,

        -- ----------------------------------------------------------------
        -- Recency (R in RFM) — raw timestamps; consumers compute day diffs
        -- ----------------------------------------------------------------
        min(requested_at)                                               as first_job_at,
        max(requested_at)                                               as last_job_at,
        max(completed_at)                                               as last_completed_at

    from jobs
    group by customer_id
),

final as (
    select
        -- ----------------------------------------------------------------
        -- Identity
        -- ----------------------------------------------------------------
        c.user_id                                                       as customer_id,
        c.city,
        c.account_status,
        c.is_active,
        c.is_suspended,
        c.signup_at,

        -- ----------------------------------------------------------------
        -- Frequency
        -- ----------------------------------------------------------------
        coalesce(j.total_jobs, 0)                                       as total_jobs,
        coalesce(j.completed_jobs, 0)                                   as completed_jobs,
        coalesce(j.canceled_jobs, 0)                                    as canceled_jobs,
        coalesce(j.customer_cancellations, 0)                           as customer_cancellations,
        j.completion_rate_pct,
        j.customer_cancel_rate_pct,

        -- ----------------------------------------------------------------
        -- Monetary
        -- ----------------------------------------------------------------
        coalesce(j.total_gross_spend_usd, 0)                            as total_gross_spend_usd,
        coalesce(j.total_net_spend_usd, 0)                              as total_net_spend_usd,
        j.avg_job_gross_usd,

        -- ----------------------------------------------------------------
        -- Refund / dispute signals
        -- ----------------------------------------------------------------
        coalesce(j.jobs_with_refunds, 0)                                as jobs_with_refunds,
        coalesce(j.total_refunded_usd, 0)                               as total_refunded_usd,
        j.refund_rate_pct,
        coalesce(j.full_refund_jobs, 0)                                 as full_refund_jobs,

        -- ----------------------------------------------------------------
        -- Recency timestamps
        -- ----------------------------------------------------------------
        j.first_job_at,
        j.last_job_at,
        j.last_completed_at,

        -- ----------------------------------------------------------------
        -- Derived health bucket
        -- ----------------------------------------------------------------
        -- Simple rule-based segmentation; replace with ML score when ready.
        -- Priority order: suspended → never ordered → at risk → low engagement
        --                 → healthy → high value
        case
            when c.is_suspended                               then 'suspended'
            when j.customer_id is null                        then 'never_ordered'
            when j.last_job_at < d.ref_date - interval '90' day
                                                              then 'at_risk'
            when j.refund_rate_pct > 30
              or j.customer_cancel_rate_pct > 40              then 'low_engagement'
            when j.total_gross_spend_usd >= 1000
             and j.completion_rate_pct   >= 70                then 'high_value'
            else                                                   'healthy'
        end                                                             as health_segment

    from customers          c
    left join job_stats     j on c.user_id = j.customer_id
    cross join max_job_date d
)

select * from final
