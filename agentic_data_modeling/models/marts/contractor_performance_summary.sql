-- Grain: one row per contractor.
--
-- Purpose: operational scorecard combining volume, quality, earnings, and
-- service breadth. Intended for tier reviews, risk flagging, and contractor
-- health monitoring.
--
-- References mart models (fct_jobs, dim_contractors, dim_services) rather than
-- intermediates because job-level refund and payout totals are already correctly
-- assembled in fct_jobs. Re-implementing that logic here would duplicate it.

with contractors as (
    select * from {{ ref('dim_contractors') }}
),

jobs as (
    select * from {{ ref('fct_jobs') }}
),

services as (
    select * from {{ ref('dim_services') }}
),

job_stats as (
    select
        contractor_id,

        -- Volume
        count(*)                                                        as total_jobs,
        count(*) filter (where is_completed)                            as completed_jobs,
        count(*) filter (where is_canceled)                             as canceled_jobs,
        count(*) filter (where canceled_by = 'contractor')             as contractor_cancellations,
        count(*) filter (where canceled_by = 'customer')               as customer_cancellations,

        -- Completion rate: completed / all jobs requested
        round(
            100.0 * count(*) filter (where is_completed) / nullif(count(*), 0),
        2)                                                              as completion_rate_pct,

        -- Contractor cancellation rate: useful risk signal
        round(
            100.0 * count(*) filter (where canceled_by = 'contractor')
            / nullif(count(*), 0),
        2)                                                              as contractor_cancel_rate_pct,

        -- Revenue generated on platform
        round(sum(gross_amount_usd), 2)                                 as total_gross_usd,
        round(sum(net_revenue_usd), 2)                                  as total_net_revenue_usd,
        round(avg(gross_amount_usd) filter (where is_completed), 2)    as avg_job_gross_usd,

        -- Contractor earnings (net of commission; pre-payout-adjustment deductions)
        round(sum(total_payout_amount_usd), 2)                         as total_earned_usd,

        -- Refund exposure: how much of this contractor's revenue was refunded
        count(*) filter (where total_refund_usd > 0)                   as jobs_with_refunds,
        round(sum(total_refund_usd), 2)                                 as total_refunded_usd,
        round(
            100.0 * count(*) filter (where total_refund_usd > 0)
            / nullif(count(*) filter (where is_completed), 0),
        2)                                                              as refund_rate_pct,

        -- Hours quality: positive = ran over estimate, negative = finished early
        round(avg(hours_variance) filter (where is_completed), 2)      as avg_hours_variance,

        -- Recency: timestamps only; consumers compute tenure/recency at query time
        min(requested_at)                                               as first_job_at,
        max(completed_at)                                               as last_completed_at

    from jobs
    group by contractor_id
),

service_stats as (
    select
        contractor_id,
        count(*)                                  as total_services,
        count(*) filter (where is_available)      as available_services,
        -- Distinct categories this contractor covers
        count(distinct service_category)          as service_category_count
    from services
    group by contractor_id
),

final as (
    select
        -- Identity & tier
        c.contractor_id,
        c.contractor_tier,
        c.is_premium_tier,
        c.commission_rate,
        c.is_eligible_to_work,
        c.city,
        c.signup_at,

        -- Job volume
        coalesce(j.total_jobs, 0)                   as total_jobs,
        coalesce(j.completed_jobs, 0)               as completed_jobs,
        coalesce(j.canceled_jobs, 0)                as canceled_jobs,
        coalesce(j.contractor_cancellations, 0)     as contractor_cancellations,
        coalesce(j.customer_cancellations, 0)       as customer_cancellations,
        j.completion_rate_pct,
        j.contractor_cancel_rate_pct,

        -- Revenue & earnings
        coalesce(j.total_gross_usd, 0)              as total_gross_usd,
        coalesce(j.total_net_revenue_usd, 0)        as total_net_revenue_usd,
        j.avg_job_gross_usd,
        coalesce(j.total_earned_usd, 0)             as total_earned_usd,

        -- Quality signals
        coalesce(j.jobs_with_refunds, 0)            as jobs_with_refunds,
        coalesce(j.total_refunded_usd, 0)           as total_refunded_usd,
        j.refund_rate_pct,
        j.avg_hours_variance,

        -- Recency
        j.first_job_at,
        j.last_completed_at,

        -- Service breadth
        coalesce(s.total_services, 0)               as total_services,
        coalesce(s.available_services, 0)           as available_services,
        coalesce(s.service_category_count, 0)       as service_category_count

    from contractors        c
    left join job_stats     j on c.contractor_id = j.contractor_id
    left join service_stats s on c.contractor_id = s.contractor_id
)

select * from final
