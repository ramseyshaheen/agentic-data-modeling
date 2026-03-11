-- Grain: one row per service_category.
--
-- Purpose: product-catalog health report combining demand-side job metrics
-- (volume, revenue, refund exposure) with supply-side listing counts
-- (contractor coverage, availability). Supports pricing decisions,
-- category investment prioritisation, and supply-gap identification.
--
-- Revenue figures in USD. Rate tiers (budget / standard / premium) come
-- from dim_services; refund and cancellation rates are vs completed jobs.

with jobs as (
    select * from {{ ref('fct_jobs') }}
),

services as (
    select * from {{ ref('dim_services') }}
),

-- ----------------------------------------------------------------
-- Demand side: aggregate fct_jobs to service_category grain.
-- Only completed jobs contribute to revenue / hours metrics;
-- canceled jobs are counted separately as a demand-quality signal.
-- ----------------------------------------------------------------
demand as (
    select
        service_category,

        -- Volume
        count(*)                                                        as total_jobs,
        count(*) filter (where is_completed)                            as completed_jobs,
        count(*) filter (where is_canceled)                             as canceled_jobs,

        round(
            100.0 * count(*) filter (where is_completed)
            / nullif(count(*), 0),
        2)                                                              as job_completion_rate_pct,

        -- Revenue
        round(sum(gross_amount_usd), 2)                                 as total_gross_revenue_usd,
        round(sum(net_revenue_usd), 2)                                  as total_net_revenue_usd,
        round(avg(gross_amount_usd) filter (where is_completed), 2)    as avg_job_gross_usd,

        -- Platform fees (proxy for platform take; may be null before payout)
        round(sum(total_platform_fee_usd), 2)                          as total_platform_fee_usd,

        -- Refund / dispute exposure
        count(*) filter (where total_refund_usd > 0)                   as jobs_with_refunds,
        round(sum(total_refund_usd), 2)                                 as total_refunded_usd,
        round(
            100.0 * count(*) filter (where total_refund_usd > 0)
            / nullif(count(*) filter (where is_completed), 0),
        2)                                                              as refund_rate_pct,
        count(*) filter (where has_full_refund)                        as full_refund_jobs,

        -- Hours quality: positive = ran over estimate
        round(avg(hours_variance) filter (where is_completed), 2)      as avg_hours_variance,

        -- Recency: latest demand signal per category
        max(requested_at)                                               as last_job_at

    from jobs
    where service_category is not null
    group by service_category
),

-- ----------------------------------------------------------------
-- Supply side: aggregate dim_services to service_category grain.
-- A contractor can list the same category multiple times (e.g. at
-- different rates), so we count distinct contractors to avoid
-- double-counting supply depth.
-- ----------------------------------------------------------------
supply as (
    select
        service_category,

        -- Listing counts
        count(*)                                                        as total_listings,
        count(*) filter (where is_available)                           as available_listings,

        -- Distinct contractors offering this category
        count(distinct contractor_id)                                   as contractor_count,
        count(distinct contractor_id) filter (where is_available)      as active_contractor_count,

        -- Rate distribution across listings in this category
        round(min(hourly_rate_usd), 2)                                 as min_hourly_rate_usd,
        round(avg(hourly_rate_usd), 2)                                 as avg_hourly_rate_usd,
        round(max(hourly_rate_usd), 2)                                 as max_hourly_rate_usd,

        -- Rate tier mix: share of listings at each tier
        round(
            100.0 * count(*) filter (where rate_tier = 'budget')
            / nullif(count(*), 0),
        2)                                                              as budget_listing_pct,
        round(
            100.0 * count(*) filter (where rate_tier = 'standard')
            / nullif(count(*), 0),
        2)                                                              as standard_listing_pct,
        round(
            100.0 * count(*) filter (where rate_tier = 'premium')
            / nullif(count(*), 0),
        2)                                                              as premium_listing_pct

    from services
    group by service_category
),

-- ----------------------------------------------------------------
-- Derived: supply-demand tension.
-- jobs_per_active_contractor flags categories where a small pool of
-- contractors is absorbing a large share of demand — a supply-gap risk.
-- ----------------------------------------------------------------
final as (
    select
        -- Identity
        coalesce(d.service_category, s.service_category)               as service_category,

        -- ----------------------------------------------------------------
        -- Demand (job activity)
        -- ----------------------------------------------------------------
        coalesce(d.total_jobs, 0)                                       as total_jobs,
        coalesce(d.completed_jobs, 0)                                   as completed_jobs,
        coalesce(d.canceled_jobs, 0)                                    as canceled_jobs,
        d.job_completion_rate_pct,

        -- Revenue
        coalesce(d.total_gross_revenue_usd, 0)                         as total_gross_revenue_usd,
        coalesce(d.total_net_revenue_usd, 0)                           as total_net_revenue_usd,
        d.avg_job_gross_usd,
        coalesce(d.total_platform_fee_usd, 0)                          as total_platform_fee_usd,

        -- Refund / dispute
        coalesce(d.jobs_with_refunds, 0)                               as jobs_with_refunds,
        coalesce(d.total_refunded_usd, 0)                              as total_refunded_usd,
        d.refund_rate_pct,
        coalesce(d.full_refund_jobs, 0)                                as full_refund_jobs,

        -- Effort quality
        d.avg_hours_variance,

        -- Recency
        d.last_job_at,

        -- ----------------------------------------------------------------
        -- Supply (listing / contractor coverage)
        -- ----------------------------------------------------------------
        coalesce(s.total_listings, 0)                                  as total_listings,
        coalesce(s.available_listings, 0)                              as available_listings,
        coalesce(s.contractor_count, 0)                                as contractor_count,
        coalesce(s.active_contractor_count, 0)                         as active_contractor_count,

        -- Rate distribution
        s.min_hourly_rate_usd,
        s.avg_hourly_rate_usd,
        s.max_hourly_rate_usd,
        s.budget_listing_pct,
        s.standard_listing_pct,
        s.premium_listing_pct,

        -- ----------------------------------------------------------------
        -- Supply-demand tension
        -- ----------------------------------------------------------------
        -- Jobs per active contractor: higher = tighter supply relative to demand
        round(
            coalesce(d.completed_jobs, 0)::decimal
            / nullif(s.active_contractor_count, 0),
        2)                                                              as completed_jobs_per_active_contractor

    from demand         d
    full outer join supply s using (service_category)
)

select * from final
