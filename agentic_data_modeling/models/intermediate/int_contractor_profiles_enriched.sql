with source as (
    select * from {{ ref('stg_contractor_profiles') }}
),

-- Normalize tier once; eliminates the duplicate lower(trim(...)) that was
-- required for both contractor_tier and is_premium_tier in the same SELECT.
normalized as (
    select
        * exclude (contractor_tier),
        lower(trim(contractor_tier)) as contractor_tier
    from source
),

enriched as (
    select
        -- Primary key (contractor is a user; keep column name clear)
        cast(user_id as bigint) as contractor_id,

        -- Tier: normalized in CTE above
        contractor_tier,

        -- Boolean: derived from the already-normalized value
        contractor_tier = 'premium' as is_premium_tier,

        -- Commission rate: store as precise decimal
        cast(commission_rate as decimal(5, 4)) as commission_rate,

        -- Derived: platform keeps this share of each job's gross amount
        cast(commission_rate * 100 as decimal(5, 2)) as commission_rate_pct,

        -- Eligibility flags (already boolean)
        onboarding_completed,
        background_check_passed,

        -- Derived: contractor can accept jobs only when both checks pass
        (onboarding_completed and background_check_passed) as is_eligible_to_work

    from normalized
)

select * from enriched
