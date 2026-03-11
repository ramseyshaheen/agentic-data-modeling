-- Contractors are a subset of users. This dimension combines contractor-specific
-- attributes (tier, commission, eligibility) with base user attributes (city,
-- account status) so consumers don't need to join two tables.

with contractors as (
    select * from {{ ref('int_contractor_profiles_enriched') }}
),

users as (
    select * from {{ ref('int_users_enriched') }}
),

final as (
    select
        -- Primary key
        c.contractor_id,

        -- Contractor tier & commission
        c.contractor_tier,
        c.is_premium_tier,
        c.commission_rate,
        c.commission_rate_pct,

        -- Eligibility
        c.onboarding_completed,
        c.background_check_passed,
        c.is_eligible_to_work,

        -- User-level attributes (contractors are users)
        u.signup_at,
        u.is_active,
        u.is_suspended,
        u.account_status,
        u.city

    from contractors      c
    left join users       u on c.contractor_id = u.user_id
)

select * from final
