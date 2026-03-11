with source as (
    select * from {{ ref('stg_users') }}
),

-- Normalize city whitespace once so the enriched CTE reads cleanly.
normalized as (
    select
        * exclude (city),
        trim(city) as city
    from source
),

enriched as (
    select
        -- Primary key
        cast(user_id as bigint) as user_id,

        -- Timestamps
        cast(signup_date as timestamp) as signup_at,

        -- Status flags (already boolean; surface explicitly)
        is_active,
        is_suspended,

        -- Derived: single account_status label for convenience
        case
            when is_suspended then 'suspended'
            when is_active    then 'active'
            else                   'inactive'
        end as account_status,

        -- Location
        city

        -- days_since_signup removed: current_timestamp makes it non-deterministic,
        -- breaking snapshot reproducibility and backfills. Compute in marts instead.

    from normalized
)

select * from enriched
