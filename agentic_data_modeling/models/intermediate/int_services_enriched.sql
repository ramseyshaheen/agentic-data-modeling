with source as (
    select * from {{ ref('stg_services') }}
),

-- Normalize category once; establishes the pattern and prevents drift
-- if additional category-based expressions are added later.
normalized as (
    select
        * exclude (service_category),
        lower(trim(service_category)) as service_category
    from source
),

enriched as (
    select
        -- Primary and foreign keys
        cast(service_id    as bigint) as service_id,
        cast(contractor_id as bigint) as contractor_id,

        -- Category: normalized in CTE above
        service_category,

        -- Hourly rate: precise decimal
        cast(hourly_rate as decimal(10, 2)) as hourly_rate_usd,

        -- Derived: bucket rates into tiers for reporting
        --   Data range observed: ~$20 – $200
        case
            when hourly_rate <  75  then 'budget'
            when hourly_rate < 125  then 'mid_range'
            else                         'premium'
        end as rate_tier,

        -- Timestamps
        cast(created_at as timestamp) as created_at,

        -- Availability flag (already boolean)
        is_available

    from normalized
)

select * from enriched
