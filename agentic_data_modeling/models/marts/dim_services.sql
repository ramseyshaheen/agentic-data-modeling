with services as (
    select * from {{ ref('int_services_enriched') }}
),

final as (
    select
        -- Primary and foreign keys
        service_id,
        contractor_id,

        -- Service attributes
        service_category,
        hourly_rate_usd,
        rate_tier,
        created_at,
        is_available

    from services
)

select * from final
