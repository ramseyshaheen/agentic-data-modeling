with source as (
    select * from {{ ref('stg_jobs') }}
),

-- Normalize status once; all downstream references use this value.
-- Avoids repeated lower(trim(...)) calls and closes the latent bug where
-- boolean flags were comparing against the raw (un-normalized) column.
normalized as (
    select
        * exclude (job_status),
        lower(trim(job_status)) as job_status
    from source
),

enriched as (
    select
        -- Primary and foreign keys
        cast(job_id         as bigint) as job_id,
        cast(service_id     as bigint) as service_id,
        cast(contractor_id  as bigint) as contractor_id,
        cast(customer_id    as bigint) as customer_id,

        -- Timestamps
        cast(job_requested_at as timestamp) as requested_at,
        cast(job_completed_at as timestamp) as completed_at,  -- NULL for canceled jobs

        -- Status: all flags reference the single normalized value from above
        job_status,
        job_status = 'completed'                                          as is_completed,
        job_status in ('canceled_by_customer', 'canceled_by_contractor') as is_canceled,
        case
            when job_status = 'canceled_by_customer'   then 'customer'
            when job_status = 'canceled_by_contractor' then 'contractor'
            else null
        end                                                               as canceled_by,

        -- Hours: retain source precision; rounding belongs in marts
        cast(estimated_hours as decimal(10, 2)) as estimated_hours,
        cast(actual_hours    as decimal(10, 2)) as actual_hours,  -- NULL when canceled

        -- Derived: variance only meaningful for completed jobs; no premature rounding
        case
            when job_status = 'completed'
            then actual_hours - estimated_hours
        end as hours_variance,

        -- Revenue: null preserved for canceled jobs (null ≠ zero; marts decide)
        cast(gross_amount as decimal(10, 2)) as gross_amount_usd,

        -- Derived: calendar duration in hours (null for in-progress / canceled)
        case
            when job_completed_at is not null
            then date_diff(
                'hour',
                cast(job_requested_at as timestamp),
                cast(job_completed_at as timestamp)
            )
        end as job_duration_hours

    from normalized
)

select * from enriched
