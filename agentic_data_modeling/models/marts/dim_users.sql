with users as (
    select * from {{ ref('int_users_enriched') }}
),

final as (
    select
        -- Primary key
        user_id,

        -- Timestamps
        signup_at,

        -- Status flags
        is_active,
        is_suspended,
        account_status,

        -- Location
        city

    from users
)

select * from final
