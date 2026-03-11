with source as (
    select * from {{ ref('stg_adjustments') }}
),

-- Normalize type once; was repeated five times across adjustment_type,
-- adjustment_direction, is_full_refund, is_partial_refund, is_goodwill_bonus,
-- and is_refund in the original SELECT.
normalized as (
    select
        * exclude (adjustment_type),
        lower(trim(adjustment_type)) as adjustment_type
    from source
),

enriched as (
    select
        -- Primary and foreign keys
        cast(adjustment_id as bigint) as adjustment_id,
        cast(job_id        as bigint) as job_id,

        -- Timestamps
        cast(adjustment_date as timestamp) as adjusted_at,

        -- Type: normalized in CTE above
        adjustment_type,

        -- Derived: classify direction of cash flow
        --   refunds reduce revenue; bonuses increase contractor pay
        case
            when adjustment_type in ('full_refund', 'partial_refund') then 'debit'
            when adjustment_type = 'goodwill_bonus'                   then 'credit'
        end as adjustment_direction,

        -- Boolean convenience flags
        adjustment_type = 'full_refund'    as is_full_refund,
        adjustment_type = 'partial_refund' as is_partial_refund,
        adjustment_type = 'goodwill_bonus' as is_goodwill_bonus,

        -- Derived: combined refund flag
        adjustment_type in ('full_refund', 'partial_refund') as is_refund,

        -- Amount: precise decimal; always stored as positive magnitude
        cast(adjustment_amount as decimal(10, 2)) as adjustment_amount_usd

    from normalized
)

select * from enriched
