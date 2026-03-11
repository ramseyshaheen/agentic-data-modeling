with source as (
    select * from read_parquet('../data/raw/payouts.parquet')
)

select * from source