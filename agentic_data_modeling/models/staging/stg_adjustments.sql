with source as (
    select * from read_parquet('../data/raw/adjustments.parquet')
)

select * from source