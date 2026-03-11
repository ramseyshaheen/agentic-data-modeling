with source as (
    select * from read_parquet('../data/raw/services.parquet')
)

select * from source