with source as (
    select * from read_parquet('../data/raw/users.parquet')
)

select * from source
