with source as (
    select * from read_parquet('../data/raw/jobs.parquet')
)

select * from source