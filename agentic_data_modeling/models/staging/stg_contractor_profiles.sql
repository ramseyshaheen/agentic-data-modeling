with source as (
    select * from read_parquet('../data/raw/contractor_profiles.parquet')
)

select * from source