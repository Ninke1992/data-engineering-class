{{ config(materialized='view') }}

with tripdata as 
(
  select *,
  from {{ source('staging','external_fhv_tripdata') }}
)
select
    -- identifiers
    int64_field_0,
    dispatching_base_num,
    Affiliated_base_number,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag,
    
from tripdata


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}