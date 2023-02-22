{{ config(materialized="table") }}

with external_fhv_tripdata as (
        select *, 
        'fhv' as service_type 
        from {{ ref("stg_fhv_tripdata") }}
),

fhv_all as (
    select *
    from external_fhv_tripdata
),

dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    fhv_all.int64_field_0,
    fhv_all.service_type,
    fhv_all.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_all.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_all.pickup_datetime,
    fhv_all.dropoff_datetime,
    fhv_all.dispatching_base_num,
    fhv_all.Affiliated_base_number,
    fhv_all.SR_Flag
from fhv_all
inner join
    dim_zones as pickup_zone on fhv_all.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_all.dropoff_locationid = dropoff_zone.locationid