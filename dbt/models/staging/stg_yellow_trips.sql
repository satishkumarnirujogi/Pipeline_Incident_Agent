{# One month of trips, snake_case, with a deterministic trip_id from the natural key.
   payment_type is part of the key: upstream re-sends some trips with payment_type 0. #}
{% set month = var('month') %}

select
    md5(concat_ws('|',
        vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
        pulocationid, dolocationid, total_amount, payment_type
    ))                                  as trip_id,
    pickup_month,
    vendorid                            as vendor_id,
    tpep_pickup_datetime                as pickup_at,
    tpep_dropoff_datetime               as dropoff_at,
    passenger_count::integer            as passenger_count,
    trip_distance,
    ratecodeid::integer                 as rate_code_id,
    store_and_fwd_flag,
    pulocationid                        as pu_location_id,
    dolocationid                        as do_location_id,
    payment_type::integer               as payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
from {{ source('raw', 'yellow_trips') }}
where pickup_month = '{{ month }}'
  -- a few rows per month carry pickups from other months (see docs/data_profile.md)
  and tpep_pickup_datetime >= date '{{ month }}-01'
  and tpep_pickup_datetime <  date '{{ month }}-01' + interval '1 month'
