{# Trips, revenue and average fare per pickup zone per hour, joined with weather.
   Incremental by month, replaced on every run like fct_trips. #}
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    pre_hook="{% if is_incremental() %}delete from {{ this }} where pickup_month = '{{ var('month') }}'{% endif %}"
) }}

with trips as (
    select
        pickup_month,
        pu_location_id,
        date_trunc('hour', pickup_at) as pickup_hour,
        count(*)                      as trips,
        sum(total_amount)             as revenue,
        avg(fare_amount)              as avg_fare
    from {{ ref('fct_trips') }}
    where pickup_month = '{{ var("month") }}'
    group by 1, 2, 3
)

select
    t.pickup_month,
    t.pu_location_id,
    z.borough,
    z.zone,
    t.pickup_hour,
    t.trips,
    round(t.revenue::numeric, 2)  as revenue,
    round(t.avg_fare::numeric, 2) as avg_fare,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh
from trips t
left join {{ ref('dim_zones') }} z on z.location_id = t.pu_location_id
left join {{ ref('stg_weather_hourly') }} w on w.weather_hour = t.pickup_hour
