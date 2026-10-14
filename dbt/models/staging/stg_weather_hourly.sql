{# Hourly NYC weather for one month, local time (America/New_York), hour-truncated
   to match pickup_at. Units: degC, mm, cm, km/h. #}
select
    date_trunc('hour', time)    as weather_hour,
    month,
    temperature_2m              as temperature_c,
    precipitation               as precipitation_mm,
    rain                        as rain_mm,
    snowfall                    as snowfall_cm,
    wind_speed_10m              as wind_speed_kmh
from {{ source('raw', 'weather_hourly') }}
where month = '{{ var("month") }}'
