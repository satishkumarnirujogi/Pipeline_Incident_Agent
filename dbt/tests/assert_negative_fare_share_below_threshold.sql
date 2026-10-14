{# Fails when the share of negative fares in the month exceeds the threshold.
   Clean baseline 0.36-0.42%; threshold var max_negative_fare_share (2%). #}
select
    count(*) filter (where fare_amount < 0)::float / nullif(count(*), 0) as negative_fare_share,
    count(*) filter (where fare_amount < 0)                              as negative_fares,
    count(*)                                                             as trips
from {{ ref('stg_yellow_trips') }}
having count(*) filter (where fare_amount < 0)::float / nullif(count(*), 0)
       > {{ var('max_negative_fare_share') }}
