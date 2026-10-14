select
    "LocationID"    as location_id,
    "Borough"       as borough,
    "Zone"          as zone,
    service_zone
from {{ ref('taxi_zones') }}
