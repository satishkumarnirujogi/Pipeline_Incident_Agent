{{ config(materialized='table') }}

select location_id, borough, zone, service_zone
from {{ ref('stg_zones') }}
