select distinct
    city_id,
    city_name,
    country,
    region,
    latitude,
    longitude
from {{ ref('stg_weather_hourly') }}
