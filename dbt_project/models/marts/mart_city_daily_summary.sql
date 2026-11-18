select
    city_id,
    city_name,
    country,
    region,
    observed_date_utc,
    count(*) as observations_count,
    round(cast(avg(temperature_c) as numeric), 2) as avg_temperature_c,
    round(cast(min(temperature_c) as numeric), 2) as min_temperature_c,
    round(cast(max(temperature_c) as numeric), 2) as max_temperature_c,
    round(cast(avg(relative_humidity_pct) as numeric), 1) as avg_humidity_pct,
    round(cast(sum(precipitation_mm) as numeric), 2) as total_precipitation_mm,
    round(cast(max(wind_speed_kmh) as numeric), 1) as max_wind_speed_kmh,
    round(cast(avg(us_aqi) as numeric), 1) as avg_us_aqi,
    round(cast(max(us_aqi) as numeric), 1) as max_us_aqi,
    round(cast(avg(pm2_5) as numeric), 2) as avg_pm2_5,
    round(cast(avg(comfort_index_score) as numeric), 1) as avg_comfort_index
from {{ ref('fct_city_hourly_metrics') }}
group by
    city_id,
    city_name,
    country,
    region,
    observed_date_utc
