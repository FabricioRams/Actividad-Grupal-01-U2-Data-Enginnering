with source_data as (
    select
        city_id,
        city_name,
        country,
        region,
        latitude,
        longitude,
        observed_at_utc,
        temperature_c,
        relative_humidity_pct,
        apparent_temperature_c,
        precipitation_mm,
        wind_speed_kmh,
        surface_pressure_hpa,
        cloud_cover_pct,
        weather_code,
        ingested_at_utc,
        row_number() over (
            partition by city_id, observed_at_utc
            order by ingested_at_utc desc
        ) as row_num
    from {{ source('raw', 'raw_weather_hourly') }}
)

select
    city_id || '|' || cast(observed_at_utc as varchar) as weather_pk,
    city_id,
    trim(city_name) as city_name,
    trim(country) as country,
    trim(region) as region,
    cast(latitude as double precision) as latitude,
    cast(longitude as double precision) as longitude,
    observed_at_utc,
    cast(date_trunc('day', observed_at_utc at time zone 'UTC') as date) as observed_date_utc,
    cast(temperature_c as double precision) as temperature_c,
    cast(relative_humidity_pct as double precision) as relative_humidity_pct,
    cast(apparent_temperature_c as double precision) as apparent_temperature_c,
    coalesce(cast(precipitation_mm as double precision), 0.0) as precipitation_mm,
    coalesce(cast(wind_speed_kmh as double precision), 0.0) as wind_speed_kmh,
    cast(surface_pressure_hpa as double precision) as surface_pressure_hpa,
    cast(cloud_cover_pct as double precision) as cloud_cover_pct,
    cast(weather_code as integer) as weather_code,
    ingested_at_utc
from source_data
where row_num = 1
  and temperature_c is not null
