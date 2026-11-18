with weather as (
    select * from {{ ref('stg_weather_hourly') }}
),

air_quality as (
    select * from {{ ref('stg_air_quality_hourly') }}
)

select
    w.weather_pk as observation_pk,
    w.city_id,
    w.city_name,
    w.country,
    w.region,
    w.latitude,
    w.longitude,
    w.observed_at_utc,
    w.observed_date_utc,
    w.temperature_c,
    w.relative_humidity_pct,
    w.apparent_temperature_c,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.surface_pressure_hpa,
    w.cloud_cover_pct,
    w.weather_code,
    aq.pm10,
    aq.pm2_5,
    aq.carbon_monoxide,
    aq.nitrogen_dioxide,
    aq.ozone,
    aq.us_aqi,
    aq.uv_index,
    coalesce(aq.aqi_category, 'Unknown') as aqi_category,
    round(
        cast(
            greatest(
                0.0,
                least(
                    100.0,
                    100.0
                    - abs(w.temperature_c - 21.0) * 2.1
                    - abs(coalesce(w.relative_humidity_pct, 50.0) - 50.0) * 0.45
                    - coalesce(aq.us_aqi, 25.0) * 0.25
                    - least(w.wind_speed_kmh, 40.0) * 0.3
                )
            ) as numeric
        ),
        1
    ) as comfort_index_score
from weather as w
left join air_quality as aq
    on w.city_id = aq.city_id
    and w.observed_at_utc = aq.observed_at_utc
