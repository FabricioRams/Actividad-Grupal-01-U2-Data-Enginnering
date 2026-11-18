with city_stats as (
    select
        city_id,
        avg(temperature_c) as baseline_temp_c,
        stddev_samp(temperature_c) as stddev_temp_c,
        avg(us_aqi) as baseline_aqi
    from {{ ref('fct_city_hourly_metrics') }}
    group by city_id
),

scored as (
    select
        f.observation_pk,
        f.city_id,
        f.city_name,
        f.country,
        f.region,
        f.observed_at_utc,
        f.temperature_c,
        round(cast(s.baseline_temp_c as numeric), 2) as baseline_temp_c,
        round(cast(f.temperature_c - s.baseline_temp_c as numeric), 2) as temp_delta_c,
        case
            when coalesce(s.stddev_temp_c, 0) = 0 then 0.0
            else round(cast((f.temperature_c - s.baseline_temp_c) / s.stddev_temp_c as numeric), 2)
        end as temp_z_score,
        f.us_aqi,
        f.pm2_5,
        f.aqi_category,
        f.wind_speed_kmh,
        f.precipitation_mm
    from {{ ref('fct_city_hourly_metrics') }} as f
    inner join city_stats as s
        on f.city_id = s.city_id
)

select
    *,
    case
        when us_aqi >= 100 then 'Air Quality Alert'
        when abs(temp_z_score) >= 1.75 then 'Thermal Anomaly'
        when wind_speed_kmh >= 40.0 then 'High Wind Advisory'
        when precipitation_mm >= 5.0 then 'Heavy Precipitation'
        else 'Normal'
    end as anomaly_type
from scored
where us_aqi >= 100
   or abs(temp_z_score) >= 1.75
   or wind_speed_kmh >= 40.0
   or precipitation_mm >= 5.0
