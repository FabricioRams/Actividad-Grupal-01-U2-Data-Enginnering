with source_data as (
    select
        city_id,
        observed_at_utc,
        pm10,
        pm2_5,
        carbon_monoxide,
        nitrogen_dioxide,
        ozone,
        us_aqi,
        uv_index,
        ingested_at_utc,
        row_number() over (
            partition by city_id, observed_at_utc
            order by ingested_at_utc desc
        ) as row_num
    from {{ source('raw', 'raw_air_quality_hourly') }}
)

select
    city_id || '|' || cast(observed_at_utc as varchar) as air_quality_pk,
    city_id,
    observed_at_utc,
    cast(pm10 as double precision) as pm10,
    cast(pm2_5 as double precision) as pm2_5,
    cast(carbon_monoxide as double precision) as carbon_monoxide,
    cast(nitrogen_dioxide as double precision) as nitrogen_dioxide,
    cast(ozone as double precision) as ozone,
    cast(us_aqi as double precision) as us_aqi,
    cast(uv_index as double precision) as uv_index,
    case
        when us_aqi is null then 'Unknown'
        when us_aqi <= 50 then 'Good'
        when us_aqi <= 100 then 'Moderate'
        when us_aqi <= 150 then 'Sensitive'
        when us_aqi <= 200 then 'Unhealthy'
        else 'Hazardous'
    end as aqi_category,
    ingested_at_utc
from source_data
where row_num = 1
