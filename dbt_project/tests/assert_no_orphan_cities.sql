-- Singular data quality test: every fact row must map to a valid city in dim_cities
select
    f.observation_pk,
    f.city_id
from {{ ref('fct_city_hourly_metrics') }} as f
left join {{ ref('dim_cities') }} as d
    on f.city_id = d.city_id
where d.city_id is null
