-- Singular data quality test: relative humidity must always remain in [0, 100]
select
    observation_pk,
    city_id,
    relative_humidity_pct
from {{ ref('fct_city_hourly_metrics') }}
where relative_humidity_pct < 0
   or relative_humidity_pct > 100
