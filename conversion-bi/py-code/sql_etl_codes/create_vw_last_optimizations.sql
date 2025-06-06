create or replace view data_conversion.vw_last_optimizations as
select frd.url, coalesce(action, '-') as action, optimization_date 
from `data_conversion.fact_rankings_diarios` frd
left join `data_conversion.seo_actions` sa  on frd.url = sa.url
qualify row_number() over (partition by url order by optimization_date desc) = 1