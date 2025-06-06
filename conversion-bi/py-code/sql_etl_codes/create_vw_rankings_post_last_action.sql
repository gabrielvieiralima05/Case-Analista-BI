create or replace view data_conversion.vw_rankings_post_last_action as
select frd.url, frd.event_date, case 
      when last_optimization.optimization_date is not null and event_date >= last_optimization.optimization_date then posicao_organica
      end as posicao_organica_post_action,
      case 
      when last_optimization.optimization_date is null or event_date <= last_optimization.optimization_date then posicao_organica
      end as posicao_organica_pre_action
from data_conversion.fact_rankings_diarios frd
left join data_conversion.seo_actions sa on sa.url = frd.url
left join (select url, max(optimization_date) as optimization_date from `data_conversion.seo_actions` group by 1) last_optimization on last_optimization.url = frd.url