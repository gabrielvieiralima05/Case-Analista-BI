create or replace table data_conversion.agg_rankings_diarios as 

with aggregate_ranking as (select frd.url, 

      nullif(sum(case when last_optimization.optimization_date is null or frd.event_date <= last_optimization.optimization_date then frd.cliques end),0) clicks_before_last_action,

      nullif(sum(case when frd.event_date > last_optimization.optimization_date then frd.cliques end),0) clicks_after_last_action,

      nullif(sum(case when last_optimization.optimization_date is null or frd.event_date <= last_optimization.optimization_date then frd.sessoes_organicas end),0) sessions_before_last_action,

      nullif(sum(case when frd.event_date > last_optimization.optimization_date then frd.sessoes_organicas end),0) sessions_after_last_action,

      nullif(avg(case when last_optimization.optimization_date is null or last_optimization.optimization_date is null or frd.event_date <= last_optimization.optimization_date then frd.posicao_organica end),0) position_before_last_action,

      nullif(avg(case when frd.event_date > last_optimization.optimization_date then frd.posicao_organica end),0) position_after_last_action,

      nullif(sum(case when last_optimization.optimization_date is null or frd.event_date <= last_optimization.optimization_date then frd.impressoes end),0) impressoes_before_last_action,

      nullif(sum(case when frd.event_date > last_optimization.optimization_date then frd.impressoes end),0) impressoes_after_last_action

from `data_conversion.fact_rankings_diarios` frd
left join (select url, max(optimization_date) as optimization_date from `data_conversion.seo_actions` group by 1) last_optimization on last_optimization.url = frd.url
group by 1)

select *, round(clicks_before_last_action/impressoes_before_last_action,4) ctr_percent_previous_actions,
round(clicks_after_last_action/impressoes_after_last_action,4) ctr_percent_after_action from aggregate_ranking