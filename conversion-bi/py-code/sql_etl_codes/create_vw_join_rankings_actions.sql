create or replace view data_conversion.vw_join_rankings_actions as

select frd.posicao_organica, frd.url, sa.action, frd.event_date, sa.optimization_date, event_date_max, conversoes_organicas from data_conversion.fact_rankings_diarios frd
left join data_conversion.seo_actions sa on sa.url = frd.url
left join (select url, max(event_date) as event_date_max from `data_conversion.fact_rankings_diarios`
group by 1 ) last_event on last_event.url = frd.url