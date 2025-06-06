create table data_conversion.fact_rankings_diarios (
  url string,
  event_date date,
  posicao_organica int64,
  cliques int64,
  impressoes int64,
  conversoes_organicas int64,
  sessoes_organicas int64,
  ctr_percent int64,
  url_date_key string
);