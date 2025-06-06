# Códigos utilizados para o Case Analista de BI

## Estrutura da Pasta:
- py_etl_codes/: contém código insert_data_to_bq.py 

- sql_etl_codes/: contém os códigos sql utilizados para criação de views e tabelas utilizadas no Relatório Final.

- connectors/: contém a classe de conexão, bq_connector.py ao BQ .

- Keys/: em tese, conteria a chave da conta de serviço. Por pura segurança não está inserida, mas posso enviar a chave caso queira testar ou demonstrar a apresentação em uma video-chamada.

### Código bq_connector.py

- Classe que contém o conector ao BigQuery.

- Função run_query() realiza quaisquer queries no BQ através da chave da Conta de Serviço que eu criei no IAM do GCP para este projeto.

- Função insert_df_as_table() insere o dataframe no bigquery

- Função query_sheets() recebe o ID e a Aba da planilha google e utiliza a biblioteca gspread para pegar os dados

- AVISO: Foi preciso ativar a Google Sheets API (foi possível ativar no sandbox)

### Código insert_data_to_bq.py:

- ETL que carrega, trata e insere os dados da planilha do case no BigQuery através da Conta de Serviço criada no IAM do GCP.

- Importa o BQConversionConnector do bq_connector.py.

- Esse ETL sobe a aba tabela2_rankings_diarios_expandida. A tabela1_seo_actions_atualizada eu inseri diretamente pela interface BigQuery (que possui conector nativo) para demonstrar que conheço essa possibilidade.

- Função Extract recebe o ID do sheets, Aba do sheets e coluna e utiliza a função query_sheets() do bq_connector para extrair a planilha.

- Função transform() recebe o dataframe e faz o tratamento dos dados
  - Começa retirando as datas duplicadas para a mesma url (considerei as duplicatas como erro na tabela, que faz parte do case. Como não há horário no campo de data, considerei que os dados maiores seriam os últimos capturados pelo site.
  - Os dados passam por tratamento de tipo, recalculam as CTRs para mitigar possível erro e finalizam o tratamento.

-  Função load() verifica quais dados já foram inseridos pela chave url_date_key criada para cada linha e depois insere no BQ pela 

- Nesse ETL toda a planilha é carregada, porém, numa estrutura real, seriam trazidos apenas os dados que foram atualizados ou que pertencem a uma faixa de tempo. Recarregamentos completos, por serem pontuais, seriam feitos rodando o código manualmente.

- Importante ressaltar que não utilizei INSERT e UPDATE porque o BigQuery SandBox não permite INSERT e UPDATE.

## Códigos SQL:

- create_fact_rankings_diarios: cria a tabela onde o ETL insert_data_to_bq.py insere os dados.

- create_vw_rankings_post_last_actions: pega as posições orgânicas e separa em duas colunas: Antes e Depois da Data de Otimização

- create_vw_last_optimizations: pega a última data da otimização para cada url

- create_agg_rankings_diarios: pega todos os dados da tabela fact_rankings_diarios e separa em duas colunas cada métrica: Antes e Depois da Data de Otimização

- create_vw_join_rankings_actions: realiza o join entre a tabela seo_actions e a fact_rankings_diarios pela URL

- Disclaimer: a view vw_rankings_post_last_actions possui os mesmos dados que agg_rankings_diarios. A única diferença foi o momento de criação que surgiu de acordo com a necessidade de análise de dados. Resolvi enviar também como um histórico do que foi feito.





