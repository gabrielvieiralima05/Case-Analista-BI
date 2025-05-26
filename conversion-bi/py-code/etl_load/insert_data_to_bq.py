from connectors.bq_connector import BQConversionConnector
from keys.config import CONFIG
import pandas as pd

#bq_conn = BQConversionConnector(CONFIG)

#realizar extração dos dados:
def extract(id_gsheet, aba, columns_de_para):
    df = bq_conn.query_gsheet(id_gsheet, aba)
    
    df.rename(columns=columns_de_para, inplace=True)

    return df


def transform(df):

    #Remover datas duplicadas e agrupar seus valores
    df = df.groupby(['url', 'event_date'], as_index=False).agg({
    'posicao_organica': 'max',
    'cliques': 'max',
    'impressoes':'max',
    'conversoes_organicas':'max',
    'sessoes_organicas':'max'})

    df['url_date_key'] = df['event_date'].astype(str)+df['url']

    datas_duplicatas = df[df.duplicated(subset=['url', 'event_date'], keep=False)]

    print(datas_duplicatas)

    #garantindo que as datas estejam sempre no mesmo formato:
    df['event_date'] = pd.to_datetime(df['event_date'])
    

    #tratar dados numéricos e padronizar url
    numeric_columns = ['posicao_organica', 'cliques', 'impressoes', 'ctr_percent', 'conversoes_organicas', 'sessoes_organicas']
    df['ctr_percent'] = round(df['cliques'] / df['impressoes'], 4)
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
    df['url'] = df['url'].str.strip().str.lower()

    return df

def load(df, tableName, dataset, key):
    #Verificando quais chaves já estão inseridas antes de relizar o insert
    duplicate_keys_df = bq_conn.run_query("select {0} from {1}.{2}".format(key,dataset, tableName))
    
    duplicate_keys = set(duplicate_keys_df[key].tolist())

    #Exclui linhas que já estão inseridas dentro do banco
    existing_keys = df[key].isin(duplicate_keys)
    to_insert = df[~existing_keys]

    print('Linhas a serem inseridas: ', to_insert[key].tolist())

    df_to_insert = transform(df)

    #inserindo só as linhas cujas chaves não existem na tabela
    bq_conn.insert_df_as_table(df_to_insert, dataset, tableName)

    #fazer consulta para ver se funcionou:
    result = bq_conn.run_query('select * from {0}.{1} limit 5'.format(dataset, tableName))
    print(result.head())

    

def main():
    #URL do Sheets
    id_gsheet = '1Kuasg5i0DIIeNphrQE21SAvfaCivek4_GDcMGNEQY4k'
    aba = 'tabela2_rankings_diarios_expandida'
    dataset = 'data_conversion'
    tableName = 'fact_rankings_diarios'
    key = 'url_date_key'

    columns_de_para = {'Data': 'event_date',
               'URL': 'url',
               'Posição Orgânica': 'posicao_organica',
               'Sessões Orgânicas': 'sessoes_organicas',
               'Cliques': 'cliques',
               'Impressões': 'impressoes',
               'CTR (%)': 'ctr_percent',
               'Conversões Orgânicas': 'conversoes_organicas'}
    
    df = extract(id_gsheet, aba, columns_de_para)
    df = transform(df)
    load(df, tableName, dataset, key)
    print('Carregamento Concluído')


if __name__ == '__main__':
    main()
