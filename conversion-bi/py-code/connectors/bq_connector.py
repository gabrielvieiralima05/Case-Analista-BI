from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import gspread
from gspread_dataframe import get_as_dataframe
import traceback
import pandas as pd


class BQConversionConnector: #Realiza consulta ao BQ
    def __init__(self, config): #Define configurações iniciais do BQ
        #Credenciais de query:
        self.key_path = config["KEY_BQ"]
        self.credentials = service_account.Credentials.from_service_account_info(config["KEY_BQ"], scopes = ['https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery', 'https://www.googleapis.com/auth/cloud-platform',])
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

    def run_query(self, query): #Envia query pro BiQ
        querying =  self.client.query(query)
        
        try:
            results = querying.result().to_dataframe()
        except GoogleCloudError as e:
            err = traceback.format_exc()
            raise e
        
        return results
    
    def insert_df_as_table(self, df,dataset, nameTable): #insere o dataframe como tabela
       
        try:
            df.to_gbq(
                credentials=self.credentials, 
                destination_table='{0}.{1}'.format(dataset, nameTable), 
                if_exists='replace')
        except GoogleCloudError as e:
            err = traceback.format_exc()
            return err

    def query_gsheet(self, id_sheet, aba):
        gc = gspread.authorize(self.credentials)
        spreadsheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/{0}/edit'.format(id_sheet))
        worksheet = spreadsheet.worksheet(aba)

        return get_as_dataframe(worksheet)
        