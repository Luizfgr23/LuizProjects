import pandas as pd
from database_connector import DatabaseConnector 

# Como neste arquivo pegaremos os dados do ODS e jogaremos no DW, precisamos importar a classe database_connector e usar sua função

class DataProcessor:
    def __init__(self, ods_dbname, dw_dbname, user, password, host, port):
        self.ods_connector = DatabaseConnector(ods_dbname, user, password, host, port) # Pega as informações do banco ODS
        self.dw_connector = DatabaseConnector(dw_dbname, user, password, host, port) # Pega as informações do banco DW
        
    def execute(self):
        conn_ods, engine_ods = self.ods_connector.connect_to_database()  #Executa a funcao do arquvio database_connector para o ODS
        conn_dw, engine_dw = self.dw_connector.connect_to_database() #Executa a funcao do arquvio database_connector para o DW

        if conn_ods and engine_ods and conn_dw and engine_dw:
            print("Connection to databases successful.")

            try:
                consulta_sql = "SELECT * FROM esgoto.mensagens"
                df = pd.read_sql(consulta_sql, engine_ods) # Lê a tabela do ODS
                df['data'] = pd.to_datetime(df['data'])
                result = (
                    df.groupby(pd.Grouper(key='data',freq='5min')) # Agrupa os dados pelos últimos 5 minutos da coluna data 
                    .agg({ 'payload': 'mean'}) # Pega a última data do agrupamento e a média destes dados
                    .rename(columns={'payload': 'media_ultimos_5min'}) # Renomeia a coluna
                    .reset_index()
                )
                result['media_ultimos_5min'] = result['media_ultimos_5min'].fillna(-1)
                result['media_ultimos_5min']=result['media_ultimos_5min'].round(2) # Arredonda os dados
                result.to_sql('vazoes_agrupadas', engine_dw, schema='esgoto', if_exists='replace', index=True) # Apaga as linhas da tabela antes da inserção 
                print("Processo completo")                                                                     # e cria um identificador para os valores agrupados
            except Exception as e:
                print(f"Erro no processamento de dados: {str(e)}")
            finally:
                conn_ods.close()
                conn_dw.close() # Fecha as conexões com os bancos
        else:
            print("Erro na conexâo com o banco de dados")

            