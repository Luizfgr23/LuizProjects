import psycopg2
from sqlalchemy import create_engine

class DatabaseConnector: # Criação da classe e função para inicializar a conexão 
    def __init__(self, dbname, user, password, host, port): # Cada objeto criado, será referenciado na classe principal, e seu valor será 'apontado' lá
        self.db_config = {
            'dbname': dbname, # nome do banco
            'user': user, # usuário
            'password': password, # senha do usuário
            'host': host, # host
            'port': port # 5432
        }

    def connect_to_database(self):# Função que vai executar a conexão com o banco
        try:
            conn = psycopg2.connect(**self.db_config) # Este comando puxa as informações que serão usadas na URL de conexão
            engine = create_engine(f'postgresql+psycopg2://{self.db_config["user"]}:{self.db_config["password"]}@{self.db_config["host"]}:{self.db_config["port"]}/{self.db_config["dbname"]}')
            # A engine é a url de conexão com o banco, é o comando que vai passar as "confirmar" as instâncias de conexão
            return conn, engine 
        except Exception as e:
            print(f"Error na conexão com o banco de dados: {str(e)}")
            return False
