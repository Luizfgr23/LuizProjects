from data_processor import DataProcessor

def main():
    processor=DataProcessor('ODS','DW','----','----','----','5432') # Após a criação da URL de conexão e suas instâncias,
    processor.execute()                                                        # passaremos os valores para os respectivos objetos

if __name__ == "__main__":
    main() # Executa a funcao se ela existir
