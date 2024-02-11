
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests


if __name__ == "__main__":
    # estabelece a conexão com a instância do MongoDB usando a URI fornecida. Ela retorna o cliente 
    # o MongoDB que permite interagir com o banco de dados.
    def connect_mongo(uri):

        client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
            return client
        except Exception as e:
            print(e)
            return e                                                

    # utiliza o(a) cliente do MongoDB para criar (se não existir) e conectar-se ao banco de dados especificado
    # pelo parâmetro db_name. Ela retorna o objeto de banco de dados que pode ser usado para interagir com as 
    # coleções dentro dele.    
    def create_connect_db(client, db_name):

        db = client[db_name]
        return db


    # cria (se não existir) e conecta-se à coleção especificada pelo parâmetro col_name dentro do banco de dados 
    # fornecido. Ela retorna o objeto de coleção que pode ser usado para interagir com os documentos dentro dela.
    def create_connect_collection(db, col_name):

        collections = db[col_name]
        return collections

    #  extrai dados de uma API na URL fornecida e retorna os dados extraídos no formato JSON.
    def  extract_api_data(url):

        reponse = requests.get(url)
        return reponse.json()


    # recebe uma coleção e os dados que serão inseridos nela. Ela deve adicionar todos os documentos 
    # à coleção e retornar a quantidade de documentos inseridos.
    def insert_data(col, data):
        docs = col.insert_many(data)
        return len(docs.inserted_ids)


    client = connect_mongo("mongodb+srv://sheylacantalupo:12345@cluster-pipeline.ahm40a7.mongodb.net/?retryWrites=true&w=majority")
    db = create_connect_db(client,"db_produtos_desafio")
    col = create_connect_collection(db,"produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nDocumentos inseridos na colecao: {n_docs}")

    client.close()