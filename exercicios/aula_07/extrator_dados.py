import json

import pymongo
from kafka import KafkaProducer
from pymongo import MongoClient
import pandas as pd
import time
from datetime import datetime

""""
Simula um processo de extração de dados de um banco de dados, dividindo resultado em vários arquivos e utilizando
streaming com Kafka para controle de processamento.
"""

#cliente mongoDB para obter dados dos municípios salvos no exercício da aula 06
mongodb_client = MongoClient(
    "mongodb://root:rootpassword@127.0.0.1:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256")
db = mongodb_client['exercicios']
collection = db["cidades"]

#Produtor de mensagens para envio para a fila do Kafka
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))


if __name__ == "__main__":
    #Obtem os registros da coleção de cidades do mongoDB, ordenando por nome
    cidades = collection.find().sort([("nome", pymongo.ASCENDING)])

    #Cria um Dataframe do pandas com o resultado da consulta feita no mongoDB
    dfCidades = pd.DataFrame(list(cidades))

    #O objetivo dividir as cidades em 50 arquivos csv para simular o processamento
    qtdArquivos = 50

    #Definição de quais índices ficarão em cada arquivo.
    listaIndices = [[] for _ in range(50)]
    for index, row in dfCidades.iterrows():
        listaIndices[index % qtdArquivos].append(index)

    #Para cada lista na lista de índices, será criado um arquivo .csv
    for i, indices in enumerate(listaIndices):
        caminhoArquivo=f"arquivos/cidades_{i}.csv"
        #Filtrar o Dataframe de cidades para obter apenas os registros que possuem os índices selecionados
        df = dfCidades.filter(items=indices, axis=0).reset_index()

        #Salva o Dataframe filtrado no formato .csv no diretório.
        df.to_csv(caminhoArquivo, index=False)
        print(f"[INFO] Gerado arquivo de cidades em: arquivos/cidades_{i}.csv [{len(df)} cidades]")

        #Espera 10 segundos para simular chegada de arquivos em diferentes momentos.
        time.sleep(10)

        #Envia para a fila do kafka a mensagem com os dados do arquivo que precisa ser processado
        dadosFila = {"arquivo":caminhoArquivo,"dataHora":datetime.utcnow().isoformat()}
        producer.send("aula-07-exercicios-estados-municipios",dadosFila)
