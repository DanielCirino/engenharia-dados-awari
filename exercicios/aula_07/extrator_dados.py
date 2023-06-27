import json

import pymongo
from kafka import KafkaProducer
from pymongo import MongoClient
import pandas as pd
import time
from datetime import datetime

mongodb_client = MongoClient(
    "mongodb://root:rootpassword@127.0.0.1:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256")
db = mongodb_client['exercicios']
collection = db["cidades"]


producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda x:
                         json.dumps(x).encode('utf-8'))


if __name__ == "__main__":
    cidades = collection.find().sort([("nome", pymongo.ASCENDING)])
    dfCidades = pd.DataFrame(list(cidades))

    qtdArquivos = 50
    listaIndices = [[] for _ in range(50)]
    for index, row in dfCidades.iterrows():
        listaIndices[index % qtdArquivos].append(index)

    for i, indices in enumerate(listaIndices):
        caminhoArquivo=f"arquivos/cidades_{i}.csv"
        df = dfCidades.filter(items=indices, axis=0).reset_index()
        df.to_csv(caminhoArquivo, index=False)
        print(f"[INFO] Gerado arquivo de cidades em: arquivos/cidades_{i}.csv [{len(df)} cidades]")

        time.sleep(10)

        dadosFila = {"arquivo":caminhoArquivo,"dataHora":datetime.utcnow().isoformat()}
        producer.send("aula-07-exercicios-estados-municipios",dadosFila)
