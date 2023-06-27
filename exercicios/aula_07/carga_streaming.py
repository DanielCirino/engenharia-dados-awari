from kafka import KafkaConsumer
import json
from exercicios.aula_07.carga_batch import processarArquivo
# Cria um consumidor com o Kafka
consumer = KafkaConsumer(
    'aula-07-exercicios-estados-municipios',
    bootstrap_servers=['127.0.0.1:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='pipeline-python',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

if __name__=="__main__":
    # Começa a percorrer as mensagens encontradas no kafka
    for message in consumer:
        message = message.value
        caminhoArquivo = message["arquivo"].replace("/","\\")
        processarArquivo(caminhoArquivo)