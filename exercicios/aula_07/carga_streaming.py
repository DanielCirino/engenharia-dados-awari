from kafka import KafkaConsumer
import json
from exercicios.aula_07.carga_batch import processarArquivo

# Cria um consumidor com o Kafka para ler as mensagens para processamento dos arquivos
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
        # Decodifica a mensagem
        dados = message.value

        # Normaliza o caminho para o arquivo
        caminhoArquivo = dados["arquivo"].replace("/","\\")

        # Processa o arquivo
        processarArquivo(caminhoArquivo)