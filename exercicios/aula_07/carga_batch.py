import os
from glob import glob
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

#Define um cliente para trabalhar com o servidor de armazenamento de objetos
s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='bTL03baSaNHKXulb',
    aws_secret_access_key='jHJ5XByBNIs3UNcoKM9GCGALMYZEgo85',
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False,
    region_name='sa-east-1'
)

def processarArquivo(arquivo:str):
    print(f"[INFO] **** Processando arquivo: {arquivo}")
    # Gera um Dataframe a partir do arquivo
    dfCidades = pd.read_csv(arquivo)
    # Gera uma lista com os estados das cidades do arquivo
    dfEstados = dfCidades.groupby(by="uf", as_index=True).count()

    for index, row in dfEstados.iterrows():
        #Gera um Dataframe apenas com as cidades da UF que está sendo processada, considerando apenas as colunas úteis
        dfNovasCidades = dfCidades.loc[dfCidades["uf"] == index, "codigo_ibge"::]


        if len(dfNovasCidades)>0:
            try:
                # Buffer necessário para gravar os arquivos no minIO
                buffer = StringIO()

                #Define a chave do arquivo das cidadades do estado para salvar no minIO.
                chaveS3 = f"exercicio/estados_cidades/{index}/cidades_{index}.csv"

                #Tenta recuperar no minIO o arquivo de cidades do estado. Caso não encontre ocorrerá um erro.
                arquivoCidades = s3_client.get_object(
                    Bucket='aula-07',
                    Key=chaveS3
                ).get("Body")

                # Gera um Dataframe com os dados do arquivo de cidades que já estão salvos no minIO
                dfCidadesSalvas = pd.read_csv(arquivoCidades)

                # Gera um novo Dataframe juntado as cidades salvas e a novas cidades, ignorando os índices e eliminando as duplicidades
                dfCidades = pd.concat([dfCidadesSalvas, dfNovasCidades], ignore_index=True).drop_duplicates(
                    subset=["codigo_ibge"], keep="last")


                # Exporta os dados do Dataframe de cidades para o buffer no formato .csv
                dfCidades.to_csv(buffer, index=False)

                # Salva o arquivo .csv das cidades do estado atualizado no minIO
                s3_client.put_object(Body=buffer.getvalue(), Bucket='aula-07',
                                     Key=chaveS3)
                print(f"[INFO] {len(dfNovasCidades)} cidades do estado {index} salvas. Total: {len(dfCidades)}")
            except ClientError as e:
                # Tratamento para quando o arquivo de cidades do estado não existe no minIO
                # Exporta os dados do Dataframe de cidades para o buffer no formato .csv
                dfNovasCidades.to_csv(buffer, index=False)

                # Salva o arquivo .csv das cidades do estado atualizado no minIO
                s3_client.put_object(Body=buffer.getvalue(), Bucket='aula-07',
                                     Key=chaveS3)
                print(f"[INFO] {len(dfNovasCidades)} cidades do estado {index} salvas.")
            except Exception as e:
                print(e)
    nomeArquivo = arquivo.split('\\')[-1]
    os.rename(arquivo, f"arquivos\\processados\\{nomeArquivo}")

if __name__=="__main__":
    #Define o diretório de arquivos
    diretorio_arquivos = "arquivos/"
    # Obtem a lista de arquivos disponíveis no diretório que possuem a extensão .csv
    lista_arquivos = glob(os.path.join(diretorio_arquivos, "*.csv"))


    for arquivo in lista_arquivos:
        # Faz o processamento do arquivo
        processarArquivo(arquivo)