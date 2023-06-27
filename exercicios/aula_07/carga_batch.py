import os
from glob import glob
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

diretorio_arquivos = "arquivos/"
lista_arquivos = glob(os.path.join(diretorio_arquivos,"*.csv"))

s3_client = boto3.client('s3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='bTL03baSaNHKXulb',
    aws_secret_access_key='jHJ5XByBNIs3UNcoKM9GCGALMYZEgo85',
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False,
    region_name='sa-east-1'
)

def processarArquivo(arquivo):
    print(f"[INFO] **** Processando arquivo: {arquivo}")
    dfCidades = pd.read_csv(arquivo)
    dfEstados = dfCidades.groupby(by="uf", as_index=True).count()

    for index, row in dfEstados.iterrows():
        dfNovasCidades = dfCidades.loc[dfCidades["uf"] == index, "codigo_ibge"::]
        if len(dfNovasCidades)>0:
            try:
                chaveS3 = f"exercicio/estados_cidades/{index}/cidades_{index}.csv"
                buffer = StringIO()
                arquivoCidades = s3_client.get_object(
                    Bucket='aula-07',
                    Key=chaveS3
                ).get("Body")

                dfCidadesSalvas = pd.read_csv(arquivoCidades)
                dfCidades = pd.concat([dfCidadesSalvas, dfNovasCidades], ignore_index=True).drop_duplicates(
                    subset=["codigo_ibge"], keep="last")

                dfCidades.to_csv(buffer, index=False)
                s3_client.put_object(Body=buffer.getvalue(), Bucket='aula-07',
                                     Key=chaveS3)
                print(f"[INFO] {len(dfNovasCidades)} cidades do estado {index} salvas. Total: {len(dfCidades)}")
            except ClientError as e:
                # Arquivo de cidades do estado ainda não existe
                dfNovasCidades.to_csv(buffer, index=False)
                s3_client.put_object(Body=buffer.getvalue(), Bucket='aula-07',
                                     Key=chaveS3)
                print(f"[INFO] {len(dfNovasCidades)} cidades do estado {index} salvas.")
            except Exception as e:
                print(e)
    nomeArquivo = arquivo.split('\\')[-1]
    os.rename(arquivo, f"arquivos\\processados\\{nomeArquivo}")

if __name__=="__main__":
    for arquivo in lista_arquivos:
        processarArquivo(arquivo)