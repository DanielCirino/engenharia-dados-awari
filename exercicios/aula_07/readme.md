# Tarefa: Data Import Techniques
Esta é a tarefa proposta para a 7ª aula do curso de Engenharia de Dados da Awari. Siga as instruções para desenvolvê-la e entregá-la.

## O que fazer?
* Com base no que foi visto em aula e no Docker do curso, fazer o seguinte:
1. Importar as diferenças que são criadas nos dados da pasta /exercicios/municipios-estados/streaming/. A cada vez que um novo arquivo for adicionado, o mesmo deve ser importado para a pasta da UF correspondente e adicionado ao fim do arquivo cidades.csv.
2. Utilizar Apache Kafka para fazer o mesmo processo, mas de maneira automatizada.


## Detalhes da resolução
* O script <code>extrator_dados.py</code> faz a extração de dados previamente carregados (exercício da aula 06) 
em um banco de dados MongoDB. Os dados são divididos em vários arquivos .csv e salvos no diretório <code>/arquivos</code>.

* O script <code>carga_batch.py</code> faz o processamento dos arquivos *.csv salvos no diretório <code>/arquivos</code>
identificando as cidades de cada estado presentes no arquivo e atualizando a lista no minIO. Este processamento utiliza
processamento em lote, recuperando a lista de arquivos desponíveis no momento da execução do script.

* O script <code>carga_batch.py</code> faz o mesmo processamento de arquivos, porém com uma abordagem de streaming, na
qual um consumidor de mensagens do Kafka lê as mensagens postadas com os dados dos arquivos, decodifica a mensagem e
inicia o processamento.

## Tecnologias utilizadas
* Apache Kafka (streaming de dados/mensageria)
* minIO (simula o S3 da AWS)
* MongoDB (banco de dados de documentos)
* Python (para processamento de dados)
