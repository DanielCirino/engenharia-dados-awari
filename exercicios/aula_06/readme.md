# Tarefa: Organizar Cidades por Estados
Esta é a tarefa proposta para a 6ª aula do curso de Engenharia de Dados da Awari. Siga as instruções para desenvolvê-la e entregá-la.

# O que fazer?
Com base no que foi visto em aula e com base nos dados e arquivos tratados na 5ª aula (aula anterior), a atividade consiste, sempre no workspace em Docker, em:

* Criar script para ler os datasets em /datasets — o script deve:
    * criar uma pasta nomeada com a sigla da UF para cada estado encontrado no arquivo JSON;
    * organizar as cidades por estado um único arquivo CSV, nomeado como cidades.csv;
    * salvar esse arquivo cidades.csv dentro da pasta da UF (estado) correspondente.
* Importar as pastas e arquivos salvos para um bucket no MinIO.
* Exportar os dados para o MongoDB e visualizá-los a partir do banco de dados.