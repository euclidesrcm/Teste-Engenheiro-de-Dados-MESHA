# Teste de Eng. de Dados

## Sobre
Este projeto foi desenvolvido para uma candidatura a uma vaga de engenheiro de dados e consiste na construção de um data warehouse a respeito do ENEM 2020, de forma a levantar indicadores e responder as seguintes perguntas:

1. Qual a escola com a maior média de notas?
2. Qual o aluno com a maior média de notas e o valor dessa média?
3. Qual a média geral?
4. Qual o % de Ausentes?
5. Qual o número total de Inscritos?
6. Qual a média por disciplina?
7. Qual a média por Sexo?
8. Qual a média por Etnia?


## Prerequisitos
Docker configurado e em execução. Testado em um ambiente Windows 11 com Docker Desktop 4.33.1 e WSL 2.

## O que foi utilizado
Foi criado um ambiente em Docker contendo dois containers: um para o Microsoft SQL Server e outro que foi construído através de um Dockerfile que contem um ambiente baseado em Ubuntu 22.04 contendo Open JDK 17 e preparado para o uso das seguintes bibliotecas Python:
- Pyspark;
- pyodbc;
- matplotlib;
- jinja2

Além disso, o docker compose se encarrega de montar os volumes necessários para a execução do projeto e também cria uma rede para a comunicação entre os dois containers.


## Dependências externas
Será necessário colocar no diretório "jars" do projeto o driver JDBC do Microsoft SQL Server e também o arquivo contendo os microdados do ENEM 2020 no diretório "data".

O download do arquivo de Microdados pode ser realizado [clicando aqui](https://download.inep.gov.br/microdados/microdados_enem_2020.zip).
Após o download, é necessário descompactar a pasta e copiar o arquivo no diretório microdados_enem_2020/DADOS/MICRODADOS_ENEM_2020.csv para o diretório "data" do projeto.

O download do driver JDBC pode ser feito [clicando aqui](https://go.microsoft.com/fwlink/?linkid=2283744).
Após, descompactar a pasta e copiar o arquivo sqljdbc_12.8/ptb/jars/mssql-jdbc-12.8.1.jre11.jar para o diretório "jars" do projeto.


## O que o projeto executa
Ao subir os containers do banco de dados SQL Server e também o construído através do Dockefile, será executado um código Pyspark que fará a extração dos dados brutos do arquivo CSV, faz os devidos tratamentos para manter apenas os dados relevantes para responder as perguntas elencadas, criando inclusive uma coluna extra que faz o cálculo da média por candidato. Após, esses dados tratados são salvos no banco de dados e outro script será executado a fim de fazer as consultas SQL necessárias para as respostas, gerando um relatório em HTML com texto e gráficos. O relatório será salvo no diretório report.  


## Tipo de modelagem utilizada
- Modelagem multidimensional utilizando o esquema estrela



## Como executar
1. Clonar o repositório deste projeto;
2. Fazer download do arquivo de Microdados [clicando aqui](https://download.inep.gov.br/microdados/microdados_enem_2020.zip);
3. Descompactar a pasta dos microdados e copiar o arquivo no diretório microdados_enem_2020/DADOS/MICRODADOS_ENEM_2020.csv para o diretório "data" do projeto;
4. Fazer download do driver JDBC [clicando aqui](https://go.microsoft.com/fwlink/?linkid=2283744);
5. Descompactar a pasta do driver JDBC e copiar o arquivo sqljdbc_12.8/ptb/jars/mssql-jdbc-12.8.1.jre11.jar para o diretório "jars" do projeto;
6. Em um ambiente com Docker configurado e em execução, abrir um terminal no diretório raiz do projeto (onde se encontra o arquivo docker-compose.yml);
7. Verificar os parâmetros nos arquivos do diretório scripts. Se for executado no ambiente Docker e utilizar os mesmos arquivos (inclusive com os mesmos nomes) de microdados e do driver JDBC, não será necessário efetuar mudança alguma;
8. Executar o seguinte comando:
   
   Para rodar o projeto em background:
   
   ```
   docker compose up -d
   ```
   
    Para rodar o projeto no terminal (dessa maneira, ao encerrar o processo, o container do banco de dados não estará mais disponível):

   ```
   docker compose up
   ```

9. Após a execução bem sucedida, o relatório estará disponível no diretório "reports" do projeto.


## Considerações
- Para calcular a média por escolas, foi considerado as categorias pública, privada, exterior e não respondeu;
- As médias foram calculadas somando as notas de todas as provas e dividindo a soma por 5. Se o aluno faltou a uma prova em específico, o valor da nota foi considerado como 0 para fins do cálculo da média. Se o aluno faltou em todas as provas, não foi calculado a média e o registro não foi incluído na tabela ft_notas;
- Foi considerado como candidato ausente aquele que faltou nas 4 provas (não foi usado a prova de redação como parâmetro porque não havia um atributo de presença específico para essa prova. Mas, como a redação foi realizada no mesmo dia que outras provas, quem faltou em todas as outras também faltou na redação);
- Médias formatadas com 1 casa decimal e sem arredondamento. Porcentagens formatadas com 2 casas decimais e sem arredondamentos.
