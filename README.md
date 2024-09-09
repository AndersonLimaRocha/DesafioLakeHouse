# Desafio Arquitetura Lake House
Repositório criado para o Construção de um Lake House
Extração de dados de um banco de dados PostgreSQL, validação e salvamento no formato Parquet em um Data Lake, além de realizar transformações e cálculos sobre os dados extraídos. O script também inclui a funcionalidade de enviar logs de processo por e-mail.

## Entregáveis: 
* **Desenho da Arquitetura Lakehouse** - Documento descrevendo o ambiente e tecnologias que serão utilizadas no projeto.
* **Pipeline_Superlogica.py** - Arquivo de parametrização de uma DAG do Airflow.
* **ExtracaoValidacao.py** - Script de Extração da base de dados PostgreSQL e processamento no Spark, que será orquestrado pelo Airflow
* **TransformacaoCalculo.py** - Script das transformações e cálculos de transações que será orquestrado pelo Airflow em sequencia ao script acima.
* **Subir os Serviços Docker do Desafio** - Arquivo detalhando os comandos e passos para subir os serviços Docker do projeto


### Tecnologias Utilizadas
*	**Docker**: Plataforma para criação de ambientes isolados e replicáveis para cada aplicação.
*	**Python**: Linguagem de programação utilizada para escrever os scripts e definir as tarefas.
*	**PostgreSQL**: Banco de dados relacional utilizado para armazenar metadados e informações sobre as execuções.
*	**Airflow**: Plataforma de orquestração de workflows para definir, programar e monitorar pipelines de dados.
*	**Spark**: Framework de processamento de grandes volumes de dados, utilizado para tarefas de ETL e análise de dados.
*	**VS Code**: Editor de código versátil com extensões para facilitar o desenvolvimento em Python e integração com outras ferramentas.
*	**GitHub**: Plataforma de controle de versão utilizada para gerenciar o código fonte e colaborar em equipe.


### 1.Requisitos 
Antes de executar o projeto, instale os seguintes pacotes e ferramentas: 
* **Apache Spark** (versão compatível com PySpark) 
* **Java 8 ou superior**
* **PostgreSQL JDBC Driver**
* **Python 3.x**
  
* Pacotes Python:
* **warnings**: Para desabilitar avisos irrelevantes.
* **pyspark**: Para criação de sessões Spark, manipulação e processamento de dados.
* **pandas**: Para manipulação de logs.
* **smtplib, MIMEMultipart, MIMEText**: Para envio de e-mails com os logs do processo.
* **os**: Para manipulação de caminhos de arquivos.

### 2.Scripts do Processo
* **Script - Calculos e Transformacoes.ipynb** - Script de Extração da base de dados PostgreSQL e processamento no Spark
* **Funções**: 
** **send_email** - Esta função é responsável por enviar os logs de sucesso ou falha do processo para um e-mail especificado.
** **extract_and_validate_to_parquet** - Esta função realiza a extração de dados de uma tabela no banco de dados PostgreSQL, valida os dados e armazena os resultados no formato Parquet em um Data Lake.
** **run** - Executa o processo de extração e validação para uma lista de tabelas e envia os logs gerados por e-mail.

* **Script - Calculos e Transformacoes.ipynb** - Script das transformações e cálculos de transações.
* **Funções**:
* **calcular_total_transacoes_por_condominio** - Calcula o valor total das transações realizadas por cada morador e salva os resultados no formato Parquet.
* **calcular_valor_total_transacoes_por_morador** - Calcula o valor total das transações realizadas por cada morador e salva os resultados no formato Parquet.
* **agregar_transacoes_diarias_por_tipo_imovel** - Agrega as transações diárias por tipo de imóvel e salva os resultados no formato Parquet.

### 3.Fluxo Principal
* **Configurações de Conexão**: O script define as configurações de conexão com o banco de dados PostgreSQL e o caminho do Data Lake onde os arquivos serão armazenados.

* **Execução da Extração e Validação**: A função run é executada para cada tabela, realizando a extração e salvamento dos dados no formato Parquet. Os logs são armazenados e enviados por e-mail.

* **Transformações e Cálculos**: Após a extração dos dados, as funções de cálculo e agregação são executadas, gerando novos arquivos Parquet com os resultados. O pipeline de transformação finaliza com o envio de um e-mail contendo os logs do processo.

#### Estrutura
.
├── README.md
├── requirements.txt
├── script.py
└── parquet_files/


Finalização: Após a execução do pipeline, a sessão Spark é encerrada.
