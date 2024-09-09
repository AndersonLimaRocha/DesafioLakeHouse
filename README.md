# DesafioSuperLogica
Repositório criado para o Desafio de Engenheiro de Dados da Superlogica
objetivo a extração de dados de um banco de dados PostgreSQL, validação e salvamento no formato Parquet em um Data Lake, além de realizar transformações e cálculos sobre os dados extraídos. O script também inclui a funcionalidade de enviar logs de processo por e-mail.

## Estrutura
1. Dependências
As bibliotecas e pacotes utilizados no script incluem:

warnings: Para desabilitar avisos irrelevantes.
pyspark: Para criação de sessões Spark, manipulação e processamento de dados.
pandas: Para manipulação de logs.
smtplib, MIMEMultipart, MIMEText: Para envio de e-mails com os logs do processo.
os: Para manipulação de caminhos de arquivos.
2. Funções Principais
send_email
Esta função é responsável por enviar os logs de sucesso ou falha do processo para um e-mail especificado.
