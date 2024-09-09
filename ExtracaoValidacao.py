# %%
import warnings 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
from pyspark.sql.types import NumericType
import os
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

warnings.filterwarnings("ignore")

# %%

# Função para enviar e-mail com os logs
def send_email(subject, body, recipient_email, sender_email, sender_password, smtp_server, smtp_port):
    """
    Envia um e-mail com o corpo e o assunto especificados.

    :param subject: Assunto do e-mail.
    :param body: Corpo do e-mail (texto).
    :param recipient_email: E-mail do destinatário.
    :param sender_email: E-mail do remetente (precisa de autenticação).
    :param sender_password: Senha do remetente (ou senha de aplicativo).
    :param smtp_server: Servidor SMTP.
    :param smtp_port: Porta SMTP.
    """
    try:
        # Criação da mensagem de e-mail
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Conectar ao servidor SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Ativar criptografia TLS
        server.login(sender_email, sender_password)  # Autenticar

        # Enviar o e-mail
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        print("E-mail enviado com sucesso.")
        
    except Exception as e:
        print(f"Falha ao enviar e-mail: {e}")

# %%

# Função para processar e validar os dados da tabela
def extract_and_validate_to_parquet(table_name, parquet_file_name, jdbc_url, db_properties, sparkjdbcdrivepath, data_lake_path):
    """
    Extrai, valida e salva os dados de uma tabela no formato Parquet.

    :param table_name: Nome da tabela a ser processada.
    :param parquet_file_name: Nome do arquivo Parquet.
    :param jdbc_url: URL de conexão ao banco de dados.
    :param db_properties: Propriedades do banco de dados.
    :param data_lake_path: Caminho para o Data Lake onde os arquivos Parquet serão salvos.
    
    :return: DataFrame com o log do processo.
    """
    try:
        # Criação da Spark Session
        spark = SparkSession.builder \
            .appName("CondoManage Data Ingestion") \
            .config("spark.jars", sparkjdbcdrivepath) \
            .getOrCreate()
        
        # Extrair os dados do PostgreSQL
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
        
        # Persistir no Data Lake como Parquet
        parquet_path = os.path.join(data_lake_path, parquet_file_name)
        df.write.mode("overwrite").parquet(parquet_path)

        # Log de sucesso
        msg = f"Dados da tabela {table_name} extraídos e salvos em {parquet_path} com sucesso."
        retorno_log = pd.DataFrame([{'Tipo Mensagem': 'Sucesso', 'Descrição da Msg': msg}])
    
    except Exception as e:
        # Log de erro
        msg = f"Erro ao extrair os dados da tabela {table_name}: {e}"
        retorno_log = pd.DataFrame([{'Tipo Mensagem': 'Erro', 'Descrição da Msg': msg}])
    
    return retorno_log


# %%
# Função para executar o pipeline de extração e validação
def run(tables, jdbc_url, db_properties, sparkjdbcdrivepath, data_lake_path, recipient_email, sender_email, sender_password):
    """
    Executa o processo de extração e validação para uma lista de tabelas e envia os logs por e-mail.

    :param tables: Lista de tabelas a serem processadas.
    :param jdbc_url: URL de conexão ao banco de dados.
    :param db_properties: Propriedades de conexão ao banco.
    :param data_lake_path: Caminho para o Data Lake.
    :param recipient_email: E-mail do destinatário dos logs.
    :param sender_email: E-mail do remetente.
    :param sender_password: Senha do remetente.
    
    :return: DataFrame com os logs de todas as tabelas processadas.
    """
    # Variável para armazenar os logs do processo
    dflog = pd.DataFrame(columns=['Tipo Mensagem', 'Descrição da Msg'])

    # Processar cada tabela
    for table in tables:
        log = extract_and_validate_to_parquet(table['table_name'], table['parquet_file'], jdbc_url, db_properties,sparkjdbcdrivepath, data_lake_path)
        dflog = pd.concat([dflog, log], ignore_index=True)

    # Enviar os logs por e-mail
    log_str = dflog.to_string(index=False)
    
    subject = "Logs do Processo de Extração e Validação de Dados"
    body = f"Aqui estão os logs do processo de extração e validação:\n\n{log_str}"
    
    send_email(subject, body, recipient_email, sender_email, sender_password, "smtp-mail.outlook.com", 587)

    return dflog

# %%
# Definições e parâmetros

# Configurações de conexão com o banco de dados PostgreSQL

user = 'postgres'
pwd = '1234'
serverdb = 'localhost'
portserverdb = '5432'
dbname = 'postgres'

db_properties = {
    "user": user,
    "password": pwd,
    "driver": "org.postgresql.Driver"}

# Parametros da String JDBC
jdbc_url = "jdbc:postgresql://"+ serverdb +":"+ portserverdb + "/" + dbname 

# Parametros do caminho Drive JDBC para o Spark
sparkjdbcdrivepath = "/Users/familialima/Desktop/Desafio SuperLogica/PostgreJDBC/postgresql-42.7.4.jar"

# Caminho do Data Lake para salvar os arquivos em Parquet
data_lake_path = "/Users/familialima/Desktop/Desafio SuperLogica/DataLakePath/BonzeZone/Inputs"

# Lista de tabelas a serem processadas
tables = [
    {'table_name': 'moradores', 'parquet_file': 'moradores.parquet'},
    {'table_name': 'condominios', 'parquet_file': 'condominios.parquet'},
    {'table_name': 'transacoes', 'parquet_file': 'transacoes.parquet'}
]

# Informações de e-mail
recipient_email = "andersonlimarocha@gmail.com"
sender_email = "and.al@hotmail.com"
sender_password = 'Nov@S&nh@2024' 

# Executar o processo
logs = run(tables, jdbc_url, db_properties, sparkjdbcdrivepath, data_lake_path, recipient_email, sender_email, sender_password)



