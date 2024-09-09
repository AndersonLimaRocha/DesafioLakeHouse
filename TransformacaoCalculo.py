# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, date_format
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

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
# 1. Calcular o total de transações por condomínio
def calcular_total_transacoes_por_condominio(silverzonepath):
    # Executa o Join entre imóveis e condominios
    df_cond_trans = df_transacoes.join(df_imoveis, df_transacoes.imovel_id == df_imoveis.imovel_id)\
                                 .join(df_condominios,['condominio_id'])
    # Calcula a quantidade
    total_transacoes_condominio = df_cond_trans.groupBy("condominio_id").agg(count("*").alias("total_transacoes"))
    #total_transacoes_condominio.show()
    
    # Salvar em Parquet
    total_transacoes_condominio.write.mode("overwrite").parquet(os.path.join(silverzonepath, 'total_transacoes_condominio.parquet'))



# %%
# 2. Calcular o valor total das transações por morador
def calcular_valor_total_transacoes_por_morador(silverzonepath):
    valor_total_morador = df_transacoes.groupBy("morador_id").agg(sum("valor_transacao").alias("valor_total_transacoes"))
    #valor_total_morador.show()
    
    # Salvar em Parquet
    valor_total_morador.write.mode("overwrite").parquet(os.path.join(silverzonepath, 'valor_total_transacoes_morador.parquet'))



# %%
# 3. Agregar as transações diárias por tipo de imóvel
def agregar_transacoes_diarias_por_tipo_imovel(silverzonepath):
    transacoes_diarias_imovel = df_transacoes \
                               .join(df_imoveis, df_transacoes["imovel_id"] == df_imoveis["imovel_id"]) \
                               .groupBy(date_format("data_transacao", "yyyy-MM-dd").alias("data"), "tipo") \
                               .agg(count("*").alias("total_transacoes_diarias"))
    
    #transacoes_diarias_imovel.show()
    
    # Salvar em Parquet
    transacoes_diarias_imovel.write.mode("overwrite").parquet(os.path.join(silverzonepath, 'transacoes_diarias_imovel.parquet'))

# %%
# Função principal para rodar todas as transformações
def run_transformacoes(silverzonepath,subject, body, recipient_email, sender_email, sender_password,smtp_server, smtp_port ):
    calcular_total_transacoes_por_condominio(silverzonepath)
    calcular_valor_total_transacoes_por_morador(silverzonepath)
    agregar_transacoes_diarias_por_tipo_imovel(silverzonepath)

    send_email(subject, body, recipient_email, sender_email, sender_password, smtp_server, smtp_port)

# %%
# Informações de e-mail
recipient_email = "andersonlimarocha@gmail.com"
sender_email = "and.al@hotmail.com"
sender_password = 'Nov@S&nh@2024' 
subject = "Logs do Processo de Calculo e Transformação"
body = f"Aqui estão os logs do processo de extração e validação:"
smtp_server = "smtp-mail.outlook.com"
smtp_port = '587'

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Desafio Superlogica - Calculos e Transformacoes").getOrCreate()

# Caminho Origem dos arquivos da camada Bronze do Data Lake
data_lake_path = "/Users/familialima/Desktop/Desafio SuperLogica/DataLakePath/BonzeZone/Inputs"

silverzone = "/Users/familialima/Desktop/Desafio SuperLogica/DataLakePath/SilverZone/Outputs/"

# Carregar as tabelas necessárias (Assumindo que os dados já estão em Parquet no Data Lake)
df_transacoes = spark.read.parquet(os.path.join(data_lake_path, 'transacoes.parquet'))
df_condominios = spark.read.parquet(os.path.join(data_lake_path, 'condominios.parquet'))
df_moradores = spark.read.parquet(os.path.join(data_lake_path, 'moradores.parquet'))
df_imoveis = spark.read.parquet(os.path.join(data_lake_path, 'imoveis.parquet'))


# Executar o pipeline de transformações
run_transformacoes(silverzone, subject, body, recipient_email, sender_email, sender_password,smtp_server, smtp_port )

#Finalizar a sessão Spark
spark.stop()


