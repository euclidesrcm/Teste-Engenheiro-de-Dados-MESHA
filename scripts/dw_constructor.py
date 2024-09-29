import access as db_access
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit


# Diretório de trabalho Pyspark
pyspark_dir = '/pyspark'


# Parâmetros para criação da Spark Session
app_name = 'Datawarehouse ENEM 2020 Constructor'
master = 'local[*]'
jars_path = f'{pyspark_dir}/jars/mssql-jdbc-12.8.1.jre11.jar'


# Parâmetros para leitura do arquivo CSV contendo os dados brutos
csv_file_path = f'{pyspark_dir}/data/MICRODADOS_ENEM_2020.csv'
file_encoding = 'ISO-8859-1'
csv_separator = ';'
infer_schema = True
header = True


def create_spark_session(app_name, master):
    return SparkSession.builder \
    .appName(app_name) \
    .master(master) \
    .config('spark.jars', jars_path) \
    .getOrCreate()


def get_data_from_csv_file(spark_session, file_path, header, infer_schema, csv_separator, file_encoding):
    return spark_session.read.csv(file_path, header=header, inferSchema=infer_schema, sep=csv_separator, encoding=file_encoding)


def get_candidato_inscrito(raw_data_df):
    return raw_data_df.select('nu_inscricao', 
                              'tp_sexo', 
                              'tp_escola', 
                              'tp_cor_raca', 
                              'tp_presenca_cn', 
                              'tp_presenca_ch', 
                              'tp_presenca_lc', 
                              'tp_presenca_mt', 
                              'tp_status_redacao')


def get_nota(raw_data_df):
    # Extrai notas e número de inscrição a partir dos dados brutos
    df_ft_notas = raw_data_df.select('nu_inscricao', 
                                     'nu_nota_cn', 
                                     'nu_nota_ch', 
                                     'nu_nota_lc', 
                                     'nu_nota_mt', 
                                     'nu_nota_redacao')
    
    # Considerando que, para os fins de BI, não será útil 
    # armazenar as notas dos alunos que não fizeram nenhuma 
    # das provas, a instrução seguinte remove do dataframe 
    # todas as linhas onde não havia valor em nenhuma das 5 notas
    df_ft_notas = df_ft_notas.na.drop(how='all', 
                                      subset=['nu_nota_cn', 
                                              'nu_nota_ch', 
                                              'nu_nota_lc', 
                                              'nu_nota_mt', 
                                              'nu_nota_redacao'])
    
    # A próxima instrução calcula a média dos alunos
    # e armazena a mesma em nova coluna.
    # Se uma ou mais das 5 notas não possuirem valor,
    # será considerada como 0 para fins de calculo da
    # média, mas sem mudar o seu valor no registro da nota
    df_ft_notas = df_ft_notas.withColumn(
        'nu_media',
        ( (coalesce(col('nu_nota_cn'), lit(0)) +
        coalesce(col('nu_nota_ch'), lit(0)) +
        coalesce(col('nu_nota_lc'), lit(0)) +
        coalesce(col('nu_nota_mt'), lit(0)) +
        coalesce(col('nu_nota_redacao'), lit(0))) / 5 ) )
    
    # Retornar o dataframe já tratado
    return df_ft_notas


def save_dataframe_on_database(dataframe, database_url, connection_details, table_name, write_mode):
    try:
        dataframe.write.jdbc(
            url=database_url,
            properties=connection_details,
            table=table_name,
            mode=write_mode)    
        return True
    except:
        return False
    

def create_schema_on_database_if_not_exists(schema_name, database_url, connection_details):
    query = f'''IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}') 
    BEGIN 
        EXEC('CREATE SCHEMA {schema_name}') 
    END'''
    cursor = db_access.cursor
    cursor.execute(query)
    db_access.cnxn.commit()

    
def main():
    # Criar objeto Spark Session
    spark = create_spark_session(app_name, master)

    # Cria o schema que será utilizado no banco de dados de destino
    create_schema_on_database_if_not_exists(schema_name=db_access.schema_name, 
                                            database_url=db_access.database_url, 
                                            connection_details=db_access.database_connection_details)

    # Ler dados do Enem 2020 de arquivo CSV e armazena-los em um dataframe
    raw_data_df = get_data_from_csv_file(spark_session=spark, 
                                         file_path=csv_file_path, 
                                         header=header, 
                                         infer_schema=infer_schema, 
                                         csv_separator=csv_separator, 
                                         file_encoding=file_encoding)
    
    # A partir dos dados brutos, gerar a dimensão candidatos
    df_dim_candidato_inscrito = get_candidato_inscrito(raw_data_df)

    # A partir dos dados brutos, gerar a tabela fato notas
    df_ft_notas = get_nota(raw_data_df)

    # Salvar dataframes como tabelas no banco de dados
    if (not save_dataframe_on_database(df_dim_candidato_inscrito, db_access.database_url, db_access.database_connection_details, f'{db_access.schema_name}.dim_candidato_inscrito', 'overwrite')):
        print('Falha ao salvar dim_candidato no banco de dados')
    if (not save_dataframe_on_database(df_ft_notas, db_access.database_url, db_access.database_connection_details, f'{db_access.schema_name}.ft_notas', 'overwrite')):
        print('Falha ao salvar ft_notas no banco de dados')
    

if __name__ == "__main__":
    main()