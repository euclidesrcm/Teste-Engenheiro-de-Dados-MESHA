import pyodbc 


# Parâmetros de acesso ao banco de dados
database_server = 'db'
database_name = 'master'
schema_name = 'dw_enem_2020'
database_url = f'jdbc:sqlserver://{database_server};databaseName={database_name};trustServerCertificate=true'
database_connection_details = {
    'user': 'sa',
    'password': 'Your_password123',
    'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}

# Pyodbc
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+database_server+';DATABASE='
    +database_name+';UID='+database_connection_details['user']+';PWD='+ database_connection_details['password'])
cursor = cnxn.cursor()