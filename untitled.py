import pandas as pd
import os
import time
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import psycopg2

pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# # nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
# # new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
# #
# # mod_time = os.path.getmtime(nnf_file_path)
# # # readable_mod_time = time.ctime(mod_time)
# # readable_mod_time = datetime.fromtimestamp(mod_time)
# #
# # df = pd.read_excel(nnf_file_path, index_col=False)
# # df_new = pd.read_excel(new_nnf_file_path, index_col=False)
# # # df1 = read_notis_file(r"D:\notis_analysis\modified_data\NOTIS_DATA_12DEC2024.xlsx")
# # df = df.loc[:, ~df.columns.str.startswith('Un')]
# # df_new = df_new.loc[:, ~df_new.columns.str.startswith('Un')]
# # df.columns = df.columns.str.replace(' ', '', regex = True)
# # df_new.columns = df_new.columns.str.replace(' ', '', regex = True)
# # df.dropna(how='all', inplace=True)
# # df_new.dropna(how='all', inplace=True)
# # # df[['NNFID', 'NeatID']] = df[['NNFID', 'NeatID']].astype(int)
# # # df.NeatID = df.NNFID.astype(int)
# # list_col = [col for col in df.columns if not col.startswith('NNF')]
# # grouped_df = df.groupby(['NNFID'])[list_col].sum()
# # # for index, row in grouped_df.iterrows():
# # #     print('indx-',int(index),'\n', 'row-\n', row, '\n')
# #
# # merged_df = pd. merge(df1, df, left_on='ctclid', right_on='NNFID', how='left')
# # print(merged_df)
#
# # Database connection parameters
# server = 'rms.ar.db'
# database = 'ENetMIS'
# username = 'notice_user'
# password = 'Notice@2024'
#
# # Query to execute
# query = "SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]"
#
# try:
#     # Establish connection to the database
#     connection_string = (
#         f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#         f"SERVER={server};"
#         f"DATABASE={database};"
#         f"UID={username};"
#         f"PWD={password}"
#     )
#     connection = pyodbc.connect(connection_string)
#     cursor = connection.cursor()
#
#     # Execute the query
#     cursor.execute(query)
#
#     # Fetch the results
#     rows = cursor.fetchall()
#
#     # Print the results
#     print("Query Results:")
#     for row in rows:
#         print(row)
#
# except pyodbc.Error as e:
#     print("Error connecting to database:", e)
# finally:
#     # Ensure the connection is closed
#     if 'connection' in locals() and connection:
#         connection.close()

# SQL Server connection parameters
sql_server = 'rms.ar.db'
sql_database = 'ENetMIS'
sql_username = 'notice_user'
sql_password = 'Notice@2024'

# PostgreSQL connection parameters
pg_db_name = 'data_arathi'
pg_user = 'postgres'
pg_password = 'root'
pg_host = '172.16.47.81'
pg_port = '5432'

# Query to extract data from SQL Server
sql_query = "SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]"

try:
    # Connect to SQL Server and fetch data into a pandas dataframe
    sql_connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_username};"
        f"PWD={sql_password}"
    )
    sql_conn = pyodbc.connect(sql_connection_string)
    df = pd.read_sql_query(sql_query, sql_conn)
    sql_conn.close()  # Close the SQL Server connection after fetching data

    print(f"Data fetched from SQL Server:\n{df.head()}")

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(
        dbname=pg_db_name,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port
    )
    pg_cursor = pg_conn.cursor()

    # Replace PostgreSQL table with the dataframe content
    table_name = 'rms_table'

    # Drop and recreate table in PostgreSQL
    pg_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_table_query = f"""
    CREATE TABLE {table_name} (
        {', '.join(f"{col} TEXT" for col in df.columns)}
    )
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()

    # Use pandas to insert data into PostgreSQL

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db_name}"
    )
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"Data successfully inserted into PostgreSQL table '{table_name}'.")

except (pyodbc.Error, psycopg2.Error) as e:
    print("Error occurred:", e)

finally:
    # Ensure PostgreSQL connection is closed
    if 'pg_conn' in locals() and pg_conn:
        pg_conn.close()
