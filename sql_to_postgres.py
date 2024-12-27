import pandas as pd
import os
import time
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import psycopg2

pd.set_option('display.max_columns', None)

# Sql connection parameters
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

sql_query = "SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]"

try:
    # sql to df
    sql_connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_database};"
        f"UID={sql_username};"
        f"PWD={sql_password}"
    )
    sql_conn = pyodbc.connect(sql_connection_string)
    df = pd.read_sql_query(sql_query, sql_conn)
    sql_conn.close()

    print(f"Data fetched from SQL Server:\n{df.head()}")

    # df to postgresql
    pg_conn = psycopg2.connect(
        dbname=pg_db_name,
        user=pg_user,
        password=pg_password,
        host=pg_host,
        port=pg_port
    )
    pg_cursor = pg_conn.cursor()

    table_name = 'rms_table'
    pg_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    create_table_query = f"""
    CREATE TABLE {table_name} (
        {', '.join(f"{col} TEXT" for col in df.columns)}
    )
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()

    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db_name}"
    )
    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"Data successfully inserted into PostgreSQL table '{table_name}'.")

except (pyodbc.Error, psycopg2.Error) as e:
    print("Error occurred:", e)

finally:
    if 'pg_conn' in locals() and pg_conn:
        pg_conn.close()
