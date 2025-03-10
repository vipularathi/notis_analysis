import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
from dateutil.relativedelta import relativedelta
import progressbar
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pyodbc
from sqlalchemy import create_engine, text, insert
import psycopg2
import time
import warnings
import csv
import paramiko
from db_config import engine_str, n_tbl_notis_trade_book, s_tbl_notis_trade_book, n_tbl_notis_raw_data, s_tbl_notis_raw_data, n_tbl_notis_nnf_data, s_tbl_notis_nnf_data

holidays_25 = ['2025-02-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14', '2025-04-18', '2025-05-01', '2025-08-15', '2025-08-27', '2025-10-02', '2025-10-21', '2025-10-22', '2025-11-05', '2025-12-25']
# holidays_25.append('2024-03-20') #add unusual holidays
today = datetime.now().date()
# today = datetime(year=2025, month=2, day=18).date()
# yesterday = today-timedelta(days=1)
b_days = pd.bdate_range(start=today-timedelta(days=7), end=today, freq='C', weekmask='1111100', holidays=holidays_25).date.tolist()
# b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20)])) #add unusual trading days
today, yesterday = sorted(b_days)[-1], sorted(b_days)[-2]

root_dir = os.path.dirname(os.path.abspath(__file__))
bhav_dir = os.path.join(root_dir, 'bhavcopy')
modified_dir = os.path.join(root_dir, 'modified_data')
table_dir = os.path.join(root_dir, 'table_data')
eod_dir = os.path.join(root_dir, 'eod_data')
dir_list = [bhav_dir, modified_dir, table_dir, eod_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

def read_data_db(nnf=False, for_table='ENetMIS'):
    if not nnf and for_table == 'ENetMIS':
        # Sql connection parameters
        sql_server = "rms.ar.db"
        sql_database = "ENetMIS"
        sql_username = "notice_user"
        sql_password = "Notice@2024"
        sql_query = "SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]"

        try:
            sql_connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={sql_server};"
                f"DATABASE={sql_database};"
                f"UID={sql_username};"
                f"PWD={sql_password}"
            )
            with pyodbc.connect(sql_connection_string) as sql_conn:
                df = pd.read_sql_query(sql_query, sql_conn)
            print(f"Data fetched from SQL Server. Shape:{df.shape}")
            return df

        except (pyodbc.Error, psycopg2.Error) as e:
            print("Error occurred:", e)
    elif nnf and for_table != 'ENetMIS':
        engine = create_engine(engine_str)
        df = pd.read_sql_table(n_tbl_notis_nnf_data, con=engine)
        print(f"Data fetched from {for_table} table. Shape:{df.shape}")
        return df

    elif not nnf and for_table!='ENetMIS':
        engine = create_engine(engine_str)
        df = pd.read_sql_table(for_table, con=engine)
        print(f"Data fetched from {for_table} table. Shape:{df.shape}")
        return df

def read_notis_file(filepath):
    wb = load_workbook(filepath, read_only=True)
    sheet = wb.active
    total_rows = sheet.max_row
    print('Reading Notis file...')
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                           progressbar.Bar(marker='=', left='[', right=']'),
                                                           progressbar.ETA()])

    data = []
    pbar.update(0)
    for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
        data.append(row)
        pbar.update(i)
    pbar.finish()

    df = pd.DataFrame(data[1:], columns=data[0])
    print('Notis file read')
    return df

def read_file(filepath):
    file_extension = os.path.splitext(filepath)[-1].lower()
    data = []
    if file_extension == '.xlsx':
        wb = load_workbook(filepath, read_only=True)
        sheet = wb.active
        total_rows = sheet.max_row
        print('Reading Excel file...')
        pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                               progressbar.Bar(marker='=', left='[', right=']'),
                                                               progressbar.ETA()])

        pbar.update(0)
        for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
            data.append(row)
            pbar.update(i)
        pbar.finish()
        # df = pd.DataFrame(data[1:], columns=data[0])
        print('Excel file read')

    elif file_extension == '.csv':
        total_rows = sum(1 for _ in open(filepath, 'r', encoding='utf-8')) - 1
        print(f'Reading CSV file...')
        pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                               progressbar.Bar(marker='=', left='[', right=']'),
                                                               progressbar.ETA()])
        pbar.update(0)
        with open(filepath, 'r', encoding='utf-8') as csv_file:
            # reader = csv.reader(csv_file)
            # header = next(reader)
            # for i, row in enumerate(reader, start=1):
            #     data.append(row)
            #     pbar.update(i)
            for i, row in enumerate(csv_file, start=0):
                data.append(row.strip().split(','))
                pbar.update(i)
        pbar.finish()
        # df = pd.DataFrame(data, columns=header)
        print('CSV file read')
    df = pd.DataFrame(data[1:], columns=data[0])
    return df

def write_notis_postgredb(df, table_name, raw=False):
    start_time = time.time()
    engine = create_engine(engine_str)

    with engine.begin() as conn:
        res = conn.execute(text(f'select count(*) from "{table_name}"'))
        row_count = res.scalar()
        if row_count > 0:
            conn.execute(text(f'delete from "{table_name}"'))
            print(f'Existing data from table {table_name} deleted')
    print(f'Writing {"Raw" if raw else "Modified"} data to database...')
    total_rows = len(df)
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[
        progressbar.Percentage(), ' ',
        progressbar.Bar(marker='=', left='[', right=']'),
        progressbar.ETA()
    ])

    if raw:
        list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
        list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
        list_none_str = [38]
        for i in list_str_int64:
            # df_db.loc[:, f'Column{i}'] = df_db.loc[:, f'Column{i}'].astype('int64')
            column_name = f'Column{i}'
            df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
        for i in list_str_none:
            df[f'Column{i}'] = None
        for i in list_none_str:
            df[f'Column{i}'] = df[f'Column{i}'].astype('str')
    chunk_size = 1000
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        chunk.to_sql(table_name, engine, index=False, if_exists='append', method='multi')
        pbar.update(min(i + chunk_size, total_rows))

    pbar.finish()
    print(f'{"Raw" if raw else "Modified"} Data successfully inserted into database')
    end_time = time.time()
    print(f'Total time taken: {end_time - start_time} seconds')

# def write_notis_data(df, filepath):
#     print('Writing Notis file to excel...')
#     wb = Workbook()
#     ws = wb.active
#     ws.title = 'Sheet1'
#     rows = list(dataframe_to_rows(df, index=False, header=True))
#     total_rows = len(rows)
#     pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
#                                                            progressbar.Bar(marker='=', left='[', right=']'),
#                                                            progressbar.ETA()])
#     for i, row in enumerate(rows, start=1):
#         ws.append(row)
#         pbar.update(i)
#     pbar.finish()
#     # df.to_excel(os.path.join(modified_dir, file_name))
#     print('Saving the file...')
#     # wb.save(filepath)
#     wb.save(filepath)
#     print('New Notis excel file created')

def write_notis_data(df, filepath):
    file_extention = os.path.splitext(filepath)[-1].lower()
    if file_extention == '.xlsx':
        print('Writing Notis file to excel...')
        wb = Workbook()
        ws = wb.active
        ws.title = 'Sheet1'
        rows = list(dataframe_to_rows(df, index=False, header=True))
        total_rows = len(rows)
        pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                                      progressbar.Bar(marker='=', left='[', right=']'),
                                                                      progressbar.ETA()])
        for i, row in enumerate(rows, start=1):
            ws.append(row)
            pbar.update(i)
        pbar.finish()
        # df.to_excel(os.path.join(modified_dir, file_name))
        print('Saving the file...')
        # wb.save(filepath)
        wb.save(filepath)
    elif file_extention == '.csv':
        print('Writing Notis file to CSV...')
        # df.to_csv(filepath, index=False)
        total_rows = len(df)
        pbar = progressbar.ProgressBar(max_value=total_rows,widgets=[progressbar.Percentage(),' ',progressbar.Bar(marker='=',left='[',right=']'),progressbar.ETA()])
        pbar.update(0)
        with open(filepath,mode='w',encoding='utf-8',newline='') as f:
            writer = csv.writer(f)
            writer.writerow(df.columns)
            for row_num, row in enumerate(df.itertuples(index=False, name=None), start=1):
                writer.writerow(row)
                pbar.update(row_num)
        pbar.finish()
        print('Saving the file...')

def get_date_from_jiffy(dt_val):
    """
    Converts the Jiffy format date to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Jiffy is 1/65536 of a second since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    date_time = int((base_date.timestamp() + (dt_val / 65536)))
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date

def get_date_from_non_jiffy(dt_val):
    """
    Converts the 1980 format date time to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Assuming dt_val is seconds since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    if (type(dt_val) == str):
        dt_val = int(dt_val)
    # date_time = int(base_date.timestamp() + dt_val)
    date_time = base_date.timestamp() + dt_val
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date

def download_bhavcopy():
    host = '192.168.112.81'
    username = 'greek'
    password = 'greeksoft'
    filename = f"regularNSEBhavcopy_{today.strftime('%d%m%Y')}.csv"  # sample=regularBhavcopy_13022025
    remote_path = rf'/home/greek/NSE_BSE_Broadcast/NSE/Bhavcopy/Files/{filename}'
    local_path = os.path.join(bhav_dir, filename)
    try:
        transport = paramiko.Transport((host, 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(remote_path, local_path)
        sftp.close()
        transport.close()
    except Exception as e:
        print(f'Error: {e}')