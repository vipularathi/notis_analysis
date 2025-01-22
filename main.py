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
from db_config import engine_str, n_tbl_notis_trade_book, s_tbl_notis_trade_book, n_tbl_notis_raw_data, s_tbl_notis_raw_data, n_tbl_notis_nnf_data, s_tbl_notis_nnf_data

warnings.filterwarnings('ignore', message="pandas only supports SQLAlchemy connectable.*")

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
            print(f"Data fetched from SQL Server:\n{df.head()}")
            return df

        except (pyodbc.Error, psycopg2.Error) as e:
            print("Error occurred:", e)
    elif nnf and for_table != 'ENetMIS':
        engine = create_engine(engine_str)
        df = pd.read_sql_table(n_tbl_notis_nnf_data, con=engine)
        print(f"Data fetched from NNF table:\n{df.head()}")
        return df

    elif not nnf and for_table!='ENetMIS':
        engine = create_engine(engine_str)
        df = pd.read_sql_table(for_table, con=engine)
        print(f"Data fetched from NNF table:\n{df.head()}")
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

def write_notis_postgredb(df, table_name, raw=False):
    start_time = time.time()
    engine = create_engine(engine_str)

    with engine.connect() as conn:
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

def write_notis_data(df, filepath):
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
    print('New Notis excel file created')

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
    # date_time = int(base_date.timestamp() + dt_val)
    date_time = base_date.timestamp() + dt_val
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date
def modify_file(df, df_nnf):
    # 1 - 12, 16 - 19, 21, 23, 27 - 28    str - int64
    # 15, 20, 25, 30 - 37 = None
    # 38    astype('str')
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
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
    print('Starting file modification...')
    pbar.update(0)
    df.rename(columns={
        'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
        'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
        'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
        'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
        'Column13': 'cliActNo', 'Column14': 'cpCD', 'Column15': 'remarks',
        'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
        'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
        'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
        'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
        'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
        'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
        'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
    }, inplace=True)
    pbar.update(20)

    df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
    pbar.update(40)

    df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
    pbar.update(60)

    df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
    pbar.update(80)
    # --------------------------------------------------------------------------------------------------------------------------------
    df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
    pbar.update(90)

    # df1 = pd.read_excel(nnf_file_path, index_col=False)
    # df1 = df1.loc[:, ~df1.columns.str.startswith('Un')]
    # df1.columns = df1.columns.str.replace(' ', '', regex=True)
    # df1.dropna(how='all', inplace=True)
    df['ctclid'] = df['ctclid'].astype('float64')
    df_nnf['NNFID'] = df_nnf['NNFID'].astype('float64')
    # proceed only if all ctclid from notis file is present in nnf file or not
    missing_ctclid = set(df['ctclid'].unique()) - set(df_nnf['NNFID'].unique())
    if missing_ctclid:
        print(f"Missing ctclid(s) from NNF file: {missing_ctclid}")
        raise ValueError(f'The ctclid values are not matching the NNFID values - {missing_ctclid}')
    merged_df = pd.merge(df, df_nnf, left_on='ctclid', right_on='NNFID', how='left')
    merged_df.drop(columns=['NNFID'], axis=1, inplace=True)
    pbar.update(100)
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar.finish()
    merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']] = merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']].fillna('NONE')
    merged_df = merged_df.drop_duplicates()
    return merged_df

def main():
    today = datetime.now().date().strftime("%d%b%Y").upper()
    # today = datetime(year=2024, month=12, day=24).date().strftime("%d%b%Y").upper()
    df_db = read_data_db()
    # write_notis_postgredb(df_db, table_name=n_tbl_notis_raw_data, raw=True)
    modify_filepath = os.path.join(modified_dir, f'NOTIS_DATA_{today}.xlsx')
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    if not os.path.exists(nnf_file_path):
        raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")

    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    if readable_mod_time.date() == datetime.strptime(today, '%d%b%Y').date(): # Check if the NNF file is modified today or not
        print(f'New NNF Data found, modifying the nnf data in db . . .')
        df_nnf = pd.read_excel(nnf_file_path, index_col=False)
        df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
        df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
        df_nnf.dropna(how='all', inplace=True)
        df_nnf = df_nnf.drop_duplicates()
        write_notis_postgredb(df_nnf, n_tbl_notis_nnf_data, raw=False)
    else:
        df_nnf = read_data_db(nnf=True, for_table = n_tbl_notis_nnf_data)
        df_nnf = df_nnf.drop_duplicates()
    modified_df = modify_file(df_db, df_nnf)
    write_notis_postgredb(modified_df, table_name=n_tbl_notis_trade_book, raw=False)
    write_notis_data(modified_df, modify_filepath)
    write_notis_data(modified_df, rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\NOTIS_DATA_{today}.xlsx')
    print('file saved in modified_data folder')


if __name__ == '__main__':
    stt = time.time()
    root_dir = os.path.dirname(os.path.abspath(__file__))
    bhav_dir = os.path.join(root_dir, 'bhavcopy')
    modified_dir = os.path.join(root_dir, 'modified_data')
    table_dir = os.path.join(root_dir, 'table_data')
    eod_dir = os.path.join(root_dir, 'eod_data')
    dir_list = [bhav_dir, modified_dir, table_dir, eod_dir]
    status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]
    main()
    ett = time.time()
    print(f'total time taken for execution {ett-stt} seconds')