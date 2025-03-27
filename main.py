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
from common import read_data_db, read_notis_file, write_notis_data, write_notis_postgredb, get_date_from_non_jiffy, get_date_from_jiffy, today, yesterday, root_dir, bhav_dir, modified_dir, table_dir, eod_dir, download_bhavcopy
from net_position_cp_noncp import calc_eod_cp_noncp
from bse_utility import get_bse_trade

warnings.filterwarnings('ignore', message="pandas only supports SQLAlchemy connectable.*")
pd.set_option('display.float_format', lambda a:'%.2f' %a)

def modify_file(df, df_nnf):
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
    df.ordTm = df.ordTm.astype('datetime64[ns]')
    df.ordTm = df.ordTm.dt.date
    df.expDt = df.expDt.astype('datetime64[ns]')
    df.expDt = df.expDt.dt.date
    df.trdTm = df.trdTm.astype('datetime64[ns]')
    df.trdTm = df.trdTm.dt.date
    # --------------------------------------------------------------------------------------------------------------------------------
    df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
    pbar.update(90)

    df['remarks'] = df['cpCD'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')
    df.rename(columns={'remarks': 'broker'}, inplace=True)
    pbar.update(95)

    df['ctclid'] = df['ctclid'].astype('float64')
    df_nnf['NNFID'] = df_nnf['NNFID'].astype('float64')
    # proceed only if all ctclid from notis file is present in nnf file or not
    missing_ctclid = set(df['ctclid'].unique()) - set(df_nnf['NNFID'].unique())
    if missing_ctclid:
        print(f"\nMissing ctclid(s) from NNF file: {missing_ctclid}")
        raise ValueError(f'The ctclid values are not matching the NNFID values - {missing_ctclid}')
    else:
        print('\nAll ctclid values are present in NNF file.\n')
    merged_df = pd.merge(df, df_nnf, left_on='ctclid', right_on='NNFID', how='left')
    merged_df.drop(columns=['NNFID'], axis=1, inplace=True)
    pbar.update(100)
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar.finish()
    merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']] = merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']].fillna('NONE')
    merged_df = merged_df.drop_duplicates()
    return merged_df

def download_tables():
    table_list = ['NOTIS_DESK_WISE_NET_POSITION', 'NOTIS_NNF_WISE_NET_POSITION', 'NOTIS_USERID_WISE_NET_POSITION']
    # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
    for table in table_list:
        df = read_data_db(for_table=table)
        df.to_csv(os.path.join(table_dir, f"{table}_{today.strftime('%Y-%m-%d').upper()}.csv"), index=False)
        print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}_{today}.csv')}")

def truncate_tables():
    table_name = ["notis_raw_data","NOTIS_TRADE_BOOK","NOTIS_DESK_WISE_NET_POSITION","NOTIS_NNF_WISE_NET_POSITION","NOTIS_USERID_WISE_NET_POSITION",f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}']
    engine = create_engine(engine_str)
    # with engine.connect() as conn:
    with engine.begin() as conn:
        for each in table_name:
            res = conn.execute(text(f'select count(*) from "{each}"'))
            row_count = res.scalar()
            if row_count > 0:
                # conn.execute(text(f'delete from "{each}"'))
                conn.execute(text(f'truncate table "{each}"'))
                print(f'Existing data from table {each} deleted')
            else:
                print(f'No data in table {each}, no need to delete')
def main():
    truncate_tables()
    # today = datetime(year=2025, month=3, day=7).date()
    df_db = read_data_db()
    # df_db = read_data_db(for_table=f'notis_raw_data_{today.strftime("%Y-%m-%d")}')
    write_notis_postgredb(df_db, table_name=n_tbl_notis_raw_data, raw=True)
    modify_filepath = os.path.join(modified_dir, f'NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    if not os.path.exists(nnf_file_path):
        raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")

    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    if readable_mod_time.date() == today: # Check if the NNF file is modified today or not
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
    write_notis_data(modified_df, rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    print('file saved in modified_data folder')
    download_bhavcopy()
    print(f'Today\'s bhavcopy downloaded and stored at {bhav_dir}')
    print(f'Updating cp-noncp eod table...')
    stt = datetime.now()
    calc_eod_cp_noncp()
    ett = datetime.now()
    print(f'Eod(cp-noncp) updation completed. Total time taken: {(ett - stt).seconds} seconds')

if __name__ == '__main__':
    stt = time.time()

    main()
    ett = time.time()
    print(f'total time taken for modifying, adding data in db and writing in local directory - {ett - stt} seconds')
    pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
    pbar.update(1)
    for i in range(100):
        time.sleep(1)
        pbar.update(i + 1)
    pbar.finish()
    download_tables()
    print(f'fetching BSE trades...')
    stt = datetime.now()
    get_bse_trade()
    ett = datetime.now()
    print(f'BSE trade fetched. Total time taken: {(ett - stt).seconds} seconds')