import re, os, progressbar, pyodbc, warnings, psycopg2, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine, text, insert

from db_config import n_tbl_notis_trade_book, n_tbl_notis_raw_data, n_tbl_notis_nnf_data, n_tbl_notis_desk_wise_net_position, n_tbl_notis_nnf_wise_net_position, n_tbl_notis_eod_net_pos_cp_noncp, n_tbl_bse_trade_data
from common import read_data_db, read_notis_file, write_notis_data, write_notis_postgredb, get_date_from_non_jiffy, get_date_from_jiffy, today, yesterday, root_dir, bhav_dir, modified_dir, table_dir, download_bhavcopy, bse_dir
from nse_utility import NSEUtility
from bse_utility import BSEUtility


warnings.filterwarnings('ignore', message="pandas only supports SQLAlchemy connectable.*")
pd.set_option('display.float_format', lambda a:'%.2f' %a)

# def modify_file(df, df_nnf):
#     list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
#     list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
#     list_none_str = [38]
#     for i in list_str_int64:
#         # df_db.loc[:, f'Column{i}'] = df_db.loc[:, f'Column{i}'].astype('int64')
#         column_name = f'Column{i}'
#         df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
#     for i in list_str_none:
#         df[f'Column{i}'] = None
#     for i in list_none_str:
#         df[f'Column{i}'] = df[f'Column{i}'].astype('str')
#     # --------------------------------------------------------------------------------------------------------------------------------
#     pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
#     print('Starting file modification...')
#     pbar.update(0)
#     df.rename(columns={
#         'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
#         'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
#         'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
#         'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
#         'Column13': 'cliActNo', 'Column14': 'cpCD', 'Column15': 'remarks',
#         'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
#         'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
#         'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
#         'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
#         'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
#         'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
#         'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
#     }, inplace=True)
#     pbar.update(20)
#
#     df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
#     pbar.update(40)
#
#     df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
#     pbar.update(60)
#
#     df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
#     pbar.update(80)
#     df.ordTm = df.ordTm.astype('datetime64[ns]')
#     df.ordTm = df.ordTm.dt.date
#     df.expDt = df.expDt.astype('datetime64[ns]')
#     df.expDt = df.expDt.dt.date
#     df.trdTm = df.trdTm.astype('datetime64[ns]')
#     df.trdTm = df.trdTm.dt.date
#     # --------------------------------------------------------------------------------------------------------------------------------
#     df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
#     pbar.update(90)
#
#     df['remarks'] = df['cpCD'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')
#     df.rename(columns={'remarks': 'broker'}, inplace=True)
#     pbar.update(95)
#
#     df['ctclid'] = df['ctclid'].astype('float64')
#     df_nnf['NNFID'] = df_nnf['NNFID'].astype('float64')
#     # proceed only if all ctclid from notis file is present in nnf file or not
#     missing_ctclid = set(df['ctclid'].unique()) - set(df_nnf['NNFID'].unique())
#     if missing_ctclid:
#         print(f"\nMissing ctclid(s) from NNF file: {missing_ctclid}")
#         raise ValueError(f'The ctclid values are not matching the NNFID values - {missing_ctclid}')
#     else:
#         print('\nAll ctclid values are present in NNF file.\n')
#     merged_df = pd.merge(df, df_nnf, left_on='ctclid', right_on='NNFID', how='left')
#     merged_df.drop(columns=['NNFID'], axis=1, inplace=True)
#     pbar.update(100)
#     # --------------------------------------------------------------------------------------------------------------------------------
#     pbar.finish()
#     merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']] = merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']].fillna('NONE')
#     merged_df = merged_df.drop_duplicates()
#     return merged_df

def download_tables():
    table_list = [n_tbl_notis_desk_wise_net_position, n_tbl_notis_nnf_wise_net_position,n_tbl_notis_eod_net_pos_cp_noncp]
    # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
    for table in table_list:
        df = read_data_db(for_table=table)
        # df.to_csv(os.path.join(table_dir, f"{table}.xlsx"), index=False)
        write_notis_data(df=df, filepath=os.path.join(table_dir,f'{table}.xlsx'))
        print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}.xlsx')}")

# def truncate_tables():
#     table_name = ["notis_raw_data","NOTIS_TRADE_BOOK","NOTIS_DESK_WISE_NET_POSITION","NOTIS_NNF_WISE_NET_POSITION","NOTIS_USERID_WISE_NET_POSITION",f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}']
#     engine = create_engine(engine_str)
#     # with engine.connect() as conn:
#     with engine.begin() as conn:
#         for each in table_name:
#             res = conn.execute(text(f'select count(*) from "{each}"'))
#             row_count = res.scalar()
#             if row_count > 0:
#                 # conn.execute(text(f'delete from "{each}"'))
#                 conn.execute(text(f'truncate table "{each}"'))
#                 print(f'Existing data from table {each} deleted')
#             else:
#                 print(f'No data in table {each}, no need to delete')
def main():
    # truncate_tables()
    # today = datetime(year=2025, month=3, day=7).date()
    df_db = read_data_db()
    write_notis_postgredb(df=df_db, table_name=n_tbl_notis_raw_data, raw=True, truncate_required=True)
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
        write_notis_postgredb(df=df_nnf, table_name=n_tbl_notis_nnf_data, truncate_required=True)
    else:
        df_nnf = read_data_db(nnf=True, for_table = n_tbl_notis_nnf_data)
        df_nnf = df_nnf.drop_duplicates()
    modified_df = NSEUtility.modify_file(df_db, df_nnf)
    write_notis_postgredb(modified_df, table_name=n_tbl_notis_trade_book, truncate_required=True)
    write_notis_data(modified_df, modify_filepath)
    write_notis_data(modified_df, rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    print('file saved in modified_data folder')
    # # make net-pos tables
    # pivot_df = modified_df.pivot_table(
    #     index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
    #     columns=['bsFlg'],
    #     values=['trdQty', 'trdPrc'],
    #     aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
    #     fill_value=0
    # )
    # if len(modified_df.bsFlg.unique()) == 1:
    #     if modified_df.bsFlg.unique().tolist()[0] == 'B':
    #         pivot_df['SellAvgPrc'] = 0;
    #         pivot_df['SellQty'] = 0
    #     elif modified_df.bsFlg.unique().tolist()[0] == 'S':
    #         pivot_df['BuyAvgPrc'] = 0;
    #         pivot_df['BuyQty'] = 0
    # elif len(modified_df) == 0 or len(pivot_df) == 0:
    #     pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    # pivot_df.columns = ['BuyAvgPrc', 'SellAvgPrc', 'BuyQty', 'SellQty']
    # pivot_df.reset_index(inplace=True)
    modified_df['trdQtyPrc'] = modified_df['trdQty'] * modified_df['trdPrc']
    pivot_df = modified_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdQtyPrc'],
        aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum'},
        fill_value=0
    )
    if len(modified_df.bsFlg.unique()) == 1:
        if modified_df.bsFlg.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = 0;
            pivot_df['SellQty'] = 0
        elif modified_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = 0;
            pivot_df['BuyQty'] = 0
    elif len(modified_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    pivot_df.columns = ['BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
    pivot_df['BuyAvgPrc'] = pivot_df.apply(lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0,
                                           axis=1)
    pivot_df['SellAvgPrc'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0, axis=1)
    pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df.reset_index(inplace=True)
    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice',
                             'SellAvgPrc': 'sellAvgPrice', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    # DESK
    dsk_db_df = NSEUtility.calc_deskwise_net_pos(pivot_df)
    write_notis_postgredb(dsk_db_df, table_name=n_tbl_notis_desk_wise_net_position, truncate_required=True)
    # NNF
    nnf_db_df = NSEUtility.calc_nnfwise_net_pos(pivot_df)
    write_notis_postgredb(nnf_db_df, table_name=n_tbl_notis_nnf_wise_net_position, truncate_required=True)
    # CP NONCP
    cp_noncp_db_df = NSEUtility.calc_eod_cp_noncp(pivot_df)
    write_notis_postgredb(cp_noncp_db_df, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)

if __name__ == '__main__':
    download_bhavcopy()
    print(f'Today\'s bhavcopy downloaded and stored at {bhav_dir}')
    stt = time.time()
    main()
    ett = time.time()
    print(f'total time taken for modifying, adding data in db and writing in local directory - {ett - stt} seconds')
    print(f'fetching BSE trades...')
    stt = datetime.now()
    df_bse = BSEUtility.get_bse_trade_data()
    write_notis_data(df_bse, os.path.join(bse_dir, f'BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx'))
    write_notis_postgredb(df=df_bse, table_name=n_tbl_bse_trade_data, truncate_required=True)
    ett = datetime.now()
    print(f'BSE trade fetched. Total time taken: {(ett - stt).seconds} seconds')
    pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
    pbar.update(1)
    for i in range(100):
        time.sleep(1)
        pbar.update(i + 1)
    pbar.finish()
    download_tables()
