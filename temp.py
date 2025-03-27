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
from db_config import engine_str, n_tbl_notis_nnf_data
from common import get_date_from_non_jiffy, get_date_from_jiffy, today, yesterday, root_dir, logger, read_data_db

warnings.filterwarnings("ignore")

n_tbl_test_mod = f"test_mod_{today}"
n_tbl_test_raw = f'test_raw_{today}'
n_tbl_test_cp_noncp = f'test_cp_noncp_{today}'
n_tbl_test_net_pos_desk = f'test_net_pos_desk_{today}'
n_tbl_test_net_pos_nnf = f'test_net_pos_nnf_{today}'
n_tbl_test_bse = f'test_bse_{today}'
main_mod_df = pd.DataFrame()
main_mod_df_bse = pd.DataFrame()
def modify_file1(df, df_nnf):
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
    logger.info('Starting file modification...')
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
        logger.info(f"\nMissing ctclid(s) from NNF file: {missing_ctclid}")
        raise ValueError(f'The ctclid values are not matching the NNFID values - {missing_ctclid}')
    else:
        logger.info('\nAll ctclid values are present in NNF file.\n')
    merged_df = pd.merge(df, df_nnf, left_on='ctclid', right_on='NNFID', how='left')
    merged_df.drop(columns=['NNFID'], axis=1, inplace=True)
    pbar.update(100)
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar.finish()
    merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']] = merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']].fillna('NONE')
    merged_df = merged_df.drop_duplicates()
    return merged_df

def calc_eod_cp_noncp1(desk_db_df):
    # ================================================================
    # eod_pattern = rf"EOD Position[ _]{yesterday.day}[-_]{yesterday.strftime('%b').capitalize()}[-_]{yesterday.year}_CP.(xlsx|csv)" #sample=EOD Position 28-Jan-2025_CP or EOD Position_11_Mar_2025_CP
    # eod_matched_files = [os.path.join(eod_input_dir,f) for f in os.listdir(eod_input_dir) if re.match(eod_pattern,f)]
    # # eod_df = read_file(os.path.join(test_dir,eod_matched_files[0]))
    # eod_df = read_file(eod_matched_files[0])
    # eod_df.columns = eod_df.columns.str.replace(' ', '')
    # eod_df.drop(columns=[col for col in eod_df.columns if col is None], inplace=True)
    # eod_df = eod_df.add_prefix('Eod')
    # eod_df.EodExpiry = eod_df.EodExpiry.astype('datetime64[ns]')
    # eod_df.EodExpiry = eod_df.EodExpiry.dt.date
    # eod_df.loc[eod_df['EodOptionType'] == 'XX', 'EodStrike'] = 0
    # eod_df.rename(columns={'EodSettlementPrice':'EodBroker'}, inplace=True)
    # # eod_df.EodClosingPrice = eod_df.EodClosingPrice * 100
    #
    # grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum'})
    # grouped_eod = grouped_eod.drop_duplicates()
    # --------------------------------------------------------------------
    eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}' #NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
    eod_df = read_data_db(for_table=eod_tablename)
    eod_df.columns = [re.sub(r'Eod|\s','',each) for each in eod_df.columns]
    # Underlying	Strike	Option Type	Expiry	Net Quantity	Settlement Price
    eod_df.drop(columns=['NetQuantity','buyQty','buyAvgPrice','sellQty','sellAvgPrice','IntradayVolume','ClosingPrice'], inplace=True)
    eod_df.rename(columns={'FinalNetQty':'NetQuantity','FinalSettlementPrice':'ClosingPrice'}, inplace=True)
    eod_df = eod_df.add_prefix('Eod')
    eod_df.EodExpiry = eod_df.EodExpiry.astype('datetime64[ns]')
    eod_df.EodExpiry = eod_df.EodExpiry.dt.date
    eod_df = eod_df.query("EodExpiry >= @today and EodNetQuantity != 0")

    grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum','EodClosingPrice':'mean'})
    grouped_eod = grouped_eod.drop_duplicates()
    # ================================================================
    # # tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
    # tablenam = f'NOTIS_DESK_WISE_NET_POSITION'
    # desk_db_df = read_data_db(for_table=tablenam)
    # desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
    # desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
    desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
    desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
    desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
    # desk_db_df['broker'] = desk_db_df['brokerID'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')

    grouped_desk_db_df = desk_db_df.groupby(by=['broker','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
    grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
    grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'}, inplace=True)
    # ================================================================
    merged_df = grouped_eod.merge(grouped_desk_db_df, left_on=['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], right_on=["broker","symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
    merged_df.fillna(0, inplace=True)
    merged_df = merged_df.drop_duplicates()

    coltd1 = ['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
    coltd2 = ["broker","symbol", "expiryDate", "strikePrice", "optionType"]
    for i in range(len(coltd1)):
        merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
        merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
    merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
    merged_df.drop(columns = ['broker','symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace = True)

    # if datetime.strptime('16:00:00', '%H:%M:%S').time() < datetime.now().time():
    #     bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
    #     bhav_matched_files = [f for f in os.listdir(bhav_path) if re.match(bhav_pattern, f)]
    #     bhav_df = read_file(os.path.join(bhav_path, bhav_matched_files[0])) # regularBhavcopy_14012025.xlsx
    #     bhav_df.columns = bhav_df.columns.str.replace(' ', '')
    #     bhav_df.rename(columns={'VWAPclose':'closingPrice'}, inplace=True)
    #     bhav_df.columns = bhav_df.columns.str.capitalize()
    #     bhav_df = bhav_df.add_prefix('Bhav')
    #     bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
    #     bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
    #     bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
    #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
    #     col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
    #     bhav_df = bhav_df[col_keep]
    #     bhav_df = bhav_df.drop_duplicates()
    #
    #     merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
    #     merged_bhav_df.drop(columns = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace = True)
    # else:
    merged_bhav_df = merged_df.copy()
    merged_bhav_df['BhavClosingprice'] = 0

    merged_bhav_df.fillna(0,inplace=True)
    merged_bhav_df.buyAvgPrice = merged_bhav_df.buyAvgPrice.astype('int64')
    merged_bhav_df.sellAvgPrice = merged_bhav_df.sellAvgPrice.astype('int64')
    merged_bhav_df.BhavClosingprice = merged_bhav_df.BhavClosingprice.astype('int64')
    merged_bhav_df.EodExpiry = merged_bhav_df.EodExpiry.astype('str')
    merged_bhav_df.rename(columns = {'BhavClosingprice':'FinalSettlementPrice'}, inplace = True)
    logger.info(f'cp noncp length at {datetime.now()} is {merged_bhav_df.shape}')
    return merged_bhav_df

def calc_deskwise_net_pos(pivot_df):
    pivot_df.rename(columns ={'MainGroup':'mainGroup','SubGroup':'subGroup'}, inplace=True)
    desk_db_df = pivot_df.groupby(by=['mainGroup', 'subGroup', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
    return desk_db_df

def calc_nnfwise_net_pos(pivot_df):
    nnf_db_df = pivot_df.groupby(by=['ctclid', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
    nnf_db_df.rename(columns={'ctclid':'nnfID'}, inplace=True)
    return nnf_db_df

# def read_data_db(nnf=False, for_table='ENetMIS', from_time:str='', to_time:str=''):
#     if not nnf and for_table == 'ENetMIS':
#         logger.info(f'fetching raw data from {from_time} to {to_time}')
#         # Sql connection parameters
#         sql_server = "rms.ar.db"
#         sql_database = "ENetMIS"
#         sql_username = "notice_user"
#         sql_password = "Notice@2024"
#         # sql_query = "SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]"
#         sql_query = f"SELECT * FROM [ENetMIS].[dbo].[NSE_FO_AA100_view] WHERE CreateDate BETWEEN '{from_time}' AND '{to_time}';"
#
#         try:
#             sql_connection_string = (
#                 f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#                 f"SERVER={sql_server};"
#                 f"DATABASE={sql_database};"
#                 f"UID={sql_username};"
#                 f"PWD={sql_password}"
#             )
#             with pyodbc.connect(sql_connection_string) as sql_conn:
#                 df = pd.read_sql_query(sql_query, sql_conn)
#             logger.info(f"Data fetched from SQL Server. Shape:{df.shape}")
#             return df
#
#         except (pyodbc.Error, psycopg2.Error) as e:
#             logger.info("Error occurred:", e)
#
#     elif not nnf and for_table == 'TradeHist':
#         logger.info(f'fetching BSE trade data from {from_time} to {to_time}')
#         sql_server = '172.30.100.41'
#         sql_port = '1450'
#         sql_db = 'OMNE_ARD_PRD'
#         sql_userid = 'Pos_User'
#         sql_paswd = 'Pass@Word1'
#         sql_query = (
#             f"select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from TradeHist where (mnmSymbolName = 'BSXOPT' or mnmSymbolName = 'BSE') and mnmExchangeTime between \'{from_time}\' and \'{to_time}\'")
#         try:
#             sql_engine_str = (
#                 f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#                 f"SERVER={sql_server},{sql_port};"
#                 f"DATABASE={sql_db};"
#                 f"UID={sql_userid};"
#                 f"PWD={sql_paswd};"
#             )
#             with pyodbc.connect(sql_engine_str) as sql_conn:
#                 df_bse = pd.read_sql_query(sql_query, sql_conn)
#             logger.info(f'data fetched for bse: {df_bse.shape}')
#             return df_bse
#         except (pyodbc.Error, psycopg2.Error) as e:
#             logger.info(f'Error in fetching data: {e}')
#
#     elif nnf and for_table != 'ENetMIS':
#         engine = create_engine(engine_str)
#         df = pd.read_sql_table(n_tbl_notis_nnf_data, con=engine)
#         logger.info(f"Data fetched from {for_table} table. Shape:{df.shape}")
#         return df
#
#     elif not nnf and for_table!='ENetMIS':
#         engine = create_engine(engine_str)
#         df = pd.read_sql_table(for_table, con=engine)
#         logger.info(f"Data fetched from {for_table} table. Shape:{df.shape}")
#         return df

def write_notis_postgredb1(df, table_name, raw=False, net_pos_table=False):
    start_time = time.time()
    engine = create_engine(engine_str)

    if net_pos_table:
        with engine.begin() as conn:
            res = conn.execute(text(f'select count(*) from "{table_name}"'))
            row_count = res.scalar()
            if row_count > 0:
                conn.execute(text(f'delete from "{table_name}"'))
                logger.info(f'Existing data from table {table_name} deleted')
            else:
                logger.info(f'No existing data in table {table_name}')
    logger.info(f'Writing {"Raw" if raw else "Modified"} data to database...')
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
    logger.info(f'{"Raw" if raw else "Modified"} Data successfully inserted into database')
    end_time = time.time()
    logger.info(f'Total time taken: {end_time - start_time} seconds')

def get_bse_trade_data(from_time, to_time):
    global main_mod_df_bse
    df_bse = read_data_db(for_table='TradeHist', from_time=from_time, to_time=to_time)
    df_bse = df_bse.query("mnmTransactionType != 'L'")
    if df_bse.empty:
        print(f'No data for {from_time} hence skipping')
        return
    df_bse.replace('', 0, inplace=True)
    # df_bse = read_file(os.path.join(bse_dir,'test_bse172025_1.xlsx'))
    df_bse.columns = [re.sub(r'mnm|\s', '', each) for each in df_bse.columns]
    # df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x:pd.to_datetime(int(x), unit='s').date() if x !='' else x)
    df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x: pd.to_datetime(int(x), unit='s').date())
    # df_bse.replace('', 0, inplace=True)
    to_int_list = ['FillPrice', 'FillSize', 'StrikePrice']
    for each in to_int_list:
        df_bse[each] = df_bse[each].astype(np.int64)
    df_bse['AvgPrice'] = df_bse['AvgPrice'].astype(float).astype(np.int64)
    df_bse['StrikePrice'] = (df_bse['StrikePrice'] / 100).astype(np.int64)
    df_bse['Symbol'] = df_bse['TradingSymbol'].apply(lambda x: 'SENSEX' if x.upper().startswith('SEN') else x)
    df_bse.rename(columns={'User': 'TerminalID'}, inplace=True)
    pivot_df = df_bse.pivot_table(
        index=['TerminalID', 'Symbol', 'TradingSymbol', 'ExpiryDate', 'OptionType', 'StrikePrice', 'ExecutingBroker'],
        columns=['TransactionType'],
        values=['FillSize', 'AvgPrice'],
        aggfunc={'FillSize': 'sum', 'AvgPrice': 'mean'},
        fill_value=0
    )
    if len(df_bse.TransactionType.unique()) == 1:
        if df_bse.TransactionType.unique().tolist()[0] == 'B':
            pivot_df['SellAvgPrc'] = 0;
            pivot_df['SellQty'] = 0
        elif df_bse.TransactionType.unique().tolist()[0] == 'S':
            pivot_df['BuyAvgPrc'] = 0;
            pivot_df['BuyQty'] = 0
    elif len(df_bse) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    pivot_df.reset_index(inplace=True)
    pivot_df['BSEIntradayVol'] = pivot_df.BuyVol - pivot_df.SellVol
    pivot_df.ExpiryDate = pivot_df.ExpiryDate.astype(str)
    pivot_df['ExpiryDate'] = [re.sub(r'1970.*', '', each) for each in pivot_df['ExpiryDate']]
    to_int_list = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    for col in to_int_list:
        pivot_df[col] = pivot_df[col].astype(np.int64)
    print(f'pivot shape: {pivot_df.shape}')
    return pivot_df


def main(from_time, to_time):
    global main_mod_df
    logger.info(f'trade data fetched for {datetime.now().time()}')
    # today = datetime(year=2025, month=3, day=7).date()
    df_db = read_data_db(from_time=from_time,to_time=to_time)
    if df_db.empty:
        logger.info(f'No trade at {datetime.now().time()} hence skipping further processes')
        return
    write_notis_postgredb1(df_db, table_name=n_tbl_test_raw, raw=True)
    # df_db = read_data_db(for_table=f'notis_raw_data_{today.strftime("%Y-%m-%d")}')
    # write_notis_postgredb(df_db, table_name=n_tbl_notis_raw_data, raw=True)
    # modify_filepath = os.path.join(modified_dir, f'NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    if readable_mod_time.date() == today: # Check if the NNF file is modified today or not
        logger.info(f'New NNF Data found, modifying the nnf data in db . . .')
        df_nnf = pd.read_excel(nnf_file_path, index_col=False)
        df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
        df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
        df_nnf.dropna(how='all', inplace=True)
        df_nnf = df_nnf.drop_duplicates()
        write_notis_postgredb1(df_nnf, n_tbl_notis_nnf_data)
    else:
        df_nnf = read_data_db(nnf=True, for_table = n_tbl_notis_nnf_data)
    df_nnf = df_nnf.drop_duplicates()
    modified_df = modify_file1(df_db, df_nnf)
    write_notis_postgredb1(modified_df, table_name=n_tbl_test_mod)
    logger.info(f'length of main_mod-df before concat is {len(main_mod_df)}')
    main_mod_df = pd.concat([main_mod_df, modified_df], ignore_index=True)
    logger.info(f'length of main_mod-df after concat is {len(main_mod_df)}')
    main_mod_df['expDt'] = pd.to_datetime(main_mod_df['expDt']).dt.date
    pivot_df = main_mod_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdPrc'],
        aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
        fill_value=0
    )
    # pivot_df.columns = ['BuyAvgPrc', 'SellAvgPrc', 'BuyQty', 'SellQty']
    if len(main_mod_df.bsFlg.unique()) == 1:
        if main_mod_df.bsFlg.unique().tolist()[0] == 'B':
            pivot_df['SellAvgPrc']=0;pivot_df['SellQty']=0
        elif main_mod_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyAvgPrc']=0;pivot_df['BuyQty']=0
    elif len(main_mod_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    pivot_df.columns = ['BuyAvgPrc','SellAvgPrc','BuyQty','SellQty']
    pivot_df.reset_index(inplace=True)
    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice',
                             'SellAvgPrc': 'sellAvgPrice', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    cp_noncp_df = calc_eod_cp_noncp1(pivot_df)
    write_notis_postgredb1(cp_noncp_df,table_name=n_tbl_test_cp_noncp, net_pos_table=True)
    desk_db_df = calc_deskwise_net_pos(pivot_df)
    write_notis_postgredb1(desk_db_df, table_name=n_tbl_test_net_pos_desk, net_pos_table=True)
    nnf_db_df = calc_nnfwise_net_pos(pivot_df)
    write_notis_postgredb1(nnf_db_df, table_name=n_tbl_test_net_pos_nnf, net_pos_table=True)
    # df_db_bse = get_bse_trade_data(from_time=from_time[:-4],to_time=to_time[:-4])
    # write_notis_postgredb1(df_db_bse, table_name=n_tbl_test_bse)

if __name__ == '__main__':
    stt = datetime.now().replace(hour=9, minute=15)
    ett = datetime.now().replace(hour=15, minute=35)
    logger.info(f'test started at {datetime.now()}')
    while datetime.now() < stt:
        time.sleep(1)
    while datetime.now() < ett:
        now = datetime.now()
        # next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        # logger.info('\n')
        # main(from_time=now.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], to_time=next_min.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        # time.sleep((next_min - now).total_seconds() + 30)
        if now.second == 1:
            print('\n')
            print('now time => ', now.strftime('%Y-%m-%d %H:%M:%S'))
            main((now - timedelta(minutes=1)).replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                          now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
            time.sleep(1)
        else:
            next_time = (now + timedelta(minutes=1)).replace(second=1, microsecond=0)
            time.sleep((next_time - now).total_seconds())