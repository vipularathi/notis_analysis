import re, os, progressbar, psycopg2, pyodbc, warnings, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine, text, insert

from db_config import engine_str,n_tbl_notis_trade_book,n_tbl_notis_raw_data,n_tbl_bse_trade_data,n_tbl_notis_eod_net_pos_cp_noncp,n_tbl_notis_desk_wise_net_position ,n_tbl_notis_nnf_data, n_tbl_notis_nnf_wise_net_position
from common import get_date_from_non_jiffy, get_date_from_jiffy, today, yesterday, root_dir, logger, read_data_db, write_notis_postgredb, truncate_tables
from nse_utility import NSEUtility
warnings.filterwarnings("ignore")

n_tbl_test_mod = n_tbl_notis_trade_book
n_tbl_test_raw = n_tbl_notis_raw_data
n_tbl_test_cp_noncp = n_tbl_notis_eod_net_pos_cp_noncp
n_tbl_test_net_pos_desk = n_tbl_notis_desk_wise_net_position
n_tbl_test_net_pos_nnf = n_tbl_notis_nnf_wise_net_position
n_tbl_test_bse = n_tbl_bse_trade_data
main_mod_df = pd.DataFrame()
main_mod_df_bse = pd.DataFrame()

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
    logger.info(f'pivot shape: {pivot_df.shape}')
    write_notis_postgredb(pivot_df, table_name=n_tbl_test_bse)
    # return pivot_df

def main(from_time, to_time):
    global main_mod_df
    logger.info(f'Notis trade data fetched from {from_time} to {to_time}')
    df_db = read_data_db(from_time=from_time,to_time=to_time)
    if df_db.empty:
        logger.info(f'No trade at {datetime.now().time()} hence skipping further processes')
        return
    write_notis_postgredb(df_db, table_name=n_tbl_test_raw, raw=True)
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
        write_notis_postgredb(df_nnf, table_name=n_tbl_notis_nnf_data, truncate_required=True)
    else:
        df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
    df_nnf = df_nnf.drop_duplicates()
    modified_df = NSEUtility.modify_file(df_db, df_nnf)
    write_notis_postgredb(modified_df, table_name=n_tbl_test_mod)
    logger.info(f'length of main_mod-df before concat is {len(main_mod_df)}')
    main_mod_df = pd.concat([main_mod_df, modified_df], ignore_index=True)
    logger.info(f'length of main_mod-df after concat is {len(main_mod_df)}')
    # main_mod_df['expDt'] = pd.to_datetime(main_mod_df['expDt']).dt.date
    # pivot_df = main_mod_df.pivot_table(
    #     index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
    #     columns=['bsFlg'],
    #     values=['trdQty', 'trdPrc'],
    #     aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
    #     fill_value=0
    # )
    # if len(main_mod_df.bsFlg.unique()) == 1:
    #     if main_mod_df.bsFlg.unique().tolist()[0] == 'B':
    #         pivot_df['SellAvgPrc']=0;pivot_df['SellQty']=0
    #     elif main_mod_df.bsFlg.unique().tolist()[0] == 'S':
    #         pivot_df['BuyAvgPrc']=0;pivot_df['BuyQty']=0
    # elif len(main_mod_df) == 0 or len(pivot_df) == 0:
    #     pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    # pivot_df.columns = ['BuyAvgPrc','SellAvgPrc','BuyQty','SellQty']
    # pivot_df.reset_index(inplace=True)
    main_mod_df['trdQtyPrc'] = main_mod_df['trdQty'] * main_mod_df['trdPrc']
    pivot_df = main_mod_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdQtyPrc'],
        aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum'},
        fill_value=0
    )
    if len(main_mod_df.bsFlg.unique()) == 1:
        if main_mod_df.bsFlg.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = 0;
            pivot_df['SellQty'] = 0
        elif main_mod_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = 0;
            pivot_df['BuyQty'] = 0
    elif len(main_mod_df) == 0 or len(pivot_df) == 0:
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
    cp_noncp_df = NSEUtility.calc_eod_cp_noncp(pivot_df)
    write_notis_postgredb(cp_noncp_df, table_name=n_tbl_test_cp_noncp, truncate_required=True)
    desk_db_df = NSEUtility.calc_deskwise_net_pos(pivot_df)
    write_notis_postgredb(desk_db_df, table_name=n_tbl_test_net_pos_desk, truncate_required=True)
    nnf_db_df = NSEUtility.calc_nnfwise_net_pos(pivot_df)
    write_notis_postgredb(nnf_db_df, table_name=n_tbl_test_net_pos_nnf, truncate_required=True)

if __name__ == '__main__':
    recover = False
    stt = datetime.now().replace(hour=9, minute=15)
    ett = datetime.now().replace(hour=15, minute=35)
    # backtest_date = (datetime.now()-timedelta(days=1))
    # stt = backtest_date.replace(hour=9, minute=15)
    # ett = backtest_date.replace(hour=15, minute=35)
    actual_start_time = datetime.now()
    logger.info(f'test started at {datetime.now()}')
    if actual_start_time > stt and actual_start_time < ett:
        recover = True
    while datetime.now() < stt:
        time.sleep(1)
    while datetime.now() < ett:
        now = datetime.now()
        # now=backtest_date.replace(hour=now.hour, minute=now.minute, second=now.second, microsecond=0)
        # next_min = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        # logger.info('\n')
        # main(from_time=now.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], to_time=next_min.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        # time.sleep((next_min - now).total_seconds() + 30)
        # if now.second == 1:
        #     # now=now.replace(day=28)
        #     print('in if')
        #     logger.info('\n')
        #     logger.info(f"now time => {now.strftime('%Y-%m-%d %H:%M:%S')}")
        #     main((now - timedelta(minutes=1)).replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        #                   now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        #     get_bse_trade_data((now - timedelta(minutes=1)).replace(second=0).strftime('%d-%b-%Y %H:%M:%S'), now.replace(
        #         second=0).strftime('%d-%b-%Y %H:%M:%S'))
        # # else:
        # #     print('in else')
        # #     next_time = (now + timedelta(minutes=1)).replace(second=1, microsecond=0)
        # #     time.sleep((next_time - now).total_seconds())
        if now.second == 1:
            print('in if')
            if recover:
                print('in recover')
                table_list = [n_tbl_test_mod, n_tbl_test_raw, n_tbl_test_cp_noncp, n_tbl_test_net_pos_desk,
                              n_tbl_test_net_pos_nnf, n_tbl_test_bse]
                for each in table_list:
                    truncate_tables(each)
                main_from_time = stt.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                main_to_time = now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                bse_from_time = stt.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                bse_to_time = now.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                recover = False
            else:
                main_from_time = (now - timedelta(minutes=1)).replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                main_to_time = now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                bse_from_time = (now - timedelta(minutes=1)).replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                bse_to_time = now.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
            logger.info(f"\nnow time => {now.strftime('%Y-%m-%d %H:%M:%S')}")
            main(main_from_time, main_to_time)
            get_bse_trade_data(bse_from_time, bse_to_time)
            time.sleep(1)