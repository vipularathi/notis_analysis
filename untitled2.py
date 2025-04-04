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

n_tbl_test_mod = 'test_mod'
n_tbl_test_raw = 'test'
n_tbl_test_cp_noncp = 'test'
n_tbl_test_net_pos_desk = 'test'
n_tbl_test_net_pos_nnf = 'test'
n_tbl_test_bse = 'test'
main_mod_df = pd.DataFrame()
main_mod_df_bse = pd.DataFrame()


def calc_eod_cp_noncp(desk_db_df):
    eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}'  # NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
    eod_df = read_data_db(for_table=eod_tablename)
    eod_df.columns = [re.sub(r'Eod|\s', '', each) for each in eod_df.columns]
    # Underlying	Strike	Option Type	Expiry	Net Quantity	Settlement Price
    eod_df.drop(
        columns=['NetQuantity', 'buyQty', 'buyAvgPrice', 'sellQty', 'sellAvgPrice', 'IntradayVolume', 'ClosingPrice'],
        inplace=True)
    eod_df.rename(columns={'FinalNetQty': 'NetQuantity', 'FinalSettlementPrice': 'ClosingPrice'}, inplace=True)
    eod_df = eod_df.add_prefix('Eod')
    eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True, format='mixed')
    eod_df['EodExpiry'] = eod_df['EodExpiry'].dt.date
    eod_df = eod_df.query("EodExpiry >= @today and EodNetQuantity != 0")

    grouped_eod = eod_df.groupby(by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                 as_index=False).agg({'EodNetQuantity': 'sum', 'EodClosingPrice': 'mean'})
    grouped_eod = grouped_eod.query("EodNetQuantity != 0")
    grouped_eod = grouped_eod.drop_duplicates()

    desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
    desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
    desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
    desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
    desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
    # desk_db_df['broker'] = desk_db_df['brokerID'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')

    grouped_desk_db_df = desk_db_df.groupby(by=['broker', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
        {'buyAvgQty': 'sum', 'buyAvgPrice': 'mean', 'sellAvgQty': 'sum', 'sellAvgPrice': 'mean'}).reset_index()
    grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
    grouped_desk_db_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'}, inplace=True)
    # ================================================================
    merged_df = grouped_eod.merge(grouped_desk_db_df,
                                  left_on=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                  right_on=["broker", "symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
    merged_df.fillna(0, inplace=True)
    merged_df = merged_df.drop_duplicates()

    coltd1 = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
    coltd2 = ["broker", "symbol", "expiryDate", "strikePrice", "optionType"]
    for i in range(len(coltd1)):
        merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
        merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
    merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
    merged_df.drop(columns=['broker', 'symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace=True)

    # if datetime.strptime('16:00:00', '%H:%M:%S').time() < datetime.now().time():
    #     bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
    #     bhav_matched_files = [f for f in os.listdir(bhav_dir) if re.match(bhav_pattern, f)]
    #     bhav_df = read_file(os.path.join(bhav_dir, bhav_matched_files[0]))  # regularBhavcopy_14012025.xlsx
    #     bhav_df.columns = bhav_df.columns.str.replace(' ', '')
    #     bhav_df.rename(columns={'VWAPclose': 'closingPrice'}, inplace=True)
    #     bhav_df.columns = bhav_df.columns.str.capitalize()
    #     bhav_df = bhav_df.add_prefix('Bhav')
    #     bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
    #     bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
    #     bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
    #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x / 100 if x > 0 else x)
    #     col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype', 'BhavClosingprice']
    #     bhav_df = bhav_df[col_keep]
    #     bhav_df = bhav_df.drop_duplicates()
    #
    #     merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"],
    #                                      right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'],
    #                                      how='left')
    #     merged_bhav_df.drop(columns=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace=True)
    # else:
    #     merged_bhav_df = merged_df.copy()
    #     merged_bhav_df['BhavClosingprice'] = 0
    merged_bhav_df = merged_df.copy()
    merged_bhav_df['BhavClosingprice'] = 0

    merged_bhav_df.fillna(0, inplace=True)
    merged_bhav_df.buyAvgPrice = merged_bhav_df.buyAvgPrice.astype('int64')
    merged_bhav_df.sellAvgPrice = merged_bhav_df.sellAvgPrice.astype('int64')
    merged_bhav_df.BhavClosingprice = merged_bhav_df.BhavClosingprice.astype('int64')

    merged_bhav_df.EodExpiry = merged_bhav_df.EodExpiry.astype('str')
    merged_bhav_df.rename(columns={'BhavClosingprice': 'FinalSettlementPrice'}, inplace=True)
    logger.info(f'cp noncp length at {datetime.now()} is {merged_bhav_df.shape}')
    merged_bhav_df.EodExpiry = merged_bhav_df.EodExpiry.astype(str)
    return merged_bhav_df

def main(from_time, to_time):
    global main_mod_df
    logger.info(f'Notis trade data fetched from {from_time} to {to_time}')
    df_db = read_data_db(from_time=from_time,to_time=to_time)
    if df_db.empty:
        logger.info(f'No trade at {datetime.now().time()} hence skipping further processes')
        return
    # write_notis_postgredb(df_db, table_name=n_tbl_test_raw, raw=True)
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
    df_nnf = df_nnf.drop_duplicates()
    modified_df = NSEUtility.modify_file(df_db, df_nnf)
    # write_notis_postgredb(modified_df, table_name=n_tbl_test_mod)
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
    cp_noncp_df = calc_eod_cp_noncp(pivot_df)
    # write_notis_postgredb(cp_noncp_df, table_name=n_tbl_test_cp_noncp, truncate_required=True)
    desk_db_df = NSEUtility.calc_deskwise_net_pos(pivot_df)
    # write_notis_postgredb(desk_db_df, table_name=n_tbl_test_net_pos_desk, truncate_required=True)
    nnf_db_df = NSEUtility.calc_nnfwise_net_pos(pivot_df)
    # write_notis_postgredb(nnf_db_df, table_name=n_tbl_test_net_pos_nnf, truncate_required=True)

if __name__ == '__main__':
    recover = False
    stt = datetime.now().replace(hour=9, minute=15)
    ett = datetime.now().replace(hour=15, minute=35)
    actual_start_time = datetime.now()
    logger.info(f'test started at {datetime.now()}')
    if actual_start_time > stt and actual_start_time < ett:
        recover = True
    while datetime.now() < stt:
        time.sleep(1)
    while datetime.now() < ett:
        now = datetime.now()
        if now.second == 1:
            print('in if')
            if recover:
                print('in recover')
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
            # get_bse_trade_data(bse_from_time, bse_to_time)
            time.sleep(1)