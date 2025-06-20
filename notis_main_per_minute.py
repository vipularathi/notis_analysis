import os, warnings, time, requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from db_config import (engine_str,
                       n_tbl_notis_trade_book,n_tbl_notis_raw_data,n_tbl_bse_trade_data,
                       n_tbl_notis_eod_net_pos_cp_noncp,n_tbl_notis_desk_wise_net_position,
                       n_tbl_notis_nnf_data, n_tbl_notis_nnf_wise_net_position, n_tbl_srspl_trade_data)
from common import (get_date_from_non_jiffy, get_date_from_jiffy,
                    today, yesterday,
                    root_dir, logger, find_spot, analyze_expired_instruments,
                    read_data_db, write_notis_postgredb, truncate_tables)
from nse_utility import NSEUtility
from bse_utility import BSEUtility

warnings.filterwarnings("ignore")
today_date = datetime.now().date()

n_tbl_test_mod = n_tbl_notis_trade_book
n_tbl_test_raw = n_tbl_notis_raw_data
n_tbl_test_cp_noncp = n_tbl_notis_eod_net_pos_cp_noncp
n_tbl_test_net_pos_desk = n_tbl_notis_desk_wise_net_position
n_tbl_test_net_pos_nnf = n_tbl_notis_nnf_wise_net_position
n_tbl_test_bse = n_tbl_bse_trade_data
main_mod_df = pd.DataFrame()
main_mod_bse_df = pd.DataFrame()



def calc_rate(row):
    if row['EodOptionType'] == 'PE':
        return max(row['EodStrike']-row['ExpiredSpot_close'], 0)
    else:
        return max(row['ExpiredSpot_close']-row['EodStrike'], 0)

def get_bse_trade_data(from_time, to_time):
    global main_mod_bse_df
    pivot_df = pd.DataFrame()
    df_bse = read_data_db(for_table='TradeHist', from_time=from_time, to_time=to_time)
    if df_bse.empty:
        logger.info(f'No BSE trade from {from_time.split(" ")[1]} to {to_time.split(" ")[1]} hence skipping')
        return pivot_df
    logger.info(f'BSE trade data fetched from {from_time.split(" ")[1]} to {to_time.split(" ")[1]}, shape:{df_bse.shape}')
    modified_bse_df = BSEUtility.bse_modify_file(df_bse)
    write_notis_postgredb(df=modified_bse_df,table_name=n_tbl_bse_trade_data)
    logger.info(f'length of main_mod_bse_df before concat is {main_mod_bse_df.shape}')
    main_mod_bse_df = pd.concat([main_mod_bse_df,modified_bse_df],ignore_index=True)
    logger.info(f'length of main_mod_bse_df after concat is {main_mod_bse_df.shape}')
    main_mod_bse_df['trdQtyPrc'] = main_mod_bse_df['FillSize']*(main_mod_bse_df['FillPrice']/100)
    pivot_df = main_mod_bse_df.pivot_table(
        index=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
        columns=['TransactionType'],
        values=['FillSize', 'trdQtyPrc'],
        aggfunc={'FillSize': 'sum', 'trdQtyPrc': 'sum'},
        fill_value=0
    )
    # =========================================================================================
    if len(main_mod_bse_df.TransactionType.unique()) == 1:
        if main_mod_bse_df.TransactionType.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = 0;
            pivot_df['SellQty'] = 0
            pivot_df.columns = ['BuyQty','BuyTrdQtyPrc','SellQty','SellTrdQtyPrc']
        elif main_mod_bse_df.TransactionType.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = 0;
            pivot_df['BuyQty'] = 0
            pivot_df.columns = ['SellQty','SellTrdQtyPrc','BuyQty','BuyTrdQtyPrc']
    elif len(main_mod_bse_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    # =========================================================================================
    pivot_df['buyAvgPrice'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0.0, axis=1)
    pivot_df['sellAvgPrice'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0.0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df.rename(columns={'BuyTrdQtyPrc':'buyValue','SellTrdQtyPrc':'sellValue'}, inplace=True)
    pivot_df['IntradayVolume'] = pivot_df.BuyQty - pivot_df.SellQty
    # pivot_df = pivot_df.round(2)
    return pivot_df

def get_nse_trade(from_time, to_time):
    global main_mod_df
    pivot_df = pd.DataFrame()
    df_db = read_data_db(from_time=from_time,to_time=to_time)
    if df_db.empty:
        logger.info(f'No NSE trade between {from_time.split(" ")[1][:-4]} and {to_time.split(" ")[1][:-4]} hence skipping further processes')
        return pivot_df
    logger.info(f'Notis trade data fetched from {from_time.split(" ")[1][:-4]} to {to_time.split(" ")[1][:-4]}, shape:{df_db.shape}')
    write_notis_postgredb(df_db, table_name=n_tbl_test_raw, raw=True)
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
    main_mod_df['trdQtyPrc'] = main_mod_df['trdQty'] * (main_mod_df['trdPrc']/100)
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
            pivot_df.columns = ['BuyQty', 'BuyTrdQtyPrc', 'SellQty', 'SellTrdQtyPrc']
        elif main_mod_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = 0;
            pivot_df['BuyQty'] = 0
            pivot_df.columns = ['SellQty', 'SellTrdQtyPrc', 'BuyQty', 'BuyTrdQtyPrc']
    elif len(main_mod_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['BuyAvgPrc'] = pivot_df.apply(lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0.0,
                                           axis=1)
    pivot_df['SellAvgPrc'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0.0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)

    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyQty': 'buyAvgQty', 'BuyAvgPrc': 'buyAvgPrice','BuyTrdQtyPrc':'buyValue',
                             'SellQty': 'sellAvgQty', 'SellAvgPrc': 'sellAvgPrice', 'SellTrdQtyPrc':'sellValue'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    # pivot_df = pivot_df.round(2)
    return pivot_df

def find_net_pos(nse_pivot_df, bse_pivot_df):
    cp_noncp_nse_df, cp_noncp_bse_df = pd.DataFrame(), pd.DataFrame()
    if not nse_pivot_df.empty:
        #DESK
        desk_db_df = NSEUtility.calc_deskwise_net_pos(nse_pivot_df)
        write_notis_postgredb(desk_db_df, table_name=n_tbl_test_net_pos_desk, truncate_required=True)
        #NNF
        nnf_db_df = NSEUtility.calc_nnfwise_net_pos(nse_pivot_df)
        write_notis_postgredb(nnf_db_df, table_name=n_tbl_test_net_pos_nnf, truncate_required=True)
    if nse_pivot_df.empty and bse_pivot_df.empty:
        return
    else:
        #EOD_CP_NONCP
        eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}'  # NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
        final_eod = read_data_db(for_table=eod_tablename)
        logger.info(f'final_eod before calculation: {final_eod.shape}')
        final_eod.EodExpiry = pd.to_datetime(final_eod.EodExpiry, dayfirst=True, format='mixed').dt.date
        if not nse_pivot_df.empty:
            cp_noncp_nse_df = NSEUtility.calc_eod_cp_noncp(nse_pivot_df)
        else:
            cp_noncp_nse_df = final_eod.query("EodUnderlying != 'SENSEX'")
        if not bse_pivot_df.empty:
            cp_noncp_bse_df = BSEUtility.calc_bse_eod_net_pos(bse_pivot_df)
        else:
            cp_noncp_bse_df = final_eod.query("EodUnderlying == 'SENSEX'")
        final_eod = pd.concat([cp_noncp_nse_df, cp_noncp_bse_df], ignore_index=True)
        to_int = ['EodStrike','EodNetQuantity', 'buyQty', 'sellQty', 'IntradayVolume', 'FinalNetQty']
        final_eod.fillna(0, inplace=True)
        for each in to_int:
            final_eod[each] = final_eod[each].astype(np.int64)
        grouped_final_eod = final_eod.groupby(by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                              as_index=False).agg(
            {'EodNetQuantity': 'sum', 'buyQty': 'sum', 'buyAvgPrice': 'mean','buyValue':'sum','sellQty': 'sum', 'sellAvgPrice': 'mean', 'sellValue':'sum',
             'IntradayVolume': 'sum', 'FinalNetQty': 'sum'})
        grouped_final_eod['PreFinalNetQty'] = grouped_final_eod['EodNetQuantity'] + grouped_final_eod['IntradayVolume']
        mask = grouped_final_eod['EodOptionType'] == 'XX'
        masked_df = grouped_final_eod.loc[mask].copy()
        grouped_final_eod.loc[mask, 'buyAvgPrice'] = np.where(masked_df['buyQty'] > 0,
                                                              masked_df['buyValue'] / masked_df['buyQty'], 0)
        grouped_final_eod.loc[mask, 'sellAvgPrice'] = np.where(masked_df['sellQty'] > 0,
                                                               masked_df['sellValue'] / masked_df['sellQty'], 0)
        srspl_df = read_data_db(for_table=n_tbl_srspl_trade_data)
        grouped_final_eod = pd.concat([grouped_final_eod, srspl_df], ignore_index=True)
        grouped_final_eod['EodExpiry'] = pd.to_datetime(grouped_final_eod['EodExpiry'], dayfirst=True).dt.date
        grouped_final_eod.fillna(0, inplace=True)
        grouped_final_eod['ExpiredSpot_close'] = 0.0
        grouped_final_eod['ExpiredRate'] = 0.0
        grouped_final_eod['ExpiredAssn_value'] = 0.0
        grouped_final_eod['ExpiredSellValue'] = 0.0
        grouped_final_eod['ExpiredBuyValue'] = 0.0
        grouped_final_eod['ExpiredQty'] = 0.0
        if today in grouped_final_eod['EodExpiry'].unique():
            grouped_final_eod = analyze_expired_instruments(grouped_final_eod=grouped_final_eod)
        grouped_final_eod['FinalNetQty'] = grouped_final_eod['PreFinalNetQty'] + grouped_final_eod['ExpiredQty']
        grouped_final_eod.drop(columns=['buyAvgPrice', 'sellAvgPrice', 'IntradayVolume'], inplace=True)
        logger.info(f'final_eod after calculation: {grouped_final_eod.shape}')
        write_notis_postgredb(grouped_final_eod, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)


if __name__ == '__main__':
    if today_date == today:
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
                    logger.info('in recover')
                    table_list = [n_tbl_test_mod, n_tbl_test_raw, n_tbl_test_cp_noncp, n_tbl_test_net_pos_desk,
                                  n_tbl_test_net_pos_nnf, n_tbl_test_bse]
                    # for each in table_list:
                    #     truncate_tables(each)
                    nse_from_time = stt.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    nse_to_time = now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    bse_from_time = stt.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                    bse_to_time = now.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                    recover = False
                else:
                    nse_from_time = (now - timedelta(minutes=1)).replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    nse_to_time = now.replace(second=0).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    bse_from_time = (now - timedelta(minutes=1)).replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                    bse_to_time = now.replace(second=0).strftime('%d-%b-%Y %H:%M:%S')
                logger.info(f"\nnow time => {now.strftime('%Y-%m-%d %H:%M:%S')}")
                nse_pivot_df = get_nse_trade(nse_from_time,nse_to_time)
                bse_pivot_df = get_bse_trade_data(bse_from_time,bse_to_time)
                find_net_pos(nse_pivot_df=nse_pivot_df,bse_pivot_df=bse_pivot_df)
                # get_bse_trade_data(bse_from_time, bse_to_time)
                time.sleep(1)
    else:
        logger.info(f'Today is not a business day hence exiting.')
        exit()