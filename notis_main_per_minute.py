import os, warnings, time, requests, mibian, re, scipy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from db_config import (engine_str,
                       n_tbl_notis_trade_book,n_tbl_notis_raw_data,n_tbl_bse_trade_data,
                       n_tbl_notis_eod_net_pos_cp_noncp,n_tbl_notis_desk_wise_net_position,
                       n_tbl_notis_nnf_data, n_tbl_notis_nnf_wise_net_position, n_tbl_srspl_trade_data,
                       n_tbl_notis_delta_table)
from common import (get_date_from_non_jiffy, get_date_from_jiffy,
                    today, yesterday, holidays_25,
                    root_dir, volt_dir, logger, find_spot, analyze_expired_instruments,
                    read_data_db, read_file, write_notis_postgredb, truncate_tables)
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

def calc_dte(row):
    bdays_left = pd.bdate_range(start=today, end=row['EodExpiry'], freq='C', weekmask='1111100', holidays=holidays_25)
    actual_bdays_left = len(bdays_left)
    return actual_bdays_left
def get_delta(row):
    int_rate,annual_div = 5.5,0
    spot = row['spot']
    strike = row['EodStrike']
    dte = row['dte']
    # dte = (row['EodExpiry'] - today).days
    vol = row['volatility']
    if row['EodOptionType'] == 'XX':
        return 1.0
    calc = mibian.BS(
        [spot, strike, int_rate, dte],
        volatility=vol
    )
    return calc.callDelta if row['EodOptionType'] == 'CE' else calc.putDelta
def calc_rate(row):
    if row['EodOptionType'] == 'PE':
        return max(row['EodStrike']-row['ExpiredSpot_close'], 0)
    else:
        return max(row['ExpiredSpot_close']-row['EodStrike'], 0)
def calc_delta(eod_df):
    eod_df = eod_df.copy()
    sym_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
    col_keep = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType', 'PreFinalNetQty']
    eod_df.drop(columns=[col for col in eod_df.columns if col not in col_keep], inplace=True)
    volt_df = read_file(os.path.join(volt_dir, f'FOVOLT_{yesterday.strftime("%d%m%Y")}.csv'))
    volt_df.columns = [re.sub(r'\s', '', each) for each in volt_df.columns]
    volt_df.rename(columns={'ApplicableAnnualisedVolatility(N)=Max(ForL)': 'AnnualizedReturn'}, inplace=True)
    volt_df = volt_df.iloc[:, [1, -1]].query("Symbol in @sym_list")
    volt_df = volt_df.applymap(lambda x: re.sub(r'\s+', '', x) if isinstance(x, str) else x)
    volt_df['AnnualizedReturn'] = volt_df['AnnualizedReturn'].astype(np.float64)
    spot_dict = find_spot()
    volt_dict = dict(zip(volt_df['Symbol'], volt_df['AnnualizedReturn']))
    eod_df['spot'] = eod_df['EodUnderlying'].map(spot_dict)
    eod_df['volatility'] = eod_df['EodUnderlying'].map(volt_dict)
    eod_df['volatility'] = eod_df['volatility'].astype(np.float64)
    eod_df['volatility'] = eod_df['volatility'] * 100
    eod_df['dte'] = eod_df['EodExpiry'].apply(lambda x: (x-today).days)
    mask = eod_df['EodOptionType'] == 'XX'
    eod_df.loc[mask, 'volatility'] = 1
    eod_df['deltaPerUnit'] = eod_df.apply(get_delta, axis=1).astype(np.float64)
    eod_df['deltaQty'] = eod_df['PreFinalNetQty'] * eod_df['deltaPerUnit']
    eod_df['deltaExposure(in Cr)'] = (eod_df['spot'] * eod_df['deltaQty']) / 10_000_000
    mask = eod_df['EodOptionType'].isin(['CE', 'PE'])
    eod_df.loc[mask, 'EodOptionType'] = 'CE_PE'
    final_eod_df = pd.DataFrame()
    for each in ['XX', 'CE_PE']:
        temp_eod_df = eod_df.query("EodOptionType == @each")
        grouped_temp_eod_df = temp_eod_df.groupby(
            by=['EodOptionType', 'EodBroker', 'EodUnderlying'],
            as_index=False
        )['deltaExposure(in Cr)'].agg(
            {'Long': lambda x: x[x > 0].sum(), 'Short': lambda x: x[x < 0].sum(), 'Net': 'sum'}
        )
        total_dict = {
            'EodOptionType': '',
            'EodBroker': 'Total',
            'EodUnderlying': '',
            'Long': grouped_temp_eod_df['Long'].sum(),
            'Short': grouped_temp_eod_df['Short'].sum(),
            'Net': grouped_temp_eod_df['Net'].sum()
        }
        grouped_temp_eod_df = pd.concat([grouped_temp_eod_df, pd.DataFrame([total_dict])], ignore_index=True)
        final_eod_df = pd.concat([final_eod_df, grouped_temp_eod_df], ignore_index=True)
    for each in ['deltaExposure(in Cr)', 'deltaQty']:
        grouped_df = eod_df.groupby(by=['EodBroker', 'EodUnderlying'], as_index=False)[each].agg(
            {'Long': lambda x: x[x > 0].sum(), 'Short': lambda x: x[x < 0].sum(), 'Net': 'sum'}
        )
        if each == 'deltaExposure(in Cr)':
            use = 'Combined'
            grouped_df['EodOptionType'] = 'Combined'
        else:
            use = 'DeltaQty'
            grouped_df['EodOptionType'] = 'DeltaQty'
            grouped_df['Long'] = grouped_df['Long'] / 100000
            grouped_df['Short'] = grouped_df['Short'] / 100000
            grouped_df['Net'] = grouped_df['Net'] / 100000
        total_dict = {
            'EodOptionType': use,
            'EodBroker': 'Total',
            'EodUnderlying': '',
            'Long': grouped_df['Long'].sum(),
            'Short': grouped_df['Short'].sum(),
            'Net': grouped_df['Net'].sum()
        }
        grouped_df = pd.concat([grouped_df, pd.DataFrame([total_dict])], ignore_index=False)
        final_eod_df = pd.concat([final_eod_df, grouped_df], ignore_index=False)
    return final_eod_df
# ===================================================================================================
def get_bse_trade_data(from_time, to_time):
    global main_mod_bse_df
    pivot_df = pd.DataFrame()
    df_bse1 = read_data_db(for_table='BSE_ENetMIS', from_time=from_time, to_time=to_time)
    # df_bse1 = read_data_db(for_table='BSE_ENetMIS')
    if df_bse1 is None or df_bse1.empty:
        logger.info(f'No BSE trade from {from_time} to {to_time} hence skipping')
        return pivot_df
    logger.info(f'BSE trade data fetched from {from_time} to {to_time}, shape:{df_bse1.shape}')
    modified_bse_df1 = BSEUtility.bse_modify_file_v2(df_bse1)
    modified_bse_df1.TraderID = modified_bse_df1.TraderID.astype(np.int64)
    
    df_bse2 = read_data_db(for_table='TradeHist')
    # df_bse2 = read_data_db(for_table='TradeHist', from_time=from_time, to_time=to_time)
    modified_bse_df2 = BSEUtility.bse_modify_file(df_bse2)
    modified_bse_df2 = modified_bse_df2[['TerminalID','TradingSymbol','FillSize','TransactionType','ExchUser','Underlying', 'Strike', 'OptionType', 'Expiry']]
    modified_bse_df2.ExchUser = modified_bse_df2.ExchUser.astype(np.int64)
    modified_bse_df2.ExchUser = modified_bse_df2.ExchUser % 10000
    grouped_modified_bse_df2 = (
        modified_bse_df2
        .groupby(['ExchUser', 'TradingSymbol', 'FillSize', 'TransactionType',
                  'Underlying', 'Strike', 'OptionType', 'Expiry'], as_index=False)
        .agg({'TerminalID': 'first'})
    )
    modified_bse_df = pd.merge(modified_bse_df1, grouped_modified_bse_df2,
                               left_on=['TraderID', 'TradingSymbol', 'FillSize', 'TransactionType','Underlying', 'Strike', 'OptionType', 'Expiry'],
                               right_on=['ExchUser', 'TradingSymbol', 'FillSize', 'TransactionType','Underlying', 'Strike', 'OptionType', 'Expiry'],
                               how='left'
                               )
    modified_bse_df['TerminalID'] = np.where(modified_bse_df['TraderID'] == 1011, '945440A', modified_bse_df['TerminalID'])
    modified_bse_df.drop(columns=['ExchUser'], axis=1, inplace=True)
    modified_bse_df.fillna(0, inplace=True)
    write_notis_postgredb(df=modified_bse_df,table_name=n_tbl_bse_trade_data)
    logger.info(f'length of main_mod_bse_df before concat is {main_mod_bse_df.shape}')
    main_mod_bse_df = pd.concat([main_mod_bse_df,modified_bse_df],ignore_index=True)
    # main_mod_bse_df = modified_bse_df.copy()
    logger.info(f'length of main_mod_bse_df after concat is {main_mod_bse_df.shape}')
    main_mod_bse_df['trdQtyPrc'] = main_mod_bse_df['FillSize']*(main_mod_bse_df['FillPrice']/100)
    pivot_df = main_mod_bse_df.pivot_table(
        index=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType', 'TerminalID','TraderID'],
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
    if df_db is None or df_db.empty:
        logger.info(f'No NSE trade between {from_time.split(" ")[1][:-4]} and {to_time.split(" ")[1][:-4]} hence skipping further processes')
        return pivot_df
    logger.info(f'Notis trade data fetched from {from_time.split(" ")[1][:-4]} to {to_time.split(" ")[1][:-4]}, shape:{df_db.shape}')
    write_notis_postgredb(df_db, table_name=n_tbl_test_raw, raw=True)
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
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
        cp_noncp_nse_df = NSEUtility.calc_eod_cp_noncp(nse_pivot_df)
        cp_noncp_bse_df = BSEUtility.calc_bse_eod_net_pos(bse_pivot_df)
        # srspl_df = final_eod.query("EodBroker == 'SRSPL'")
        final_eod = pd.concat([cp_noncp_nse_df, cp_noncp_bse_df], ignore_index=True)
        # final_eod = pd.concat([pre_final_eod,srspl_df], ignore_index=True)
        to_int = ['EodStrike','EodNetQuantity', 'buyQty', 'sellQty', 'IntradayVolume', 'FinalNetQty']
        final_eod.fillna(0, inplace=True)
        for each in to_int:
            final_eod[each] = final_eod[each].astype(np.int64)
        grouped_final_eod = final_eod.groupby(by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                              as_index=False).agg({
            'EodNetQuantity': 'sum', 'buyQty': 'sum', 'buyAvgPrice': 'mean', 'buyValue': 'sum',
            'sellQty': 'sum', 'sellAvgPrice': 'mean', 'sellValue': 'sum', 'IntradayVolume': 'sum', 'FinalNetQty': 'sum'
        })
        grouped_final_eod['PreFinalNetQty'] = (grouped_final_eod['EodNetQuantity'] + grouped_final_eod['buyQty'] -
                                               grouped_final_eod['sellQty'])
        mask = grouped_final_eod['EodOptionType'] == 'XX'
        masked_df = grouped_final_eod.loc[mask].copy()
        grouped_final_eod.loc[mask, 'buyAvgPrice'] = np.where(masked_df['buyQty'] > 0,
                                                              masked_df['buyValue'] / masked_df['buyQty'], 0)
        grouped_final_eod.loc[mask, 'sellAvgPrice'] = np.where(masked_df['sellQty'] > 0,
                                                               masked_df['sellValue'] / masked_df['sellQty'], 0)
        today_srspl_df = read_data_db(for_table=n_tbl_srspl_trade_data)
        yest_srspl_df = read_data_db(for_table=f"NOTIS_EOD_NET_POS_CP_NONCP_{yesterday}")
        yest_srspl_df['EodExpiry'] = pd.to_datetime(yest_srspl_df['EodExpiry'], dayfirst=True).dt.date
        yest_srspl_df = yest_srspl_df.query("EodBroker not in ['CP','non CP'] and FinalNetQty != 0 and EodExpiry >= @today")
        yest_srspl_df['EodNetQuantity'] = yest_srspl_df['FinalNetQty']
        yest_srspl_df['PreFinalNetQty'] = yest_srspl_df['FinalNetQty']
        not_to_zero = ['EodBroker', 'EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry', 'EodNetQuantity']
        yest_srspl_df.loc[:, ~yest_srspl_df.columns.isin(not_to_zero)] = 0
        yest_srspl_df = yest_srspl_df[today_srspl_df.columns.tolist()]
        final_srspl_df = pd.concat([yest_srspl_df, today_srspl_df], ignore_index=True)
        final_srspl_df.fillna(0, inplace=True)
        final_srspl_df['EodExpiry'] = pd.to_datetime(final_srspl_df['EodExpiry'], dayfirst=True).dt.date
        grouped_srspl_df = final_srspl_df.groupby(
            by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike','EodOptionType'],
            as_index=False).agg(
            {'EodNetQuantity': 'last', 'buyQty': 'sum', 'buyValue': 'sum', 'sellQty': 'sum', 'sellValue': 'sum',
             'PreFinalNetQty': 'sum'}
        )
        grouped_srspl_df.fillna(0, inplace=True)
        grouped_final_eod = pd.concat([grouped_final_eod, grouped_srspl_df], ignore_index=True)
        grouped_final_eod['PreFinalNetQty'] = (grouped_final_eod['EodNetQuantity'] + grouped_final_eod['buyQty'] -
                                              grouped_final_eod['sellQty'])
        grouped_final_eod['EodExpiry'] = pd.to_datetime(grouped_final_eod['EodExpiry'], dayfirst=True).dt.date
        delta_df = calc_delta(grouped_final_eod)
        write_notis_postgredb(df=delta_df, table_name=n_tbl_notis_delta_table, truncate_required=True)
        # ===============================================================================================================
        # grouped_final_eod.fillna(0, inplace=True)
        grouped_final_eod['ExpiredSpot_close'] = 0.0
        grouped_final_eod['ExpiredRate'] = 0.0
        grouped_final_eod['ExpiredAssn_value'] = 0.0
        grouped_final_eod['ExpiredSellValue'] = 0.0
        grouped_final_eod['ExpiredBuyValue'] = 0.0
        grouped_final_eod['ExpiredQty'] = 0.0
        if today in grouped_final_eod['EodExpiry'].unique():
            grouped_final_eod = analyze_expired_instruments(grouped_final_eod=grouped_final_eod)
        grouped_final_eod['FinalNetQty'] = grouped_final_eod['PreFinalNetQty'] + grouped_final_eod['ExpiredQty']
        grouped_final_eod.drop(columns=['IntradayVolume'], inplace=True)
        logger.info(f'final_eod after calculation: {grouped_final_eod.shape}')
        write_notis_postgredb(grouped_final_eod, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)


if __name__ == '__main__':
    if today_date == today:
        recover = False
        stt = datetime.now().replace(hour=9, minute=15)
        ett = datetime.now().replace(hour=15, minute=35)
        actual_start_time = datetime.now()
        logger.info(f'Notis Backend started at {datetime.now()}')
        if actual_start_time > stt and actual_start_time < ett:
            recover = True
        while datetime.now() < stt:
            time.sleep(1)
        while datetime.now() < ett:
            now = datetime.now()
            if now.second == 1:
                print('\nin if')
                if recover:
                    logger.info('in recover')
                    table_list = [n_tbl_test_mod, n_tbl_test_raw, n_tbl_test_cp_noncp, n_tbl_test_net_pos_desk,
                                  n_tbl_test_net_pos_nnf, n_tbl_test_bse]
                    for each in table_list:
                        truncate_tables(each)
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