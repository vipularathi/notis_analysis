import re, os, progressbar, pyodbc, warnings, psycopg2, time, mibian, scipy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

from db_config import (n_tbl_notis_trade_book, n_tbl_notis_raw_data,
                       n_tbl_notis_nnf_data, n_tbl_notis_desk_wise_net_position,
                       n_tbl_notis_nnf_wise_net_position, n_tbl_notis_delta_table,
                       n_tbl_notis_eod_net_pos_cp_noncp, n_tbl_bse_trade_data,
                       n_tbl_srspl_trade_data, n_tbl_notis_deal_sheet)
from common import (read_data_db, write_notis_data, write_notis_postgredb, read_file,
                    today,yesterday, holidays_25,
                    root_dir, bhav_dir, modified_dir, table_dir, bse_dir, volt_dir,
                    download_bhavcopy, logger, find_spot, analyze_expired_instruments)
from nse_utility import NSEUtility
from bse_utility import BSEUtility

warnings.filterwarnings('ignore', message="pandas only supports SQLAlchemy connectable.*")
pd.set_option('display.float_format', lambda a:'%.2f' %a)
actual_date = datetime.now().date()

def calc_dte(row):
    bdays_left = pd.bdate_range(start=today, end=row['EodExpiry'], freq='C', weekmask='1111100', holidays=holidays_25)
    actual_bdays_left = len(bdays_left)
    return actual_bdays_left
def get_delta(row):
    int_rate,annual_div = 5.5,0
    # if row['EodExpiry'] == pd.to_datetime('2025-07-31').date() and row['EodOptionType'] == 'CE':
    #     p=0
    spot = row['spot']
    strike = row['EodStrike']
    dte = row['dte']
    vol = row['volatility']
    if row['EodOptionType'] == 'XX':
        return 1.0
    calc = mibian.BS(
        [spot, strike, int_rate, dte],
        volatility=vol
    )
    return calc.callDelta if row['EodOptionType'] == 'CE' else calc.putDelta

def calc_delta(eod_df):
    eod_df = eod_df.copy()
    sym_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
    col_keep = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType', 'PreFinalNetQty']
    eod_df.drop(columns=[col for col in eod_df.columns if col not in col_keep], inplace=True)
    volt_df = read_file(os.path.join(volt_dir, f'FOVOLT_{yesterday.strftime("%d%m%Y")}.csv'))
    volt_df.columns = [re.sub(r'\s', '', each) for each in volt_df.columns]
    volt_df.rename(columns={'ApplicableAnnualisedVolatility(N)=Max(ForL)': 'AnnualizedReturn'}, inplace=True)
    volt_df = volt_df.iloc[:, [1, -1]].query("Symbol in @sym_list")
    volt_df = volt_df.applymap(lambda x:re.sub(r'\s+','',x) if isinstance(x,str) else x)
    volt_df['AnnualizedReturn'] = volt_df['AnnualizedReturn'].astype(np.float64)
    spot_dict = find_spot()
    volt_dict = dict(zip(volt_df['Symbol'], volt_df['AnnualizedReturn']))
    eod_df['spot'] = eod_df['EodUnderlying'].map(spot_dict)
    eod_df['volatility'] = eod_df['EodUnderlying'].map(volt_dict)
    eod_df['volatility'] = eod_df['volatility'].astype(np.float64)
    eod_df['volatility'] = eod_df['volatility'] * 100
    eod_df['dte'] = eod_df['EodExpiry'].apply(lambda x: ((x-today).days) + 1)
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
        grouped_temp_eod_df = temp_eod_df.groupby(by=['EodOptionType', 'EodBroker', 'EodUnderlying'], as_index=False)[
            'deltaExposure(in Cr)'].agg(
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
    
def calc_rate(row):
    if row['EodOptionType'] == 'PE':
        return max(row['EodStrike']-row['ExpiredSpot_close'],0)
    else:
        return max(row['ExpiredSpot_close']-row['EodStrike'], 0)
    
def download_tables():
    table_list = [n_tbl_notis_desk_wise_net_position, n_tbl_notis_nnf_wise_net_position,n_tbl_notis_eod_net_pos_cp_noncp, n_tbl_bse_trade_data]
    # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
    for table in table_list:
        df = read_data_db(for_table=table)
        # df.to_csv(os.path.join(table_dir, f"{table}.xlsx"), index=False)
        write_notis_data(df=df, filepath=os.path.join(table_dir,f'{table}.xlsx'))
        logger.info(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}.xlsx')}")

def get_nse_data():
    logger.info(f'fetching NSE trades...')
    df_db = read_data_db()
    if df_db is None or df_db.empty:
        logger.info(f'No NSE trade done today hence skipping')
        df = pd.DataFrame()
        return df
    logger.info(f'Notis trade data fetched, shape={df_db.shape}')
    write_notis_postgredb(df=df_db, table_name=n_tbl_notis_raw_data, raw=True, truncate_required=True)
    modify_filepath = os.path.join(modified_dir, f'NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    if not os.path.exists(nnf_file_path):
        raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")
    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    if readable_mod_time.date() == today: # Check if the NNF file is modified today or not
        logger.info(f'New NNF Data found, modifying the nnf data in db . . .')
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
    logger.info('file saved in modified_data folder')
    modified_df['trdQtyPrc'] = modified_df['trdQty'] * (modified_df['trdPrc']/100)
    pivot_df = modified_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdQtyPrc', 'trdPrc'],
        aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum', 'trdPrc':['min','max']},
        fill_value=0
    )
    if len(modified_df.bsFlg.unique()) == 1:
        if modified_df.bsFlg.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = pivot_df['SellQty'] = pivot_df['SellMax'] = pivot_df['SellMin'] = 0
            pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
        elif modified_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = pivot_df['BuyQty'] = pivot_df['BuyMax'] = pivot_df['BuyMin'] = 0
            pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
    elif len(modified_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['BuyMax','SellMax','BuyMin','SellMin','BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['BuyAvgPrc'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0.0,
        axis=1)
    pivot_df['SellAvgPrc'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0.0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice','BuyTrdQtyPrc':'buyValue',
                             'SellAvgPrc': 'sellAvgPrice', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty','SellTrdQtyPrc':'sellValue'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    return pivot_df

def get_bse_data():
    stt = datetime.now()
    logger.info(f'fetching BSE trades...')
    df_bse1 = read_data_db(for_table='BSE_ENetMIS')
    if df_bse1 is None or df_bse1.empty:
        logger.info(f'No BSE trade done today hence skipping')
        df = pd.DataFrame()
        return df
    logger.info(f'BSE trade data fetched, shape={df_bse1.shape}')
    modified_bse_df1 = BSEUtility.bse_modify_file_v2(df_bse1)
    modified_bse_df1.TraderID = modified_bse_df1.TraderID.astype(np.int64)
    
    df_bse2 = read_data_db(for_table='TradeHist')
    modified_bse_df2 = BSEUtility.bse_modify_file(df_bse2)
    modified_bse_df2 = modified_bse_df2[['TerminalID', 'ExchUser', 'TradingSymbol', 'FillSize', 'TransactionType','Underlying', 'Strike', 'OptionType', 'Expiry']]
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
    modified_bse_df['TerminalID'] = np.where(modified_bse_df['TraderID'] == 1011, '945440A',
                                             modified_bse_df['TerminalID'])
    modified_bse_df.drop(columns=['ExchUser'], axis=1, inplace=True)
    modified_bse_df.fillna(0, inplace=True)
    write_notis_postgredb(df=modified_bse_df,table_name=n_tbl_bse_trade_data,truncate_required=True)
    write_notis_data(modified_bse_df, os.path.join(bse_dir, f'BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx'))
    write_notis_data(modified_bse_df, rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx')
    modified_bse_df['trdQtyPrc'] = modified_bse_df['FillSize'] * (modified_bse_df['AvgPrice'] / 100)
    pivot_df = modified_bse_df.pivot_table(
        index=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType', 'TerminalID', 'TraderID'],
        columns=['TransactionType'],
        values=['FillSize', 'trdQtyPrc', 'AvgPrice'],
        aggfunc={'FillSize': 'sum', 'trdQtyPrc': 'sum', 'AvgPrice':['min','max']},
        fill_value=0
    )
    if len(modified_bse_df.TransactionType.unique()) == 1:
        if modified_bse_df.TransactionType.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = pivot_df['SellQty'] = pivot_df['sellMax'] = pivot_df['sellMin'] = 0
            pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
        elif modified_bse_df.TransactionType.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = pivot_df['BuyQty'] = pivot_df['buyMax'] = pivot_df['buyMin'] = 0
            pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
    elif len(modified_bse_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['buyMax','sellMax','buyMin','sellMin','BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['buyAvgPrice'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0, axis=1)
    pivot_df['sellAvgPrice'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df['IntradayVolume'] = pivot_df['BuyQty'] - pivot_df['SellQty']
    pivot_df.rename(columns={'BuyTrdQtyPrc': 'buyValue', 'SellTrdQtyPrc': 'sellValue'}, inplace=True)
    # pivot_df = pivot_df.round(2)
    ett = datetime.now()
    logger.info(f'BSE trade fetched. Total time taken: {(ett - stt).seconds} seconds')
    return pivot_df

def find_net_pos(nse_pivot_df, bse_pivot_df):
    # DESK
    dsk_db_df = NSEUtility.calc_deskwise_net_pos(nse_pivot_df)
    write_notis_postgredb(dsk_db_df, table_name=n_tbl_notis_desk_wise_net_position, truncate_required=True)
    # NNF
    nnf_db_df = NSEUtility.calc_nnfwise_net_pos(nse_pivot_df)
    write_notis_postgredb(nnf_db_df, table_name=n_tbl_notis_nnf_wise_net_position, truncate_required=True)
    # CP NONCP
    nse_deal_df = NSEUtility.calc_nse_deal_sheet(nse_pivot_df)
    bse_deal_df = BSEUtility.calc_bse_deal_sheet(bse_pivot_df)
    final_deal_df = pd.concat([nse_deal_df, bse_deal_df], ignore_index=True)
    grouped_main_deal_df = final_deal_df.groupby(
        by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
        as_index=False
    ).agg(
        {'BuyMax': 'max', 'SellMax': 'max',
         'BuyMin': 'min', 'SellMin': 'min',
         'BuyQty': 'sum', 'SellQty': 'sum',
         'BuyValue':'sum', 'SellValue':'sum'}
    )
    write_notis_postgredb(df=grouped_main_deal_df, table_name=n_tbl_notis_deal_sheet, truncate_required=True)
    cp_noncp_nse_df = NSEUtility.calc_eod_cp_noncp(nse_pivot_df)
    cp_noncp_bse_df = BSEUtility.calc_bse_eod_net_pos(bse_pivot_df)
    final_cp_noncp_eod_df = pd.concat([cp_noncp_nse_df, cp_noncp_bse_df], ignore_index=True)
    underlying_list = ['NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY', 'SENSEX', 'BANKEX']
    final_cp_noncp_eod_df = final_cp_noncp_eod_df.query("EodUnderlying in @underlying_list")
    final_cp_noncp_eod_df.fillna(0, inplace=True)
    to_int = ['EodStrike','EodNetQuantity', 'buyQty', 'sellQty', 'IntradayVolume', 'FinalNetQty']
    for each in to_int:
        final_cp_noncp_eod_df[each] = final_cp_noncp_eod_df[each].astype(np.int64)
    grouped_final_eod = final_cp_noncp_eod_df.groupby(
        by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
        as_index=False).agg(
        {'EodNetQuantity': 'sum', 'buyQty': 'sum', 'buyAvgPrice': 'mean','buyValue':'sum',
        'sellQty': 'sum','sellAvgPrice': 'mean','sellValue':'sum'}
    )
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
    grouped_final_eod.fillna(0, inplace=True)
    delta_df = calc_delta(grouped_final_eod)
    write_notis_postgredb(df=delta_df, table_name=n_tbl_notis_delta_table, truncate_required=True)
    grouped_final_eod['ExpiredSpot_close'] = 0.0
    grouped_final_eod['ExpiredRate'] = 0.0
    grouped_final_eod['ExpiredAssn_value'] = 0.0
    grouped_final_eod['ExpiredSellValue'] = 0.0
    grouped_final_eod['ExpiredBuyValue'] = 0.0
    grouped_final_eod['ExpiredQty'] = 0.0
    if today in grouped_final_eod.EodExpiry.unique():
        spot_dict = find_spot()
        mask = grouped_final_eod['EodExpiry'] == today
        grouped_final_eod.loc[mask, 'ExpiredSpot_close'] = grouped_final_eod['EodUnderlying'].map(spot_dict)
        grouped_final_eod.loc[mask, 'ExpiredRate'] = grouped_final_eod.loc[mask].apply(calc_rate, axis=1)
        grouped_final_eod.loc[mask, 'ExpiredAssn_value'] = (grouped_final_eod.loc[mask, 'PreFinalNetQty'] * grouped_final_eod.loc[mask, 'ExpiredRate'])
        grouped_final_eod.loc[mask, 'ExpiredSellValue'] = np.where(grouped_final_eod.loc[mask, 'PreFinalNetQty'] > 0,
                                                                  abs(grouped_final_eod.loc[mask,
                                                                  'ExpiredAssn_value']), 0)
        grouped_final_eod.loc[mask, 'ExpiredBuyValue'] = np.where(grouped_final_eod.loc[mask, 'PreFinalNetQty'] < 0,
                                                                   abs(grouped_final_eod.loc[mask,
                                                                   'ExpiredAssn_value']), 0)
        grouped_final_eod.loc[mask, 'ExpiredQty'] = -1 * grouped_final_eod.loc[mask, 'PreFinalNetQty']
    grouped_final_eod['FinalNetQty'] = grouped_final_eod['PreFinalNetQty'] + grouped_final_eod['ExpiredQty']
    # grouped_final_eod.drop(columns=['IntradayVolume'], inplace=True)
    grouped_final_eod = grouped_final_eod.round(2)
    write_notis_postgredb(grouped_final_eod, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)

if __name__ == '__main__':
    if actual_date == today:
        logger.info(f'Starting final main.')
        download_bhavcopy()
        logger.info(f'Today\'s bhavcopy downloaded and stored at {bhav_dir}')
        stt = time.time()
        nse_pivot_df = get_nse_data()
        bse_pivot_df = get_bse_data()
        find_net_pos(nse_pivot_df=nse_pivot_df, bse_pivot_df=bse_pivot_df)
        ett = time.time()
        logger.info(f'total time taken for modifying, adding data in db and writing in local directory - {ett - stt} seconds')
        pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
        pbar.update(1)
        for i in range(100):
            time.sleep(1)
            pbar.update(i + 1)
        pbar.finish()
        download_tables()
    else:
        logger.info(f'Today is not a business date hence exiting.')
        exit()