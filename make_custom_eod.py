import pandas as pd
import numpy as np
import os
from datetime import timedelta
from nse_utility import NSEUtility
from bse_utility import BSEUtility
from common import (read_data_db, analyze_expired_instruments_v2, test_dir, calc_delta_v2,
                    write_notis_postgredb)
    
def get_nse_data():
    modified_df = read_data_db(for_table=f'NOTIS_TRADE_BOOK_{for_dt}')
    # write_notis_postgredb(modified_df, table_name=n_tbl_notis_trade_book, truncate_required=True)
    # write_notis_data(modified_df, modify_filepath)
    modified_df['trdQtyPrc'] = modified_df['trdQty'] * (modified_df['trdPrc'] / 100)
    pivot_df = modified_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdQtyPrc', 'trdPrc'],
        aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum', 'trdPrc': ['min', 'max']},
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
        pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                            'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['BuyAvgPrc'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0.0,
        axis=1)
    pivot_df['SellAvgPrc'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0.0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice',
                             'BuyTrdQtyPrc': 'buyValue', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty',
                             'SellAvgPrc': 'sellAvgPrice', 'SellTrdQtyPrc': 'sellValue'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    return pivot_df

def get_bse_data():
    modified_bse_df = read_data_db(for_table=f'BSE_TRADE_DATA_{for_dt}')
    modified_bse_df['trdQtyPrc'] = modified_bse_df['FillSize'] * (modified_bse_df['AvgPrice'] / 100)
    pivot_df = modified_bse_df.pivot_table(
        index=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType', 'TerminalID', 'TraderID'],
        columns=['TransactionType'],
        values=['FillSize', 'trdQtyPrc', 'AvgPrice'],
        aggfunc={'FillSize': 'sum', 'trdQtyPrc': 'sum', 'AvgPrice': ['min', 'max']},
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
        pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                            'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['buyAvgPrice'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0, axis=1)
    pivot_df['sellAvgPrice'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df['IntradayVolume'] = pivot_df['BuyQty'] - pivot_df['SellQty']
    pivot_df.rename(columns={'BuyTrdQtyPrc': 'buyValue', 'SellTrdQtyPrc': 'sellValue'}, inplace=True)
    return pivot_df
def find_eod(nse_df, bse_df):
    nse_df, bse_df = nse_df.copy(), bse_df.copy()
    # nse_list = nse_df.symbol.unique().tolist()
    # bse_list = bse_df.Underlying.unique().tolist()
    cp_noncp_nse_df = NSEUtility.calc_eod_cp_noncp_v2(for_date=for_dt,for_date_yest=for_dt_yest,desk_db_df=nse_df)
    cp_noncp_bse_df = BSEUtility.calc_bse_eod_net_pos_v2(for_date=for_dt,for_date_yest=for_dt_yest,desk_bse_df=bse_df)
    final_eod = pd.concat([cp_noncp_nse_df, cp_noncp_bse_df], ignore_index=True)
    to_int = ['EodStrike','EodNetQuantity', 'buyQty', 'sellQty', 'FinalNetQty']
    final_eod.fillna(0, inplace=True)
    for each in to_int:
        final_eod[each] = final_eod[each].astype(np.int64)
    grouped_final_eod = final_eod.groupby(
        by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
        as_index=False).agg(
        {
            'EodNetQuantity': 'sum', 'buyQty': 'sum', 'buyAvgPrice': 'mean', 'buyValue': 'sum',
            'sellQty': 'sum', 'sellAvgPrice': 'mean', 'sellValue': 'sum', 'FinalNetQty': 'sum'
        }
    )
    grouped_final_eod['PreFinalNetQty'] = (grouped_final_eod['EodNetQuantity'] + grouped_final_eod['buyQty'] -
                                           grouped_final_eod['sellQty'])
    mask = grouped_final_eod['EodOptionType'] == 'XX'
    masked_df = grouped_final_eod.loc[mask].copy()
    grouped_final_eod.loc[mask, 'buyAvgPrice'] = np.where(masked_df['buyQty'] > 0,
                                                          masked_df['buyValue'] / masked_df['buyQty'], 0)
    grouped_final_eod.loc[mask, 'sellAvgPrice'] = np.where(masked_df['sellQty'] > 0,
                                                           masked_df['sellValue'] / masked_df['sellQty'], 0)
    # ===============================================================================================================
    today_srspl_df = read_data_db(for_table=f"SRSPL_TRADE_DATA_{for_dt}")
    yest_srspl_df = read_data_db(for_table=f"NOTIS_EOD_NET_POS_CP_NONCP_{for_dt_yest}")
    yest_srspl_df['EodExpiry'] = pd.to_datetime(yest_srspl_df['EodExpiry'], dayfirst=True).dt.date
    yest_srspl_df = yest_srspl_df.query(
        "EodBroker not in ['CP','non CP'] and FinalNetQty != 0 and EodExpiry >= @for_dt"
    )
    yest_srspl_df['EodNetQuantity'] = yest_srspl_df['FinalNetQty']
    yest_srspl_df['PreFinalNetQty'] = yest_srspl_df['FinalNetQty']
    not_to_zero = ['EodBroker', 'EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry', 'EodNetQuantity']
    yest_srspl_df.loc[:, ~yest_srspl_df.columns.isin(not_to_zero)] = 0
    yest_srspl_df = yest_srspl_df[today_srspl_df.columns.tolist()]
    final_srspl_df = pd.concat([yest_srspl_df, today_srspl_df], ignore_index=True)
    final_srspl_df.fillna(0, inplace=True)
    final_srspl_df['EodExpiry'] = pd.to_datetime(final_srspl_df['EodExpiry'], dayfirst=True).dt.date
    grouped_srspl_df = final_srspl_df.groupby(
        by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
        as_index=False).agg(
        {'EodNetQuantity': 'last', 'buyQty': 'sum', 'buyValue': 'sum', 'sellQty': 'sum', 'sellValue': 'sum',
         'PreFinalNetQty': 'sum'}
    )
    grouped_srspl_df.fillna(0, inplace=True)
    grouped_final_eod = pd.concat([grouped_final_eod, grouped_srspl_df], ignore_index=True)
    grouped_final_eod['PreFinalNetQty'] = (grouped_final_eod['EodNetQuantity'] + grouped_final_eod['buyQty'] -
                                           grouped_final_eod['sellQty'])
    grouped_final_eod['EodExpiry'] = pd.to_datetime(grouped_final_eod['EodExpiry'], dayfirst=True).dt.date
    delta_df = calc_delta_v2(for_date=for_dt, eod_df=grouped_final_eod)
    # write_notis_postgredb(df=delta_df,table_name=f"NOTIS_DELTA_{for_dt}",truncate_required=True)
    delta_df.to_excel(os.path.join(test_dir, f'final_delta_{for_dt}.xlsx'), index=False)
    # ===============================================================================================================
    grouped_final_eod['ExpiredSpot_close'] = 0.0
    grouped_final_eod['ExpiredRate'] = 0.0
    grouped_final_eod['ExpiredAssn_value'] = 0.0
    grouped_final_eod['ExpiredSellValue'] = 0.0
    grouped_final_eod['ExpiredBuyValue'] = 0.0
    grouped_final_eod['ExpiredQty'] = 0.0
    if for_dt in grouped_final_eod['EodExpiry'].unique():
        grouped_final_eod = analyze_expired_instruments_v2(for_date=for_dt,grouped_final_eod=grouped_final_eod)
    grouped_final_eod['FinalNetQty'] = grouped_final_eod['PreFinalNetQty'] + grouped_final_eod['ExpiredQty']
    print(f'final_eod after calculation: {grouped_final_eod.shape}')
    # write_notis_postgredb(grouped_final_eod, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)
    return grouped_final_eod

for_dt = pd.to_datetime('07-10-2025', dayfirst=True).date()
for_dt_yest = for_dt - timedelta(days=1)
# for_dt_yest = pd.to_datetime('05-09-2025', dayfirst=True).date()
nse_pivot_df = get_nse_data()
# nse_pivot_df.to_excel(os.path.join(test_dir,f'nse_trade_{for_dt}_1.xlsx'), index=False)
bse_pivot_df = get_bse_data()
# bse_pivot_df.to_excel(os.path.join(test_dir,f'bse_trade_{for_dt}_1.xlsx'), index=False)
final_eod_df = find_eod(nse_df=nse_pivot_df,bse_df=bse_pivot_df)
final_eod_df.to_excel(os.path.join(test_dir,f'final_eod_{for_dt}_1.xlsx'), index=False)
# write_notis_postgredb(df=final_eod_df,table_name=f"NOTIS_EOD_NET_POS_CP_NONCP_{for_dt}",truncate_required=True)
p=0