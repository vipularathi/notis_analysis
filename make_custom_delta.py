import os, requests, io, base64, warnings, re
from datetime import datetime,date,time,timedelta
import pandas as pd
import numpy as np
import mibian
from common import (volt_dir,table_dir,
                    logger,
                    read_data_db, write_notis_postgredb, read_file)
from db_config import n_tbl_notis_delta_table,n_tbl_notis_eod_net_pos_cp_noncp, n_tbl_spot_data

warnings.filterwarnings('ignore')
# today = pd.to_datetime('02-09-2025', dayfirst=True).date()
# yesterday = today - timedelta(days=1)

# if running on a random day, spot of that random day
# spot_dict = {
#     'NIFTY':24579.6,
#     'BANKNIFTY':53661,
#     'SENSEX':80157.88
# }
# if running before the start of next day
# spot_df = read_data_db(for_table=n_tbl_spot_data)
# spot_dict = spot_df.to_dict(orient='records')[0]

def get_delta(row):
    int_rate,annual_div = 5.5,0
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
    delta_df = eod_df.copy()
    delta_df['EodExpiry'] = pd.to_datetime(delta_df['EodExpiry'], dayfirst=True).dt.date
    sym_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
    col_keep = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType', 'PreFinalNetQty']
    delta_df.drop(columns=[col for col in delta_df.columns if col not in col_keep], inplace=True)
    delta_df = delta_df.query("EodExpiry != @today")
    volt_df = read_file(os.path.join(volt_dir, f'FOVOLT_{today.strftime("%d%m%Y")}.csv'))
    volt_df.columns = [re.sub(r'\s', '', each) for each in volt_df.columns]
    volt_df.rename(columns={'ApplicableAnnualisedVolatility(N)=Max(ForL)': 'AnnualizedReturn'}, inplace=True)
    volt_df = volt_df.iloc[:, [1, -1]].query("Symbol in @sym_list")
    volt_df = volt_df.applymap(lambda x: re.sub(r'\s+', '', x) if isinstance(x, str) else x)
    volt_df['AnnualizedReturn'] = volt_df['AnnualizedReturn'].astype(np.float64)
    volt_dict = dict(zip(volt_df['Symbol'], volt_df['AnnualizedReturn']))
    delta_df['spot'] = delta_df['EodUnderlying'].map(spot_dict)
    delta_df['volatility'] = delta_df['EodUnderlying'].map(volt_dict)
    delta_df['volatility'] = delta_df['volatility'].astype(np.float64)
    delta_df['volatility'] = delta_df['volatility'] * 100
    delta_df['dte'] = delta_df['EodExpiry'].apply(lambda x: (x - today).days)
    mask = delta_df['EodExpiry'] == today
    delta_df.loc[mask, 'dte'] = 1
    mask = delta_df['EodOptionType'] == 'XX'
    delta_df.loc[mask, 'volatility'] = 1
    delta_df['deltaPerUnit'] = delta_df.apply(get_delta, axis=1).astype(np.float64)
    delta_df['deltaQty'] = (delta_df['PreFinalNetQty'] * delta_df['deltaPerUnit'])
    delta_df['deltaExposure(in Cr)'] = (delta_df['spot'] * delta_df['deltaQty']) / 10_000_000
    delta_df.to_excel(os.path.join(table_dir, f'pre_delta_{today}.xlsx'), index=False)
    delta_df1 = delta_df.copy()
    final_delta_df = pd.DataFrame()
    mask = delta_df1['EodOptionType'].isin(['CE', 'PE'])
    delta_df1.loc[mask, 'EodOptionType'] = 'CE_PE'
    for each in ['XX', 'CE_PE']:
        temp_delta_df1 = delta_df1.query("EodOptionType == @each")
        grouped_temp_delta_df1 = \
            temp_delta_df1.groupby(by=['EodOptionType', 'EodBroker', 'EodUnderlying'], as_index=False)[
                'deltaExposure(in Cr)'].agg(
                {'Long': lambda x: x[x > 0].sum(), 'Short': lambda x: x[x < 0].sum(), 'Net': 'sum'}
            )
        total_dict = {
            'EodOptionType': each,
            'EodBroker': 'Total',
            'Long': grouped_temp_delta_df1['Long'].sum(),
            'Short': grouped_temp_delta_df1['Short'].sum(),
            'Net': grouped_temp_delta_df1['Net'].sum()
        }
        grouped_temp_delta_df1 = pd.concat([grouped_temp_delta_df1, pd.DataFrame([total_dict])], ignore_index=True)
        final_delta_df = pd.concat([final_delta_df, grouped_temp_delta_df1], ignore_index=True)
    for each in ['deltaExposure(in Cr)', 'deltaQty']:
        grouped_df = delta_df1.groupby(by=['EodBroker', 'EodUnderlying'], as_index=False)[each].agg(
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
            'Long': grouped_df['Long'].sum(),
            'Short': grouped_df['Short'].sum(),
            'Net': grouped_df['Net'].sum()
        }
        grouped_df = pd.concat([grouped_df, pd.DataFrame([total_dict])], ignore_index=False)
        final_delta_df = pd.concat([final_delta_df, grouped_df], ignore_index=False)
    delta_df2 = delta_df.copy()
    for each in ['deltaExposure(in Cr)', 'deltaQty']:
        grouped_df = delta_df2.groupby(by=['EodUnderlying'], as_index=False)[each].agg(
            {'Long': lambda x: x[x > 0].sum(), 'Short': lambda x: x[x < 0].sum(), 'Net': 'sum'}
        )
        if each == 'deltaExposure(in Cr)':
            use = 'Underlying Combined'
        else:
            use = 'Underlying DeltaQty'
            grouped_df['Long'] = grouped_df['Long'] / 100000
            grouped_df['Short'] = grouped_df['Short'] / 100000
            grouped_df['Net'] = grouped_df['Net'] / 100000
        grouped_df['EodOptionType'] = use
        total_dict = {
            'EodOptionType': use,
            'EodBroker': 'Total',
            'Long': grouped_df['Long'].sum(),
            'Short': grouped_df['Short'].sum(),
            'Net': grouped_df['Net'].sum()
        }
        grouped_df = pd.concat([grouped_df, pd.DataFrame([total_dict])], ignore_index=True)
        final_delta_df = pd.concat([final_delta_df, grouped_df], ignore_index=True)
    return final_delta_df

eod_df = read_data_db(for_table=f"NOTIS_EOD_NET_POS_CP_NONCP_{today}")
eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True).dt.date
delta_df = calc_delta(eod_df)
logger.info(f"New delta table made with volatility file of {today}")
write_notis_postgredb(df=delta_df,table_name=f"NOTIS_DELTA_{today}",truncate_required=True)
delta_df.to_excel(os.path.join(table_dir, f'custom_delta_{today}_final.xlsx'), index=False)