import re, os, progressbar, pyodbc, warnings, psycopg2, time, mibian, scipy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from py_vollib.black_scholes.greeks.analytical import delta
from common import read_file, volt_dir, find_spot, holidays_25, holidays_26, read_data_db, yesterday, test_dir

today = pd.to_datetime('2025-07-30', dayfirst=True).date()
# yesterday = today - timedelta(days=1)
# a=find_spot()
# i=0
def calc_bus_dte(row):
    total_holidays = holidays_25 + holidays_26
    bdays_left = pd.bdate_range(start=today, end=row['EodExpiry'], freq='C', weekmask='1111100', holidays=total_holidays)
    actual_bdays_left = len(bdays_left)
    return actual_bdays_left
def get_delta(row):
    int_rate = 5.5
    annual_div = 0.0
    # if row['EodExpiry'] == pd.to_datetime('2025-07-31').date() and row['EodOptionType'] == 'CE':
    #     p=0
    spot = row['spot']
    strike = row['EodStrike']
    dte = row['dte']
    # dte = (row['EodExpiry'] - today).days
    vol = float(row['volatility'])
    if row['EodOptionType'] == 'XX':
        return 1.0
    calc = mibian.BS(
        [spot, strike, int_rate, dte],
        volatility=vol
    )
    return calc.callDelta if row['EodOptionType'] == 'CE' else calc.putDelta
    # if row['EodOptionType'] == 'CE':
    #     # calc = mibian.BS(
    #     #     [row['spot'],row['EodStrike'],int_rate,row['dte']],
    #     #     volatility=row['volatility']
    #     # )
    #     return calc.callDelta
    # else:
    #     # calc = mibian.BS(
    #     #     [row['spot'], row['EodStrike'],int_rate,row['dte']],
    #     #     volatility=row['volatility']
    #     # )
    #     return calc.putDelta
    # # else:
    # #     return 1.0

def get_greeks(row):
    int_rate = 5.5
    annual_div = 0
    spot = row['Spot']
    strike = row['Strike']
    dte = row['DTE']
    if row['OptionType'] == 'XX':
        iv = 1
        return [0,1,0,0,0,0]
    elif row['OptionType'] == 'CE':
        calc = mibian.Me([spot,strike,int_rate,annual_div,dte],callPrice=row['Price'])
    else:
        calc = mibian.Me([spot, strike, int_rate, annual_div, dte], putPrice=row['Price'])
    iv = calc.impliedVolatility
    greek_calc = mibian.Me([spot,strike,int_rate,annual_div,dte],volatility=iv)
    if row['OptionType'] == 'CE':
        return [iv,greek_calc.callDelta,greek_calc.callTheta, greek_calc.gamma, greek_calc.vega, greek_calc.callRho]
    else:
        return [iv,greek_calc.putDelta,greek_calc.putTheta,greek_calc.gamma, greek_calc.vega, greek_calc.putRho]

def get_delta_vollib(row):
    int_rate = 0.055
    spot= row['spot']
    strike = row['EodStrike']
    dte = ((row['EodExpiry']-today).days)/365
    vol = row['volatility'] / 100
    if row['EodOptionType'] == 'XX':
        return 1.0
    elif row['EodOptionType'] == 'CE':
        d = delta('c',spot,strike,dte,int_rate,vol)
        return d
    else:
        d = delta('p',spot,strike,dte,int_rate,vol)
        return d

def calc_delta(eod_df):
    eod_df = eod_df.copy()
    # eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True).dt.date
    sym_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX']
    col_keep = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType', 'PreFinalNetQty']
    eod_df.drop(columns=[col for col in eod_df.columns if col not in col_keep], inplace=True)
    volt_df = read_file(os.path.join(volt_dir, f'FOVOLT_{today.strftime("%d%m%Y")}.csv'))
    volt_df.columns = [re.sub(r'\s', '', each) for each in volt_df.columns]
    volt_df.rename(columns={'ApplicableAnnualisedVolatility(N)=Max(ForL)': 'AnnualizedReturn'}, inplace=True)
    volt_df = volt_df.iloc[:, [1, -1]].query("Symbol in @sym_list")
    volt_df = volt_df.applymap(lambda x:re.sub(r'\s+','',x) if isinstance(x,str) else x)
    volt_df['AnnualizedReturn'] = volt_df['AnnualizedReturn'].astype(np.float64)
    # volt_df = volt_df.reset_index()
    # spot_dict = find_spot()
    spot_dict = {
        'NIFTY':24855.05,
        'BANKNIFTY':56150.7,
        'SENSEX':81481.86
    }
    volt_dict = dict(zip(volt_df['Symbol'], volt_df['AnnualizedReturn']))
    eod_df['spot'] = eod_df['EodUnderlying'].map(spot_dict)
    eod_df['volatility'] = eod_df['EodUnderlying'].map(volt_dict)
    eod_df['volatility'] = eod_df['volatility'].astype(np.float64)
    eod_df['volatility'] = eod_df['volatility'] * 100
    # eod_df['dte'] = eod_df.apply(calc_dte, axis=1)
    # eod_df['cal_dte'] = (eod_df['EodExpiry'] - today).days
    eod_df['dte'] = eod_df['EodExpiry'].apply(lambda x: (x-today).days)
    mask = eod_df['EodOptionType'] == 'XX'
    eod_df.loc[mask, 'volatility'] = 1
    eod_df['deltaPerUnit'] = eod_df.apply(get_delta, axis=1).astype(np.float64)
    eod_df['deltaQty'] = (eod_df['PreFinalNetQty'] * eod_df['deltaPerUnit'])
    eod_df['deltaExposure(in Cr)'] = ((eod_df['spot'] * eod_df['deltaQty']) / 10_000_000)
    eod_df.to_excel(os.path.join(test_dir, f'eod_delta{datetime.today().strftime("%H%M")}.xlsx'), index=False)
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

def calc_greeks(trade_df):
    trade_df = trade_df.copy()
    trade_df.columns = [re.sub(r'\s', '', each) for each in trade_df.columns]
    trade_df = trade_df.applymap(lambda x: re.sub(r'\s+', '', x) if isinstance(x, str) else x)
    trade_df.rename(
        columns={'Type': 'OptionType', 'Maturity': 'Expiry', 'Bhav': 'Price'},
        inplace=True
    )
    trade_df.Strike = trade_df.Strike.astype(np.int64)
    trade_df['Expiry'] = pd.to_datetime(trade_df['Expiry'], dayfirst=True).dt.date
    to_float = ['Spot', 'Price']
    for each in to_float:
        trade_df[each] = trade_df[each].astype(np.float64)
    trade_df['OptionType'] = np.where(trade_df['OptionType'] == 'FUT', 'XX', trade_df['OptionType'])
    trade_df['DTE'] = trade_df['Expiry'].apply(lambda x: (x-today).days)
    trade_df[['IV','Delta','Theta','Gamma','Vega','Rho']] = trade_df.apply(get_greeks, axis=1, result_type='expand')
    return trade_df

# yest_eod_df = read_data_db(for_table=f"NOTIS_EOD_NET_POS_CP_NONCP_{today}")
orig_trade_df = pd.read_excel(rf"D:\notis_analysis\input_data\Position file 30-07-25.xlsx", index_col=False)
# delta_df = calc_delta(trade_df)
greek_df = calc_greeks(orig_trade_df)
greek_df.to_excel(os.path.join(test_dir, f'new_trade_greeks_{today}.xlsx'), index=False)