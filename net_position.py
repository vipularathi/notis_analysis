import numpy as np
import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from sqlalchemy import create_engine
from common import get_date_from_non_jiffy, read_data_db, read_notis_file, write_notis_data, today, yesterday, write_notis_postgredb
import warnings
from db_config import n_tbl_notis_desk_wise_net_position

# def find_expired_mtm(row):
#     if row['expired'] == True: # ClosingQty=NetQty
#         # if (row['MTM'] > 0 and abs(row['MTM'])>abs(row['EodMTM'])) or (row['EodMTM'] > 0 and abs(row['EodMTM'])>abs(row['MTM'])):
#         #     sign = 1
#         #     return row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice'])
#         # else:
#         #     sign = -1
#         #     return -1*(row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice']))
#         if row['NetQty'] != 0:
#             if row['EodOptionType'] == 'PE':
#                 rate = max((row['EodStrike'] - row['Spot']),0)
#             else:
#                 rate = max((row['Spot'] - row['EodStrike']),0)
#             if row['NetQty'] < 0:
#                 BuyQty = -1*row['NetQty']
#                 BuyRate= rate
#                 SellQty = 0
#                 SellRate = 0
#             else:
#                 BuyQty = 0
#                 BuyRate = 0
#                 SellQty = row['NetQty']
#                 SellRate = rate
#             BuyValue = BuyRate * BuyQty
#             SellValue = SellRate * SellQty
#             return pd.Series({
#                     'ExpRate': rate,
#                     'ExpBuyQty': BuyQty,
#                     'ExpBuyRate': BuyRate,
#                     'ExpSellQty': SellQty,
#                     'ExpSellRate': SellRate,
#                     'ExpBuyValue': BuyValue,
#                     'ExpSellValue': SellValue,
#                 })
#     return pd.Series({
#         'ExpRate': None,
#         'ExpBuyQty': None,
#         'ExpBuyRate': None,
#         'ExpSellQty': None,
#         'ExpSellRate': None,
#         'ExpBuyValue': None,
#         'ExpSellValue': None,
#     })

def find_expired_mtm(row):
    if row['expired'] == True:  # ClosingQty = NetQty
        if row['ClosingQty'] != 0:
            # Calculate 'ExpRate' based on 'EodOptionType'
            if row['EodOptionType'] == 'PE':
                ExpRate = max((row['EodStrike'] - row['Spot']), 0)
            else:
                ExpRate = max((row['Spot'] - row['EodStrike']), 0)

            # Initialize Buy and Sell quantities and rates
            if row['ClosingQty'] < 0:
                ExpBuyQty = -1 * row['ClosingQty']
                ExpBuyRate = ExpRate
                ExpSellQty = 0
                ExpSellRate = 0
            else:
                ExpBuyQty = 0
                ExpBuyRate = 0
                ExpSellQty = row['ClosingQty']
                ExpSellRate = ExpRate

            # Calculate Buy and Sell Values
            ExpBuyValue = ExpBuyRate * ExpBuyQty
            ExpSellValue = ExpSellRate * ExpSellQty

            # Return computed values as a Series
            return pd.Series({
                'ExpRate': ExpRate,
                'ExpBuyQty': ExpBuyQty,
                'ExpBuyRate': ExpBuyRate,
                'ExpSellQty': ExpSellQty,
                'ExpSellRate': ExpSellRate,
                'ExpBuyValue': ExpBuyValue,
                'ExpSellValue': ExpSellValue
            })

    # If condition doesn't match, return NaN for all new columns
    return pd.Series({
        'ExpRate': None,
        'ExpBuyQty': None,
        'ExpBuyRate': None,
        'ExpSellQty': None,
        'ExpSellRate': None,
        'ExpBuyValue': None,
        'ExpSellValue': None
    })


# def find_expired_mtm(row):
#     if row['expired'] == True:  # ClosingQty=NetQty
#         if row['NetQty'] != 0:
#             if row['EodOptionType'] == 'PE':
#                 row['rate'] = max((row['EodStrike'] - row['Spot']), 0)
#             else:
#                 row['rate'] = max((row['Spot'] - row['EodStrike']), 0)
#             if row['NetQty'] < 0:
#                 row['BuyQty'] = -1 * row['NetQty']
#                 row['BuyRate'] = row['rate']
#                 row['SellQty'] = 0
#                 row['SellRate'] = 0
#             else:
#                 row['BuyQty'] = 0
#                 row['BuyRate'] = 0
#                 row['SellQty'] = row['NetQty']
#                 row['SellRate'] = row['rate']
#             row['BuyValue'] = row['BuyRate'] * row['BuyQty']
#             row['SellValue'] = row['SellRate'] * row['SellQty']
#             return row

def find_intraday_pnl(row):
    if row['ClosingQty'] == 0:
        # row['IntradayPnL'] = (row['EodShort']*row['EodClosingPrice'] + row['SellQty']*row['sellAvgPrice']) - (row['EodLong']*row['EodClosingQty'] + row['BuyQty']*row['buyAvgPrice'])
        IntradayPnL = (row['EodShort']*row['EodClosingPrice'] + row['SellQty']*row['sellAvgPrice']) - (row['EodLong']*row['EodClosingPrice'] + row['BuyQty']*row['buyAvgPrice'])
        return IntradayPnL

def update_qty(row):
    if row.Long > row.Short:
        row.Long = row.ClosingQty
        row.Short = 0
    elif row.Long < row.Short:
        row.Short = abs(row.ClosingQty)
        row.Long = 0
    return row

def find_spot(row):
    if row['expired']:
        return bhav_lookup.get((row['EodUnderlying'], row['EodExpiry']), None)
    return None

today = datetime(year=2025, month=1, day=23).date()
yesterday = datetime(year=2025, month=1, day=22).date()
pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

n_tbl_notis_desk_wise_net_position = f"NOTIS_DESK_WISE_EOD_POSITION_{today.strftime('%Y-%m-%d')}"

root_dir = os.path.dirname(os.path.abspath(__file__))
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
eod_test_dir = os.path.join(root_dir, 'eod_testing')
eod_input_dir = os.path.join(root_dir, 'eod_original')
eod_output_dir = os.path.join(root_dir, 'eod_data')
table_dir = os.path.join(root_dir, 'table_data')
bhav_path = os.path.join(root_dir, 'bhavcopy')
test_dir = os.path.join(root_dir, 'testing')


if not os.path.exists(os.path.join(eod_input_dir, f'EOD Position_{yesterday.strftime("%d_%b_%Y")}_2.xlsx')):
    raise FileNotFoundError(f"Missing yedterday\'s EOD file.")
eod_df = read_notis_file(os.path.join(eod_input_dir, f'EOD Position_{yesterday.strftime("%d_%b_%Y")}_2.xlsx'))
# eod_table_name = f"NOTIS_DESK_WISE_EOD_POSITION_{yesterday.strftime('%Y-%m-%d')}"
# eod_df = read_data_db(for_table=eod_table_name)
q=0
# eod_df = read_notis_file(os.path.join(eod_dir, rf'NOTIS_DESK_WISE_FINAL_NET_POSITION_{yesterday.strftime("%Y-%m-%d")}_testing_1.xlsx'))
eod_df.columns = eod_df.columns.str.replace(' ', '')
eod_df = eod_df.add_prefix('Eod')
# eod_df = read_notis_file(rf"C:\Users\vipulanand\Downloads\Book1.xlsx")
eod_df.EodExpiry = eod_df.EodExpiry.dt.date
eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
eod_df.EodClosingPrice = eod_df.EodClosingPrice * 100
# eod_df = eod_df.query("ClosingQty != 0 and expired.isnull() == True")
# col_keep = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry','EodSubGroup', 'EodMainGroup','Long', 'Short','ClosingQty', 'ClosingPrice']
# eod_df = eod_df[col_keep]
# eod_df.columns = eod_df.columns.str.replace('Eod','')
# eod_df.Expiry = eod_df.Expiry.astype('datetime64[ns]')
# eod_df.Expiry = eod_df.Expiry.dt.date
# eod_df.Strike = eod_df.Strike.astype('int64')
# eod_df = eod_df.add_prefix('Eod')
eod_df = eod_df.drop_duplicates()


tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
desk_db_df = read_data_db(for_table=tablenam)
# desk_db_df1 = read_notis_file(os.path.join(table_dir, f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y_%m_%d")}.xlsx'))
desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
grouped_desk_db_df = desk_db_df.groupby(by=['mainGroup','subGroup','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'sum','sellAvgQty':'sum','sellAvgPrice':'sum','volume':'sum'}).reset_index()
grouped_desk_db_df = grouped_desk_db_df.drop_duplicates()


if not os.path.exists(os.path.join(bhav_path, rf'regularBhavcopy_{today.strftime("%d%m%Y")}.xlsx')):
    raise FileNotFoundError(f'Bhav copy for date:{today} is missing.')
bhav_df = read_notis_file(os.path.join(bhav_path, rf'regularBhavcopy_{today.strftime("%d%m%Y")}.xlsx')) # regularBhavcopy_14012025.xlsx
bhav_df.columns = bhav_df.columns.str.replace(' ', '')
bhav_df.columns = bhav_df.columns.str.capitalize()
bhav_df = bhav_df.add_prefix('Bhav')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
bhav_df = bhav_df[col_keep]
bhav_df = bhav_df.drop_duplicates()


merged_df = eod_df.merge(grouped_desk_db_df, right_on=["mainGroup","subGroup", "symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodMainGroup','EodSubGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
merged_df.fillna(0, inplace=True)
merged_df = merged_df.drop_duplicates()
coltd1 = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry', 'EodMainGroup','EodSubGroup']
coltd2 = ['symbol', 'strikePrice', 'optionType', 'expiryDate', 'mainGroup','subGroup']
for i in range(len(coltd1)):
    merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
    merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
merged_df['NetQty'] = merged_df['EodClosingQty'] + merged_df['volume']


merged_bhav_df = merged_df.merge(bhav_df, left_on=["symbol", "expiryDate", "strikePrice", "optionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
merged_bhav_df = merged_bhav_df.drop_duplicates()
merged_bhav_df['Long'] = merged_bhav_df['EodLong'] + merged_bhav_df['buyAvgQty']
merged_bhav_df['Short'] = merged_bhav_df['EodShort'] + merged_bhav_df['sellAvgQty']
merged_bhav_df.rename(columns={'NetQty':'ClosingQty','BhavClosingprice':'ClosingPrice','buyAvgQty':'BuyQty','sellAvgQty':'SellQty'}, inplace=True )
merged_bhav_df = merged_bhav_df.apply(update_qty, axis=1)
# merged_bhav_df = merged_bhav_df.apply(find_intraday_pnl, axis=1)
merged_bhav_df['IntradayPnL'] = merged_bhav_df.apply(find_intraday_pnl, axis=1)
# --------------------------------------------------------------------------------
# for expired
merged_bhav_df.loc[merged_bhav_df['expiryDate'] == today, 'expired'] = True
# merged_bhav_df['Spot'] = merged_bhav_df.apply(lambda row: row['ClosingPrice'] if row['expiryDate'] == today else '', axis=1)
bhav_lookup = bhav_df[bhav_df["BhavOptionType"] == 'XX'].set_index(["BhavSymbol", "BhavExpiry"])['BhavClosingprice'].to_dict()
merged_bhav_df['Spot'] = merged_bhav_df.apply(find_spot, axis=1)
exp_cols = merged_bhav_df.apply(find_expired_mtm, axis=1)
merged_bhav_df = pd.concat([merged_bhav_df,exp_cols], axis=1)

# # Initialize new columns with default values
# merged_bhav_df['rate'] = None
# merged_bhav_df['BuyQty'] = None
# merged_bhav_df['BuyRate'] = None
# merged_bhav_df['SellQty'] = None
# merged_bhav_df['SellRate'] = None
# merged_bhav_df['BuyValue'] = None
# merged_bhav_df['SellValue'] = None
#
# mask = (merged_bhav_df['expired'] == True) & (merged_bhav_df['NetQty'] != 0)
# merged_bhav_df.loc[mask & (merged_bhav_df['EodOptionType'] == 'PE'), 'rate'] = (
#     (merged_bhav_df['EodStrike'] - merged_bhav_df['Spot']).clip(lower=0)
# )
# merged_bhav_df.loc[mask & (merged_bhav_df['EodOptionType'] != 'PE'), 'rate'] = (
#     (merged_bhav_df['Spot'] - merged_bhav_df['EodStrike']).clip(lower=0)
# )
# merged_bhav_df.loc[mask & (merged_bhav_df['NetQty'] < 0), 'BuyQty'] = -merged_bhav_df['NetQty']
# merged_bhav_df.loc[mask & (merged_bhav_df['NetQty'] < 0), 'BuyRate'] = merged_bhav_df['rate']
# merged_bhav_df.loc[mask & (merged_bhav_df['NetQty'] > 0), 'SellQty'] = merged_bhav_df['NetQty']
# merged_bhav_df.loc[mask & (merged_bhav_df['NetQty'] > 0), 'SellRate'] = merged_bhav_df['rate']
# merged_bhav_df['BuyValue'] = merged_bhav_df['BuyQty'] * merged_bhav_df['BuyRate']
# merged_bhav_df['SellValue'] = merged_bhav_df['SellQty'] * merged_bhav_df['SellRate']

# --------------------------------------------------------------------------------
a=0
# merged_bhav_df['spot'] = np.where(merged_bhav_df['expiryDate'] == today, merged_bhav_df['BhavClosingprice'], merged_bhav_df['spot'])
# merged_bhav_df['NetAvgPrice'] = merged_bhav_df.apply(lambda row: abs(row['NetQty'])/abs(row['BhavClosingprice']) if abs(row['volume'])>0 else None, axis=1)
# col_to_keep = desk_db_df.columns.tolist()+['EodLong', 'EodShort','EodClosingQty','EodClosingPrice','EodSubGroup','EodMainGroup', 'EodMTM', 'expired', 'NetQty','BhavClosingprice', 'NetAvgPrice', 'expiredMTM']
# merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_to_keep], axis=1, inplace=True)
col_drop = ['mainGroup', 'subGroup', 'symbol', 'expiryDate', 'strikePrice', 'optionType','BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype']
merged_bhav_df = merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col in col_drop])
# merged_bhav_df.columns = merged_bhav_df.columns.str.replace('Eod','')

# col_keep = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry','EodSubGroup', 'EodMainGroup', 'Long','Short','NetQty','BhavClosingprice']
# merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)

# merged_bhav_df = merged_bhav_df[['Underlying', 'Strike', 'OptionType', 'Expiry', 'Long', 'Short', 'ClosingQty', 'ClosingPrice', 'SubGroup', 'MainGroup']]


# merged_bhav_df.drop(columns=['EodMTM','mainGroup','symbol', 'expiryDate', 'strikePrice', 'optionType','BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','expired', 'expiredMTM', 'Long', 'Short'], axis=1, inplace=True)
# # merged_df.drop(columns=eod_df.columns.tolist(), axis=1, inplace=True)
# merged_bhav_df['Long'] = merged_bhav_df['buyAvgQty'] + merged_bhav_df['EodLong']
# merged_bhav_df['Short'] = merged_bhav_df['sellAvgQty'] + merged_bhav_df['EodShort']
# col_keep = ['symbol', 'strikePrice', 'optionType', 'expiryDate', 'Long', 'Short', 'NetQty', 'BhavClosingprice', 'EodSubGroup', 'mainGroup']
# merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)
# merged_bhav_df.rename(columns={'symbol':'Underlying', 'strikePrice':'Strike', 'optionType':'OptionType', 'NetQty':'ClosingQty', 'BhavClosingprice':'ClosingPrice', 'EodSubGroup':'SubGroup', 'mainGroup':'mainGroup'})
# merged_bhav_df = merged_bhav_df[col_keep]
# drop the columns
# make changes to db schema
b=0
write_notis_data(merged_bhav_df, os.path.join(eod_output_dir, f'Eod_{today.strftime("%Y_%m_%d")}_test_2.xlsx'))
# merged_bhav_df.fillna(value=None, inplace=True)
# merged_bhav_df.replace('',None, inplace=True)
# write_notis_postgredb(merged_bhav_df, table_name=n_tbl_notis_desk_wise_net_position)
print(f'file made for {today}')
# write_notis_data(desk_db_df, f'desk_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(eod_df, f'eod_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(bhav_df, f'bhav_{today.strftime("%Y-%m-%d")}.xlsx')
# print(eod_df.head(),'\n',desk_db_df.head())
c=0

# send 20th eod file as desk 3 would not match cause subgroup is being added
# in the table from 21 and without subgroup the desk 3 data would not match