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
from common import get_date_from_non_jiffy, read_data_db, read_notis_file, write_notis_data, today, yesterday, write_notis_postgredb, read_file
import warnings
import re


today = datetime(year=2025, month=3, day=5).date()
yesterday = datetime(year=2025, month=3, day=4).date()
# yesterday = today - timedelta(days=1)
# dd = datetime(year=2025, month=1, day=22).date()
pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')
# holidays_25 = ['2025-02-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14', '2025-04-18', '2025-05-01', '2025-08-15', '2025-08-27', '2025-10-02', '2025-10-21', '2025-10-22', '2025-11-05', '2025-12-25']
# # holidays_25.append('2024-03-20') #add unusual holidays
# # today = datetime.now().date()
# today = datetime(year=2025, month=1, day=21).date()
# b_days = pd.bdate_range(start=today-timedelta(days=7), end=today, freq='C', weekmask='1111100', holidays=holidays_25).date.tolist()
# # b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20)])) #add unusual trading days
#
# # yesterday = today-timedelta(days=1)
# # yesterday = datetime(year=2025, month=1, day=13).date()
# today, yesterday = sorted(b_days)[-1], sorted(b_days)[-2]

root_dir = os.path.dirname(os.path.abspath(__file__))
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
eod_test_dir = os.path.join(root_dir, 'eod_testing')
eod_input_dir = os.path.join(root_dir, 'eod_original')
eod_output_dir = os.path.join(root_dir, 'eod_data')
table_dir = os.path.join(root_dir, 'table_data')
bhav_path = os.path.join(root_dir, 'bhavcopy')
test_dir = os.path.join(root_dir, 'testing')
eod_net_pos_input_dir = os.path.join(root_dir, 'overall_net_position_input')
eod_net_pos_output_dir = os.path.join(root_dir, 'overall_net_position_output')


# eod_df = read_notis_file(os.path.join(eod_input_dir, f'EOD Position_{yesterday.strftime("%d_%b_%Y")}_1.xlsx'))
eod_df = read_file(os.path.join(eod_input_dir, f'EOD Position {yesterday.strftime("%d-%b-%Y")}.xlsx')) #EOD Position 28-Jan-2025
# # eod_df = read_notis_file(os.path.join(eod_dir, rf'NOTIS_DESK_WISE_FINAL_NET_POSITION_{yesterday.strftime("%Y-%m-%d")}_testing_1.xlsx'))
eod_df.columns = eod_df.columns.str.replace(' ', '')
eod_df.drop(columns=[col for col in eod_df.columns if col is None], inplace=True)
eod_df = eod_df.add_prefix('Eod')
# # eod_df = read_notis_file(rf"C:\Users\vipulanand\Downloads\Book1.xlsx")
eod_df.EodExpiry = eod_df.EodExpiry.astype('datetime64[ns]')
eod_df.EodExpiry = eod_df.EodExpiry.dt.date
eod_df.loc[eod_df['EodOptionType'] == 'XX', 'EodStrike'] = 0
eod_df.EodSettlementPrice = eod_df.EodSettlementPrice * 100
# eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
# eod_df = eod_df.iloc[:,1:]
grouped_eod = eod_df.groupby(by=['EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum','EodSettlementPrice':'mean'})
grouped_eod = grouped_eod.drop_duplicates()



tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
# tablenam = f'NOTIS_DESK_WISE_NET_POSITION'
desk_db_df = read_data_db(for_table=tablenam)
# # desk_db_df1 = read_file(os.path.join(table_dir, f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y_%m_%d")}.xlsx'))
desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# grouped_desk_db_df = desk_db_df.groupby(by=['mainGroup','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','sellAvgQty':'sum','volume':'sum'}).reset_index()
# grouped_desk_db_df = grouped_desk_db_df.drop_duplicates()
grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'})
# bhav_df = read_notis_file(os.path.join(bhav_path, rf'regularBhavcopy_{today.strftime("%d%m%Y")}.xlsx')) # regularBhavcopy_14012025.xlsx
# bhav_df.columns = bhav_df.columns.str.replace(' ', '')
# bhav_df.columns = bhav_df.columns.str.capitalize()
# bhav_df = bhav_df.add_prefix('Bhav')
# bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
# bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
# bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
# bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
# bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
# # desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
# # eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# # desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
# # desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# # desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# # desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
# # eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# # eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
# col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
# bhav_df = bhav_df[col_keep]
# # col_drop = ['BhavTotalValue','BhavOpenInterest','BhavChangeInOpenInterest']
# # bhav_df = bhav_df.drop(columns=[col for col in bhav_df.columns if col in col_drop])
# bhav_df = bhav_df.drop_duplicates()

# merged_df = grouped_desk_db_df.merge(eod_df, left_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], right_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
# merged_df = eod_df.merge(grouped_desk_db_df, right_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
merged_df = grouped_eod.merge(grouped_desk_db_df, left_on=['EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], right_on=["symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
merged_df.fillna(0, inplace=True)
merged_df = merged_df.drop_duplicates()

coltd1 = ['EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
coltd2 = ["symbol", "expiryDate", "strikePrice", "optionType"]
for i in range(len(coltd1)):
    merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
    merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
def find_avg_price(row):
    if row['IntradayVolume'] != 0:
        return abs(((row['buyAvgQty'] * row['buyAvgPrice']) - (row['sellAvgQty'] * row['sellAvgPrice'])))/(row['buyAvgQty'] + row['sellAvgQty'])
    else:
        return 0
# merged_df['IntradayNetAvgPrice'] = abs(((merged_df['buyAvgQty'] * merged_df['buyAvgPrice']) - (merged_df['sellAvgQty'] * merged_df['sellAvgPrice'])))/(merged_df['buyAvgQty']+merged_df['sellAvgQty'])
merged_df['IntradayNetAvgPrice'] = merged_df.apply(find_avg_price, axis=1)
merged_df.drop(columns = ['symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace = True)
a=0

# --------------------------------------------------------------------------------
# Orig
# if not os.path.exists(os.path.join(bhav_path, rf'regularBhavcopy_{dd.strftime("%d%m%Y")}.xlsx')):
#     raise FileNotFoundError(f'Bhav copy for date:{today} is missing.')
bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
bhav_matched_files = [f for f in os.listdir(bhav_path) if re.match(bhav_pattern, f)]
bhav_df = read_file(os.path.join(bhav_path, bhav_matched_files[0])) # regularBhavcopy_14012025.xlsx
bhav_df.columns = bhav_df.columns.str.replace(' ', '')
bhav_df.rename(columns={'VWAPclose':'closingPrice'}, inplace=True)
bhav_df.columns = bhav_df.columns.str.capitalize()
bhav_df = bhav_df.add_prefix('Bhav')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
# bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
bhav_df = bhav_df[col_keep]
bhav_df = bhav_df.drop_duplicates()
# --------------------------------------------------------------------------------
# # For contract master bhavcopy_fo ONLY FOR 10FEB2025
# bhav_df = read_file(rf"D:\notis_analysis\testing\regularBhavcopy_{today.strftime('%d%m%Y')}.csv")
# bhav_df.columns = bhav_df.columns.str.replace(' ','').str.capitalize()
# bhav_df = bhav_df.add_prefix('Bhav')
# col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
# bhav_df = bhav_df[col_keep]
# bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# bhav_df.BhavExpiry = pd.to_datetime(bhav_df.BhavExpiry).dt.date
# bhav_df.BhavOptiontype = bhav_df.BhavOptiontype.replace('NULL','XX')
# bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
# bhav_df.BhavClosingprice = bhav_df.BhavClosingprice.apply(lambda x: x*100 if x>0 else x)
# bhav_df = bhav_df.drop_duplicates()

b=0
merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
merged_bhav_df.drop(columns = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace = True)
c=0
# filtered_merged = merged_bhav_df.query("FinalNetQty != 0 and EodExpiry != @today")
filtered_merged = merged_bhav_df.query("EodExpiry != @today")
filtered_merged.drop(columns = ['IntradayNetAvgPrice'], inplace=True)
filtered_merged.rename(columns = {'BhavClosingprice':'FinalSettlementPrice'}, inplace = True)
# filtered_merged.columns = filtered_merged.columns.str.replace('Eod','')
# col_keep = ['Underlying', 'Expiry', 'Strike', 'OptionType','FinalNetQty','IntradayNetAvgPrice', 'BhavClosingprice']
# filtered_merged = filtered_merged[col_keep]
d=0
write_notis_data(filtered_merged, os.path.join(eod_net_pos_output_dir, f'final_overall_net_pos_{today.strftime("%d_%m_%Y")}.xlsx'))
e=0