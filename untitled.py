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

today = datetime(year=2025, month=1, day=24).date()
yesterday = datetime(year=2025, month=1, day=23).date()
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
eod_net_pos_input_dir = os.path.join(root_dir, 'test_net_position_original')
eod_net_pos_output_dir = os.path.join(root_dir, 'test_net_position_code')


# # eod_df = read_notis_file(os.path.join(eod_input_dir, f'EOD Position_{yesterday.strftime("%d_%b_%Y")}_1.xlsx'))
# eod_df = read_notis_file(os.path.join(eod_net_pos_input_dir, f'net_position_eod_{yesterday.strftime("%d_%m_%Y")}.xlsx')) # net_position_eod_23_01_2025.xlsx
# # # eod_df = read_notis_file(os.path.join(eod_dir, rf'NOTIS_DESK_WISE_FINAL_NET_POSITION_{yesterday.strftime("%Y-%m-%d")}_testing_1.xlsx'))
# eod_df.columns = eod_df.columns.str.replace(' ', '')
# eod_df.drop(columns=[col for col in eod_df.columns if col is None], inplace=True)
# eod_df = eod_df.add_prefix('Eod')
# # # eod_df = read_notis_file(rf"C:\Users\vipulanand\Downloads\Book1.xlsx")
# eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# # eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# # eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
# # eod_df = eod_df.iloc[:,1:]
# grouped_eod = eod_df.groupby(by=['EodSymbol','EodExpiry','EodStrike','EodType']).agg({'EodEODQty':'sum'}).reset_index()
# grouped_eod = grouped_eod.drop_duplicates()
#
#
#
# tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
# desk_db_df = read_data_db(for_table=tablenam)
# # # desk_db_df1 = read_notis_file(os.path.join(table_dir, f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y_%m_%d")}.xlsx'))
# desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
# desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
# desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
# desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# # grouped_desk_db_df = desk_db_df.groupby(by=['mainGroup','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','sellAvgQty':'sum','volume':'sum'}).reset_index()
# # grouped_desk_db_df = grouped_desk_db_df.drop_duplicates()
# grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','sellAvgQty':'sum'}).reset_index()
# grouped_desk_db_df['IntradayNetQty'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
# grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'})
# # bhav_df = read_notis_file(os.path.join(bhav_path, rf'regularBhavcopy_{today.strftime("%d%m%Y")}.xlsx')) # regularBhavcopy_14012025.xlsx
# # bhav_df.columns = bhav_df.columns.str.replace(' ', '')
# # bhav_df.columns = bhav_df.columns.str.capitalize()
# # bhav_df = bhav_df.add_prefix('Bhav')
# # bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
# # bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
# # bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
# # bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# # bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
# # bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
# # # desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
# # # eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# # # desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
# # # desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# # # desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# # # desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
# # # eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# # # eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
# # col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
# # bhav_df = bhav_df[col_keep]
# # # col_drop = ['BhavTotalValue','BhavOpenInterest','BhavChangeInOpenInterest']
# # # bhav_df = bhav_df.drop(columns=[col for col in bhav_df.columns if col in col_drop])
# # bhav_df = bhav_df.drop_duplicates()
#
# # merged_df = grouped_desk_db_df.merge(eod_df, left_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], right_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
# # merged_df = eod_df.merge(grouped_desk_db_df, right_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
# merged_df = grouped_eod.merge(grouped_desk_db_df, right_on=["symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodSymbol', 'EodExpiry', 'EodStrike', 'EodType'], how='outer')
# merged_df.fillna(0, inplace=True)
# merged_df = merged_df.drop_duplicates()
#
# coltd1 = ['EodSymbol', 'EodStrike', 'EodType', 'EodExpiry']
# coltd2 = ['symbol', 'strikePrice', 'optionType', 'expiryDate']
# for i in range(len(coltd1)):
#     merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
#     merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
# merged_df['EodNetQty'] = merged_df['EodEODQty'] + merged_df['IntradayNetQty']
#
# a=0
# # merged_bhav_df = merged_df.merge(bhav_df, left_on=["symbol", "expiryDate", "strikePrice", "optionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
# # # for index, row in merged_bhav_df.iterrows():
# # #     if abs(row['volume']) > 0:
# # #         if (row['MTM'] > 0 and abs(row['MTM'])>abs(row['EodMTM'])) or (row['EodMTM'] > 0 and abs(row['EodMTM'])>abs(row['MTM'])):
# # #             sign = 1
# # #         else:
# # #             sign = -1
# # #         # merged_bhav_df.loc[index, 'NetAvgPrice'] = (abs(row['MTM']) + abs(
# # #         #     (row['BhavClosingprice'] * row['EodClosingQty']))) / (abs(row['volume']) + abs(row['EodClosingQty']))
# # #         merged_bhav_df.loc[index, 'NetAvgPrice'] = abs(row['NetQty']) / abs(row['BhavClosingprice'])
# # merged_bhav_df = merged_bhav_df.drop_duplicates()
# #
# # def find_expired_mtm(row):
# #     if row['expired'] == True: # ClosingQty=NetQty
# #             # if (row['MTM'] > 0 and abs(row['MTM'])>abs(row['EodMTM'])) or (row['EodMTM'] > 0 and abs(row['EodMTM'])>abs(row['MTM'])):
# #             #     sign = 1
# #             #     return row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice'])
# #             # else:
# #             #     sign = -1
# #             #     return -1*(row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice']))
# #             if row['NetQty'] != 0:
# #                 if row.EodOptionType == 'PE':
# #                     row.rate = max((row.EodStrike - row.Spot),0)
# #                 else:
# #                     row.rate = max((row.Spot - row.EodStrike),0)
# #                 if row.NetQty < 0:
# #                     row['BuyQty'] = -1*row.NetQty
# #                     row.BuyRate = row.rate
# #                     row.SellQty = 0
# #                     row.SellRate = 0
# #                 else:
# #                     row['BuyQty'] = 0
# #                     row.BuyRate = 0
# #                     row.SellQty = row.NetQty
# #                     row.SellRate = row.rate
# #                 row.BuyValue = row.BuyRate * row.BuyQty
# #                 row.SellValue = row.SellRate * row.SellQty
# #
# #             return (-1*row['MTM'])+row['EodMTM']+(row['NetQty']*row['BhavClosingprice'])
# #
# # merged_bhav_df.loc[merged_bhav_df['expiryDate'] == today, 'expired'] = True
# # merged_bhav_df['Spot'] = merged_bhav_df.apply(lambda row: row['BhavClosingprice'] if row['expiryDate'] == today else '', axis=1)
# # # merged_bhav_df['spot'] = np.where(merged_bhav_df['expiryDate'] == today, merged_bhav_df['BhavClosingprice'], merged_bhav_df['spot'])
# # # merged_bhav_df['NetAvgPrice'] = merged_bhav_df.apply(lambda row: abs(row['NetQty'])/abs(row['BhavClosingprice']) if abs(row['volume'])>0 else None, axis=1)
# # merged_bhav_df['expiredMTM'] = merged_bhav_df.apply(find_expired_mtm, axis=1)
# # # col_to_keep = desk_db_df.columns.tolist()+['EodLong', 'EodShort','EodClosingQty','EodClosingPrice','EodSubGroup','EodMainGroup', 'EodMTM', 'expired', 'NetQty','BhavClosingprice', 'NetAvgPrice', 'expiredMTM']
# # # merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_to_keep], axis=1, inplace=True)
# # # col_drop = ['EodMTM','mainGroup', 'account', 'brokerID', 'tokenNumber','MTM', 'symbol', 'expiryDate', 'strikePrice', 'optionType','BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype', 'NetAvgPrice']
# # # merged_bhav_df = merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col in col_drop])
# # merged_bhav_df['Long'] = merged_bhav_df['EodLong'] + merged_bhav_df['buyAvgQty']
# # merged_bhav_df['Short'] = merged_bhav_df['EodShort'] + merged_bhav_df['sellAvgQty']
# # # col_keep = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry','EodSubGroup', 'EodMainGroup', 'Long','Short','NetQty','BhavClosingprice']
# # # merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)
# # # merged_bhav_df.columns = merged_bhav_df.columns.str.replace('Eod','')
# # merged_bhav_df.rename(columns={'NetQty':'ClosingQty','BhavClosingprice':'ClosingPrice'}, inplace=True )
# # # merged_bhav_df = merged_bhav_df[['Underlying', 'Strike', 'OptionType', 'Expiry', 'Long', 'Short', 'ClosingQty', 'ClosingPrice', 'SubGroup', 'MainGroup']]
# # merged_bhav_df = merged_bhav_df.drop_duplicates()
# # merged_bhav_df.rename(columns={'buyAvgQty':'BuyQty','sellAvgQty':'SellQty'}, inplace=True )
# # merged_bhav_df.drop(columns=['EodMTM','mainGroup','symbol', 'expiryDate', 'strikePrice', 'optionType','BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','expired', 'expiredMTM', 'Long', 'Short'], axis=1, inplace=True)
# # # # merged_df.drop(columns=eod_df.columns.tolist(), axis=1, inplace=True)
# # # merged_bhav_df['Long'] = merged_bhav_df['buyAvgQty'] + merged_bhav_df['EodLong']
# # # merged_bhav_df['Short'] = merged_bhav_df['sellAvgQty'] + merged_bhav_df['EodShort']
# # # col_keep = ['symbol', 'strikePrice', 'optionType', 'expiryDate', 'Long', 'Short', 'NetQty', 'BhavClosingprice', 'EodSubGroup', 'mainGroup']
# # # merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)
# # # merged_bhav_df.rename(columns={'symbol':'Underlying', 'strikePrice':'Strike', 'optionType':'OptionType', 'NetQty':'ClosingQty', 'BhavClosingprice':'ClosingPrice', 'EodSubGroup':'SubGroup', 'mainGroup':'mainGroup'})
# # # merged_bhav_df = merged_bhav_df[col_keep]
# # a=0
# # def update_qty(row):
# #     if row.Long > row.Short:
# #         row.Long = row.ClosingQty
# #         row.Short = 0
# #     elif row.Long < row.Short:
# #         row.Short = row.ClosingQty
# #         row.Long = 0
# #     return row
# # # merged_bhav_df = merged_bhav_df.apply(update_qty, axis=1)
# # write_notis_data(merged_bhav_df, os.path.join(eod_output_dir, f'Eod_{today.strftime("%Y_%m_%d")}_test_1.xlsx'))
# # write_notis_postgredb(merged_bhav_df, table_name=n_tbl_notis_net_position, raw=False)
# # print(f'file made for {today}')
# # # write_notis_data(desk_db_df, f'desk_{today.strftime("%Y-%m-%d")}.xlsx')
# # # write_notis_data(eod_df, f'eod_{today.strftime("%Y-%m-%d")}.xlsx')
# # # write_notis_data(bhav_df, f'bhav_{today.strftime("%Y-%m-%d")}.xlsx')
# # # print(eod_df.head(),'\n',desk_db_df.head())
# filtered_merged = merged_df.query("EodEODQty != 0 or IntradayNetQty != 0")
# write_notis_data(filtered_merged, os.path.join(eod_net_pos_output_dir, f'test_net_pos_{today.strftime("%d_%m_%y")}.xlsx'))
# b=0
#
# # pbar = progressbar.ProgressBar(max_value=10, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
# # pbar.update(1)
# # for i in range(10):
# #     time.sleep(1)
# #     pbar.update(i + 1)
# # pbar.finish()

# n_tbl_notis_trade_book = "NOTIS_TRADE_BOOK"
# # n_tbl_notis_trade_book = "NOTIS_DESK_WISE_EOD_POSITION_2025-01-21"
# df = read_notis_file(rf"D:\notis_analysis\modified_data\NOTIS_DATA_29JAN2025.xlsx")
# # df = read_notis_file(rf"D:\notis_analysis\table_data\NOTIS_DESK_WISE_NET_POSITION_2025_01_20_2.xlsx")
# # df = read_notis_file(rf"D:\notis_analysis\eod_data\Eod_2025_01_21_test_2.xlsx")
# a=0
# write_notis_postgredb(df, n_tbl_notis_trade_book)
#
# # grouped_a = a.groupby(by=['MainGroup','SubGroup','sym','expDt', 'strPrc', 'optType','bsFlg'], as_index=False).agg({'trdQty':'sum', 'trdPrc':'mean'})


# bhav_df = read_notis_file(rf"D:\notis_analysis\testing\regularBhavcopy_10022025.xlsx")
# bhav_df1 = read_file(rf"D:\notis_analysis\testing\regularBhavcopy_10022025.xlsx")
# bhav_df2 = read_file(rf"D:\notis_analysis\testing\regularBhavcopy_10022025.csv")
a=0
ex_dt = '11-02-2025  06:03:30'
ex = datetime(2025, 2, 12, 9, 36, 50, microsecond=297695)
epoch_time = int(time.mktime(ex.timetuple()))
print(epoch_time)
b=0