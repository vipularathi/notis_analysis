import numpy as np
import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone, date
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from urllib.parse import quote
import pytz
import gzip
import xlsxwriter
from common import get_date_from_non_jiffy, get_date_from_jiffy, read_data_db, read_notis_file, write_notis_data, today, yesterday, write_notis_postgredb, read_file, engine_str
import warnings
from fastapi import FastAPI, Query, status, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from db_config import n_tbl_notis_trade_book, s_tbl_notis_trade_book, n_tbl_notis_raw_data, s_tbl_notis_raw_data, n_tbl_notis_nnf_data, s_tbl_notis_nnf_data
from main import modify_file
import openpyxl
import io
import gzip
import progressbar
import csv
import sys

# today = datetime(year=2025, month=1, day=24).date()
# yesterday = datetime(year=2025, month=1, day=23).date()
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
zipped_dir=os.path.join(root_dir, 'zipped_files')
dir_list = [zipped_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

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
# modified_df = read_data_db(for_table=tablenam)
# # # modified_df1 = read_notis_file(os.path.join(table_dir, f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y_%m_%d")}.xlsx'))
# modified_df.expiryDate = modified_df.expiryDate.astype('datetime64[ns]')
# modified_df.expiryDate = modified_df.expiryDate.dt.date
# modified_df.loc[modified_df['optionType'] == 'XX', 'strikePrice'] = 0
# modified_df.strikePrice = modified_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# modified_df.strikePrice = modified_df.strikePrice.astype('int64')
# # grouped_modified_df = modified_df.groupby(by=['mainGroup','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','sellAvgQty':'sum','volume':'sum'}).reset_index()
# # grouped_modified_df = grouped_modified_df.drop_duplicates()
# grouped_modified_df = modified_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','sellAvgQty':'sum'}).reset_index()
# grouped_modified_df['IntradayNetQty'] = grouped_modified_df['buyAvgQty'] - grouped_modified_df['sellAvgQty']
# grouped_modified_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'})
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
# # # modified_df.expiryDate = modified_df.expiryDate.astype('datetime64[ns]')
# # # eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# # # modified_df.expiryDate = modified_df.expiryDate.dt.date
# # # modified_df.strikePrice = modified_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# # # modified_df.strikePrice = modified_df.strikePrice.astype('int64')
# # # modified_df.loc[modified_df['optionType'] == 'XX', 'strikePrice'] = 0
# # # eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# # # eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
# # col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
# # bhav_df = bhav_df[col_keep]
# # # col_drop = ['BhavTotalValue','BhavOpenInterest','BhavChangeInOpenInterest']
# # # bhav_df = bhav_df.drop(columns=[col for col in bhav_df.columns if col in col_drop])
# # bhav_df = bhav_df.drop_duplicates()
#
# # merged_df = grouped_modified_df.merge(eod_df, left_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], right_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
# # merged_df = eod_df.merge(grouped_modified_df, right_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
# merged_df = grouped_eod.merge(grouped_modified_df, right_on=["symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodSymbol', 'EodExpiry', 'EodStrike', 'EodType'], how='outer')
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
# # # col_to_keep = modified_df.columns.tolist()+['EodLong', 'EodShort','EodClosingQty','EodClosingPrice','EodSubGroup','EodMainGroup', 'EodMTM', 'expired', 'NetQty','BhavClosingprice', 'NetAvgPrice', 'expiredMTM']
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
# # # write_notis_data(modified_df, f'desk_{today.strftime("%Y-%m-%d")}.xlsx')
# # # write_notis_data(eod_df, f'eod_{today.strftime("%Y-%m-%d")}.xlsx')
# # # write_notis_data(bhav_df, f'bhav_{today.strftime("%Y-%m-%d")}.xlsx')
# # # print(eod_df.head(),'\n',modified_df.head())
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
# ex_dt = '11-02-2025  06:03:30'
# ex = datetime(2025, 2, 12, 9, 36, 50, microsecond=297695)
# epoch_time = int(time.mktime(ex.timetuple()))
# print(epoch_time)
b=0
# class ServiceApp:
#     def __init__(self):
#         super().__init__()
#         self.app = FastAPI(title='NOTIS_Net_Position', description='Notis_net_position', docs_url='/docs', openapi_url='/openapi.json')
#         self.app.add_middleware(CORSMiddleware, allow_origins = ['*'], allow_credentials = True, allow_methods=['*'], allow_headers=['*'])
#         self.add_routes()
#
#     def add_routes(self):
#         self.app.add_api_route('/netPosition/intraday', methods=['GET'], endpoint=self.get_intraday_net_position)
#
#     def get_intraday_net_position(self):
#         # df_db = read_data_db()
#         # nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
#         # if not os.path.exists(nnf_file_path):
#         #     raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")
#         # readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
#         # if readable_mod_time.date() == today:  # Check if the NNF file is modified today or not
#         #     print(f'New NNF Data found, modifying the nnf data in db . . .')
#         #     df_nnf = pd.read_excel(nnf_file_path, index_col=False)
#         #     df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
#         #     df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
#         #     df_nnf.dropna(how='all', inplace=True)
#         #     df_nnf = df_nnf.drop_duplicates()
#         #     write_notis_postgredb(df_nnf, n_tbl_notis_nnf_data, raw=False)
#         # else:
#         #     df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
#         #     df_nnf = df_nnf.drop_duplicates()
#         # modified_df = modify_file(df_db, df_nnf)
#         # modified_df.expiryDate = modified_df.expiryDate.astype('datetime64[ns]')
#         # modified_df.expiryDate = modified_df.expiryDate.dt.date
#         # modified_df.loc[modified_df['optionType'] == 'XX', 'strikePrice'] = 0
#         # modified_df.strikePrice = modified_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
#         # modified_df.strikePrice = modified_df.strikePrice.astype('int64')
#         # grouped_modified_df = modified_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
#         #     {'buyAvgQty': 'sum', 'buyAvgPrice': 'sum', 'sellAvgQty': 'sum', 'sellAvgPrice': 'sum'}).reset_index()
#         # grouped_modified_df['IntradayVolume'] = grouped_modified_df['buyAvgQty'] - grouped_modified_df['sellAvgQty']
#         # grouped_modified_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
#         # return grouped_modified_df.to_dict(orient='records')
#
#         # tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
#         tablenam = f'NOTIS_DESK_WISE_NET_POSITION_2025-02-14'
#         desk_db_df = read_data_db(for_table=tablenam)
#         desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
#         desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
#         desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
#         desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
#         desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
#         grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
#             {'buyAvgQty': 'sum', 'buyAvgPrice': 'sum', 'sellAvgQty': 'sum', 'sellAvgPrice': 'sum'}).reset_index()
#         grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
#         grouped_desk_db_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
#         return grouped_desk_db_df.to_dict(orient='records')
#
# service = ServiceApp()
# app = service.app
#
# if __name__ == '__main__':
#     uvicorn.run('untitled:app', host='0.0.0.0', port=8851, workers=5)
c=0
# df = read_file(rf"D:\notis_analysis\modified_data\NOTIS_TRADE_DATA_14FEB2025.xlsx")
# col_keep = ['trdQty', 'trdPrc', 'bsFlg','ctclid','sym', 'inst', 'expDt', 'strPrc', 'optType','CreateDate', 'TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']
# df = df[col_keep]
# df.expDt = pd.to_datetime(df.expDt).dt.date
# grouped_df = df.groupby(by=['ctclid','sym', 'inst', 'expDt', 'strPrc', 'optType','bsFlg'], as_index=False).agg({
#     'trdQty':'sum',
#     'trdPrc':'mean',
#     'CreateDate': lambda x: x.unique()[0],
#     'TerminalID': lambda x: x.unique()[0],
#     'TerminalName': lambda x: x.unique()[0],
#     'UserID': lambda x: x.unique()[0],
#     'SubGroup': lambda x: x.unique()[0],
#     'MainGroup': lambda x: x.unique()[0],
#     'NeatID': lambda x: x.unique()[0]
# })
b=0
# cur_time = datetime.now().time()
# tar_time = datetime.strptime('1530', '%H%M').time()
c=0
# def get_zerodha_data(from_time: datetime = Query(), to_time: datetime = Query()):
#     db_name = f'data_analytics'
#     pg_user = 'postgres'
#     pg_pass = 'Rathi@321'
#     pg_host = '192.168.100.173'
#     pg_port = '5432'
#     pg_pass_encoded = quote(pg_pass)
#
#     ist = pytz.timezone('Asia/Kolkata')
#     from_datetime = ist.localize(datetime.strptime(f'{today} {from_time}', '%Y-%m-%d %H:%M:%S'))
#     to_datetime = ist.localize(datetime.strptime(f'{today} {to_time}', '%Y-%m-%d %H:%M:%S'))
#     minute_list = []
#     start_time = from_datetime
#     while start_time <= to_datetime:
#         minute_list.append(start_time)
#         start_time += timedelta(minutes=1)
#
#     engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass_encoded}@{pg_host}:{pg_port}/{db_name}"
#     engine = create_engine(engine_str)
#     conn = engine.connect()
#     query = f"""
#         SELECT * FROM snap
#         WHERE timestamp >= '{from_datetime}'
#         AND timestamp <= '{to_datetime}'
#         ORDER BY id ASC
#     """
#     df = pd.read_sql(query, con=conn)
#     return df
d=0
# to download bhavcopy file from the server
# import paramiko
# host = '192.168.112.81'
# username = 'greek'
# password = 'greeksoft'
# filename = f"regularBhavcopy_{yesterday.strftime('%d%m%Y')}.csv" #sample=regularBhavcopy_13022025
# remote_path = rf'/home/greek/NSE_BSE_Broadcast/NSE/Bhavcopy/Files/{filename}'
# local_path = os.path.join(test_dir, filename)
# try:
#     transport = paramiko.Transport((host, 22))
#     transport.connect(username=username, password=password)
#     sftp = paramiko.SFTPClient.from_transport(transport)
#     sftp.get(remote_path, local_path)
#     sftp.close()
#     transport.close()
# except Exception as e:
#     print(f'Error: {e}')
e=0
# tablename = f'notis_raw_data_2025-02-17'
# df = read_data_db(for_table=tablename)
# list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
# list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
# list_none_str = [38]
# for i in list_str_int64:
#     column_name = f'Column{i}'
#     df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
# for i in list_str_none:
#     df[f'Column{i}'] = None
# for i in list_none_str:
#     df[f'Column{i}'] = df[f'Column{i}'].astype('str')
# print('Starting file modification...')
# df.rename(columns={
#     'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
#     'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
#     'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
#     'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
#     'Column13': 'cliActNo', 'Column14': 'cpCD', 'Column15': 'remarks',
#     'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
#     'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
#     'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
#     'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
#     'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
#     'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
#     'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
# }, inplace=True)
#
# df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
#
# df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
# df.expDt = df.expDt.astype('datetime64[ns]').dt.date
#
# df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
# # --------------------------------------------------------------------------------------------------------------------------------
# df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
# e=0
# # def collected_ctcl(series):
# #     return list(series.unique())
# # user_choice = int(input('Enter how would you like to make net-pos,\n 1-ctclidwise\n 2-symbolwise\n 3-both'))
# # if user_choice == 1 or user_choice == 3:
# #     print('Calculating ctclid wise Net-position...')
# #     grouped_df = df.groupby(['ctclid','sym','expDt','strPrc','optType','bsFlg'], as_index=False).agg({'trdQty':'sum','trdPrc':'mean'})
# # else:
# #     print('calculating sym wise netposition')
# #     grouped_df = df.groupby(['sym', 'expDt', 'strPrc', 'optType', 'bsFlg'], as_index=False).agg({'trdQty': 'sum', 'trdPrc': 'mean','ctclid':collected_ctcl})
# f=0
# # pivot_df = grouped_df.pivot_table(
# #     index=['ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
# #     columns='bsFlg',
# #     values=['trdQty', 'trdPrc'],
# #     aggfunc='sum',
# #     fill_value=0
# # )
#
# pivot_df = df.pivot_table(
#     index=['ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
#     columns='bsFlg',
#     values=['trdQty', 'trdPrc'],
#     aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
#     fill_value=0
# )
#
# # pivot_df.columns = ['BuyPrc', 'SellPrc','BuyVol', 'SellVol']
# # pivot_df = pivot_df.reset_index()
# # pivot_df = pivot_df[['ctclid', 'sym', 'expDt', 'strPrc', 'optType', 'BuyVol', 'BuyPrc', 'SellVol', 'SellPrc']]
# pivot_df.columns = ['BuyPrc', 'SellPrc','BuyVol', 'SellVol']
# pivot_df = pivot_df.reset_index()
# pivot_df = pivot_df[['ctclid', 'sym', 'expDt', 'strPrc', 'optType', 'BuyVol', 'BuyPrc', 'SellVol', 'SellPrc']]
f=0
# engine = create_engine(engine_str)
# sessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# today=datetime.now().date()
# def get_db():
#     db = sessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()
# def test_page_download_trade_data(for_date: date,db:Session=Depends(get_db)):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     zip_path = os.path.join(zipped_dir,f'zippped_{tablename}.xlsx.gz')
#     if not os.path.exists(zip_path):
#         query=text(rf'select * from "{tablename}"')
#         result = db.execute(query).fetchall()
#         with gzip.open(zip_path, 'wb') as f:
#             workbook=xlsxwriter.Workbook(f,{'in_memory':True})
#             worksheet=workbook.add_worksheet()
#             headers = result.keys()
#             for col, header in enumerate(headers):
#                 worksheet.write(0, col, header)
#             for row_num, row in enumerate(result, start=1):
#                 for col_num, cell in enumerate(row):
#                     worksheet.write(row_num, col_num, cell)
#             workbook.close()
#         # return FileResponse(zip_path, media_type='application/gzip')
#         return pd.read_excel(zip_path)
#     else:
#         return pd.read_excel(zip_path)
q=0
# def test_xlsx_gz(for_date:date):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     zip_path = os.path.join(zipped_dir, f'zippped_{tablename}.xlsx.gz')
#     if not os.path.exists(zip_path):
#         query = text(rf'select * from "{tablename}"')
#         with engine.connect() as conn:
#             result = conn.execute(query)
#         total_rows = len(result.fetchall())
#         # # wb=openpyxl.Workbook()
#         # # ws=wb.active
#         # # headers = result.keys()
#         # # ws.append(headers)
#         # # for row in result:
#         # #     ws.append(row)
#         # # buffer = io.BytesIO()
#         # # wb.save(buffer)
#         # # buffer.seek(0)
#         # # with gzip.open(zip_path, 'wb', encoding='utf-8') as f:
#         # #     f.write(buffer.getvalue())
#         # with gzip.open(zip_path, 'wb') as f:
#         #     workbook=xlsxwriter.Workbook(f,{'in_memory':True})
#         #     worksheet=workbook.add_worksheet()
#         #     headers = result.keys()
#         #     for col, header in enumerate(headers):
#         #         worksheet.write(0, col, header)
#         #     for row_num, row in enumerate(result, start=1):
#         #         for col_num, cell in enumerate(row):
#         #             worksheet.write(row_num, col_num, cell)
#         #     workbook.close()
#         pbar=progressbar.ProgressBar(max_value=total_rows,widgets=[progressbar.Percentage(),' ',progressbar.Bar(marker='=',left='[',right=']'),progressbar.ETA()])
#         buffer = io.BytesIO()
#         wb = xlsxwriter.Workbook(buffer,{'in_memory':True})
#         ws = wb.add_worksheet()
#         # pbar.update(0)
#         header = result.keys()
#         for col, header_value in enumerate(header):
#             ws.write(0, col, header_value)
#         pbar.update(0)
#         for row_num, row in enumerate(result, start=1):
#             for col_num, cell in enumerate(row):
#                 ws.write(row_num, col_num, cell)
#             pbar.update(row_num+1)
#         wb.close()
#         pbar.finish()
#         buffer.seek(0)
#         with gzip.open(zip_path, 'wb') as f:
#             f.write(buffer.getvalue())
#         return 'Done'
#     else:
#         return True
q=0
# def test_xlsx_gz_2(for_date:date):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     zip_path = os.path.join(zipped_dir, f'zippped_{tablename}.xlsx.gz')
#     xl_path = os.path.join(zipped_dir, f'xl_{tablename}.xlsx')
#     if not os.path.exists(zip_path):
#         query = text(rf'select * from "{tablename}"')
#         buffer=io.BytesIO()
#         wb=xlsxwriter.Workbook(buffer,{'in_memory':True})
#         ws=wb.add_worksheet()
#         with engine.connect() as conn:
#             result = conn.execute(query)
#             total_rows = result.rowcount
#             pbar = progressbar.ProgressBar(max_value=total_rows+1, widgets=[progressbar.Percentage(),' ',progressbar.Bar(marker='=',left='[',right=']'),progressbar.ETA()])
#             header = result.keys()
#             for col, row_value in enumerate(header):
#                 ws.write(0,col, row_value)
#             pbar.update(1)
#             for row_num, row in enumerate(result, start=1):
#                 for col_num, cell in enumerate(row):
#                     ws.write(row_num, col_num, cell)
#                 pbar.update(row_num+1)
#             pbar.finish()
#         wb.close()
#         buffer.seek(0)
#         print('zipping file . . .')
#         with gzip.open(zip_path, 'wb') as f:
#             f.write(buffer.getvalue())
#         with open(xl_path, 'wb') as f:
#             f.write(buffer.getvalue())
#         df = pd.read_sql_table(tablename, con=engine)
#         print(f"Data fetched from {tablename} table. Shape:{df.shape}")
#         print('done')
#     else:
#         return True
y=0
# def test_xlsx_gz_3(for_date:date):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     zip_path = os.path.join(zipped_dir, f'zippped_{tablename}.csv.gz')
#     csv_path = os.path.join(zipped_dir, f'zippped_{tablename}.csv')
#     # if not os.path.exists(zip_path):
#     query = text(rf'select * from "{tablename}"')
#     with engine.connect() as conn:
#         result = conn.execute(query)
#         total_rows = result.rowcount
#         pbar = progressbar.ProgressBar(max_value=total_rows+1,widgets=[progressbar.Percentage(),' ',progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
#         buffer=io.BytesIO()
#         with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
#             writer = csv.writer(io.TextIOWrapper(gz, encoding='utf-8', newline=''))
#             header = result.keys()
#             writer.writerow(header)
#             pbar.update(1)
#             for row_num, row in enumerate(result, start=1):
#                 writer.writerow(row)
#                 pbar.update(row_num+1)
#             pbar.finish()
#     with open(zip_path, 'wb') as f:
#         f.write(buffer.getvalue())
#     print('done')

# def test_csv(for_date:date):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     csv_path = os.path.join(zipped_dir, f'zippped_{tablename}.csv')
#     query = text(rf'select * from "{tablename}"')
#     with engine.connect() as conn:
#         result = conn.execute(query)
#         total_rows = result.rowcount
#         pbar = progressbar.ProgressBar(max_value=total_rows+1,widgets=[progressbar.Percentage(),' ',progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
#         with open(csv_path, mode='w', encoding='utf-8', newline='') as csvfile:
#             writer = csv.writer(csvfile)
#             header = result.keys()
#             writer.writerow(header)
#             pbar.update(1)
#             for row_num, row in enumerate(result, start=1):
#                 writer.writerow(row)
#                 pbar.update(row_num + 1)
#             pbar.finish()
#     print('done')
t=0
engine=create_engine(engine_str)
# def test_modif(for_date:date):
#     for_dt = pd.to_datetime(for_date).date()
#     tablename = f'NOTIS_TRADE_BOOK' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
#     zip_path = os.path.join(zipped_dir, f'zippped_{tablename}.csv.gz')
#     csv_path = os.path.join(zipped_dir, f'csv_{tablename}.csv')
#     query = text(rf'select * from "{tablename}"')
#     with engine.connect() as conn:
#         result = conn.execute(query)
#         total_rows = result.rowcount
#         pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[progressbar.Percentage(), ' ',
#                                                                           progressbar.Bar(marker='=', left='[',
#                                                                                           right=']'),
#                                                                           progressbar.ETA()])
#         buffer = io.StringIO()
#         writer = csv.writer(buffer)
#         header = result.keys()
#         writer.writerow(header)
#         pbar.update(1)
#         for row_num, row in enumerate(result, start=1):
#             writer.writerow(row)
#             pbar.update(row_num + 1)
#         pbar.finish()
#
#     with open(csv_path, mode='w', encoding='utf-8', newline='') as csvfile:
#         csvfile.write(buffer.getvalue())
#
#     with gzip.open(zip_path, mode='wt', encoding='utf-8', newline='') as zipfile:
#         zipfile.write(buffer.getvalue())
o=0
# oi=pd.to_datetime('20250224').date()
# a = test_modif(oi)
r=0
# for_tab = ['NOTIS_TRADE_BOOK','NOTIS_DESK_WISE_NET_POSITION', 'NOTIS_NNF_WISE_NET_POSITION', 'NOTIS_USERID_WISE_NET_POSITION']
# tod = datetime(year=2025,month=2,day=28).date()
# for each in for_tab:
#     df = read_data_db(for_table=f"{each}_{tod.strftime('%Y-%m-%d')}")
#     print(f"data read from {each}_{tod.strftime('%Y-%m-%d')}")
#     write_notis_data(df=df, filepath=os.path.join(test_dir,f"{each}_{tod.strftime('%Y-%m-%d')}.csv"))
#     print(f"data written to {each}_{tod.strftime('%Y-%m-%d')}.csv")
e=0
# def truncate_tables():
#     table_name = ["NOTIS_TRADE_BOOK_2024-12-31","NOTIS_DESK_WISE_NET_POSITION","NOTIS_NNF_WISE_NET_POSITION","NOTIS_USERID_WISE_NET_POSITION"]
#     engine = create_engine(engine_str)
#     with engine.connect() as conn:
#         for each in table_name:
#             print(each)
#             res = conn.execute(text(f'select count(*) from "{each}"'))
#             row_count = res.scalar()
#             if row_count > 0:
#                 conn.execute(text(f'delete from "{each}"'))
#                 print(f'Existing data from table {each} deleted')
#             else:
#                 print(f'No data in table {each}, no need to delete')
# truncate_tables()
u=0
# import os
# import io
# import gzip
# import progressbar
# import pandas as pd
# from sqlalchemy.sql import text
# from openpyxl import Workbook
# from fastapi.responses import FileResponse
#
#
# def export_db_to_xlsx(tablename, zip_path):
#     if not os.path.exists(zip_path):
#         stt = datetime.now()
#         query = text(f'select * from "{tablename}"')
#         with engine.connect() as conn:
#             result = conn.execute(query)
#         total_rows = result.rowcount
#         batch_size = 1_000_000  # 1 million rows per sheet
#         num_sheets = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
#
#         print(f'Total rows in DB: {total_rows}, Splitting into {num_sheets} sheets')
#
#         buffer = io.BytesIO()
#         wb = Workbook()
#         ws = wb.active
#         ws.title = "Sheet1"
#
#         header = list(result.keys())
#         ws.append(header)
#
#         pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[
#             progressbar.Percentage(), ' ',
#             progressbar.Bar(marker='=', left='[', right=']'),
#             progressbar.ETA()
#         ])
#         pbar.update(1)
#
#         sheet_index = 1
#         row_num = 0
#         for row in result:
#             if row_num > 0 and row_num % batch_size == 0:
#                 sheet_index += 1
#                 ws = wb.create_sheet(title=f"Sheet{sheet_index}")
#                 ws.append(header)
#             ws.append(tuple(row))
#             row_num += 1
#             pbar.update(row_num + 1)
#
#         pbar.finish()
#
#         wb.save(buffer)
#         buffer.seek(0)
#
#         with gzip.open(zip_path, 'wb') as f_out:
#             f_out.write(buffer.getvalue())
#         ett=datetime.now()
#         print(f"Data exported successfully with {num_sheets} sheets.")
#         print(f'total time taken for zip_path1:{(ett-stt).total_seconds()}')
#         # return FileResponse(path=zip_path, media_type='application/gzip')
#     else:
#         return FileResponse(path=zip_path, media_type='application/gzip')
#
# def export_db_to_xlsx_2(tablename, zip_path):
#     if not os.path.exists(zip_path):
#         stt=datetime.now()
#         query = text(f'select * from "{tablename}"')
#         with engine.connect() as conn:
#             result = conn.execute(query)
#         total_rows = result.rowcount
#         batch_size = 1_000_000  # 1 million rows per sheet
#         num_sheets = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
#         print(f'Total rows in DB: {total_rows}, Splitting into {num_sheets} sheets')
#         buffer = io.BytesIO()
#         wb = xlsxwriter.Workbook(buffer, {'in_memory':True})
#         # ws = wb.add_worksheet()
#         header = result.keys()
#
#         pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[progressbar.Percentage(), ' ',progressbar.Bar(marker='=', left='[',right=']'),progressbar.ETA()])
#         # # for col, header_value in enumerate(header):
#         # #     ws.write(0,col,header_value)
#         # #
#         # # for row_num, row in enumerate(result, start=1):
#         # #     for col_num, cell in enumerate(row):
#         # #         ws.write(row_num,col_num,cell)
#         # #     pbar.update(row_num)
#         # # pbar.finish()
#         # # wb.close()
#         # #
#         # # buffer.seek(0)
#         # # with gzip.open(zip_path,'wb') as f:
#         # #     f.write(buffer.getvalue())
#         # for sheet_num in range(num_sheets):
#         #     ws = wb.add_worksheet(f'Sheet{sheet_num+1}')
#         #     for col, header_value in enumerate(header):
#         #         ws.write(0,col,header_value)
#         #     start_index = sheet_num*batch_size
#         #     last_index = min(start_index,total_rows)
#         #     for row_num, row in enumerate(result[start_index:last_index], start=1):
#         #         for col_num, cell in enumerate(row):
#         #             ws.write(row_num,col_num,cell)
#         #         pbar.update(start_index+row_num)
#         # pbar.finish()
#         # wb.close()
#
#         sheet_idx = 0
#         ws = wb.add_worksheet(f'Sheet{sheet_idx+1}')
#         for col, header_value in enumerate(header):
#             ws.write(0, col, header_value)
#         row_cnt = 1
#         with engine.connect() as conn:
#             result = conn.execution_options(stream_results=True).execute(query)
#             for row_num, row in enumerate(result, start=1):
#                 if row_cnt>batch_size:
#                     sheet_idx+=1
#                     row_cnt = 1
#                     ws = wb.add_worksheet(f'Sheet{sheet_idx+1}')
#                     for col, header_value in enumerate(header):
#                         ws.write(0, col, header_value)
#
#                 for col_num, cell in enumerate(row):
#                     ws.write(row_cnt, col_num, cell)
#                 row_cnt+=1
#                 pbar.update(row_num)
#         pbar.finish()
#         wb.close()
#
#         buffer.seek(0)
#         with gzip.open(zip_path, 'wb') as f:
#             f.write(buffer.getvalue())
#         ett=datetime.now()
#         print(f'total time taken for zip_path2:{(ett-stt).total_seconds()}')
#         print('done')
#
# def export_db_to_xlsx_3(tablename, zip_path):
#     if not os.path.exists(zip_path):
#         stt=datetime.now()
#         query = text(f'select * from "{tablename}"')
#         count_query = text(f'select count(*) from "{tablename}"')
#         with engine.connect() as conn:
#             total_rows = conn.execute(count_query).scalar()
#         # total_rows = result.rowcount
#         batch_size = 1_000_000  # 1 million rows per sheet
#         num_sheets = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
#         print(f'Total rows in DB: {total_rows}, Splitting into {num_sheets} sheets')
#         buffer = io.BytesIO()
#         # wb = xlsxwriter.Workbook(buffer, {'in_memory':True})
#         # writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
#         writer = pd.ExcelWriter(buffer, engine='openpyxl')
#         # ws = wb.add_worksheet()
#         # header = result.keys()
#
#         pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[progressbar.Percentage(), ' ',progressbar.Bar(marker='=', left='[',right=']'),progressbar.ETA()])
#
#         # # sheet_idx = 0
#         # # ws = wb.add_worksheet(f'Sheet{sheet_idx+1}')
#         # # for col, header_value in enumerate(header):
#         # #     ws.write(0, col, header_value)
#         # # row_cnt = 1
#         # # with engine.connect() as conn:
#         # #     result = conn.execution_options(stream_results=True).execute(query)
#         # #     for row_num, row in enumerate(result, start=1):
#         # #         if row_cnt>batch_size:
#         # #             sheet_idx+=1
#         # #             row_cnt = 1
#         # #             ws = wb.add_worksheet(f'Sheet{sheet_idx+1}')
#         # #             for col, header_value in enumerate(header):
#         # #                 ws.write(0, col, header_value)
#         # #
#         # #         for col_num, cell in enumerate(row):
#         # #             ws.write(row_cnt, col_num, cell)
#         # #         row_cnt+=1
#         # #         pbar.update(row_num)
#         # # pbar.finish()
#         # # wb.close()
#         # to_update = 0
#         # with engine.connect() as conn:
#         #     for i, df in enumerate(pd.read_sql(query, conn, chunksize=batch_size)):
#         #         df.to_excel(writer, sheet_name=f'Sheet{i+1}', index=False)
#         #         to_update+=len(df)
#         #         pbar.update(to_update)
#         #         print(f'Written sheet{i+1} with {len(df)}rows')
#         # writer.close()
#         # pbar.finish()
#
#         to_update = 0
#         with engine.connect() as conn:
#             for i, df in enumerate(pd.read_sql(query, conn, chunksize=batch_size)):
#                 temp_csv = io.StringIO()
#                 df.to_csv(temp_csv, index=False)
#                 temp_csv.seek(0)
#                 df = pd.read_csv(temp_csv, index_col=False)
#                 df.to_excel(writer, sheet_name=f'Sheet{i+1}', index=False)
#                 to_update+=len(df)
#                 pbar.update(to_update)
#                 sys.stdout.flush()
#                 print(f'Written sheet{i+1} with {len(df)} rows')
#         writer.close()
#         pbar.finish()
#
#         buffer.seek(0)
#         with gzip.open(zip_path, 'wb') as f:
#             f.write(buffer.getvalue())
#         ett=datetime.now()
#         print(f'total time taken for zip_path3:{(ett-stt).total_seconds()}')
#         print('done')
#
# tablename = 'NOTIS_TRADE_BOOK_2025-02-24'
# zip_path = os.path.join(zipped_dir, f'zipped_{tablename}_older.xlsx.gz')
# zip_path1 = os.path.join(zipped_dir, f'zipped_{tablename}_test1.xlsx.gz')
# zip_path2 = os.path.join(zipped_dir, f'zipped_{tablename}_test2.xlsx.gz')
# zip_path3 = os.path.join(zipped_dir, f'zipped_{tablename}_test3.xlsx.gz')
# # export_db_to_xlsx(tablename, zip_path1)
# # export_db_to_xlsx_2(tablename, zip_path2)
# # export_db_to_xlsx_3(tablename, zip_path3)
u=0
tablename = 'NOTIS_TRADE_BOOK_2025-03-04'
zip_path4 = os.path.join(zipped_dir, f'zipped_{tablename}_test_2.xlsx.gz')
def get_db():
    engine=create_engine(engine_str)
    sessionLocal = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    db = sessionLocal()
    try:
        yield db
    finally:
        db.close()
def export_test_1(tablename, zip_path, db):
    if not os.path.exists(zip_path):
        stt=datetime.now()
        total_rows = db.execute(text(rf'select count(*) from "{tablename}"')).scalar()
        page_size = 5_00_000
        num_pages = total_rows//page_size + (1 if (total_rows%page_size) else 0)
        buffer = io.BytesIO()
        wb = xlsxwriter.Workbook(buffer,{'in_memory':True})
        # pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[progressbar.Percentage(),'', progressbar.Bar(marker='=', left='[',right=']'), progressbar.ETA()])
        for page in range(num_pages):
            query = f'select * from "{tablename}" limit {page_size} offset {(page)*page_size}'
            print(query)
            pbar = progressbar.ProgressBar(max_value=total_rows + 1, widgets=[progressbar.Percentage(), '',
                                                                              progressbar.Bar(marker='=', left='[',
                                                                                              right=']'),
                                                                              progressbar.ETA()])
            result = db.execute(query)
            ws = wb.add_worksheet(f'Sheet{page+1}')
            for col, header in enumerate(result.keys()):
                ws.write(0,col,header)
            for rn,row in enumerate(result, start=1):
                for col, cell in enumerate(row):
                    ws.write(rn, col, cell)
                pbar.update(rn)
            pbar.finish()
            p = 0
        wb.close()
        print('fetching data from buffer')
        buffer.seek(0)
        print('Writing to xlsx file and zipping . . ')
        with gzip.open(zip_path, 'wb') as f:
            f.write(buffer.getvalue())
        ett=datetime.now()
        print(f'total time taken for zip_path4:{(ett-stt).total_seconds()}')


db=next(get_db())
export_test_1(tablename, zip_path4, db)
y=0