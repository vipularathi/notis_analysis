# import numpy as np
# import pandas as pd
# import os
# import time
# from datetime import datetime, timedelta, timezone
# from openpyxl import Workbook, load_workbook
# from openpyxl.utils.dataframe import dataframe_to_rows
# import progressbar
# from sqlalchemy import create_engine
# from common import get_date_from_non_jiffy, read_data_db, read_notis_file, write_notis_data, today, yesterday, write_notis_postgredb, read_file
# import warnings
# import re
#
#
# # today = datetime(year=2025, month=3, day=19).date()
# # # yesterday = datetime(year=2025, month=3, day=13).date()
# # yesterday = today - timedelta(days=1)
#
# pd.set_option('display.max_columns', None)
# warnings.filterwarnings('ignore')
#
# root_dir = os.path.dirname(os.path.abspath(__file__))
# # nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
# # eod_test_dir = os.path.join(root_dir, 'eod_testing')
# # eod_input_dir = os.path.join(root_dir, 'eod_original')
# # eod_output_dir = os.path.join(root_dir, 'eod_data')
# # table_dir = os.path.join(root_dir, 'table_data')
# bhav_path = os.path.join(root_dir, 'bhavcopy')
# test_dir = os.path.join(root_dir, 'testing')
# # eod_net_pos_input_dir = os.path.join(root_dir, 'overall_net_position_input')
# # eod_net_pos_output_dir = os.path.join(root_dir, 'overall_net_position_output')
#
# def calc_eod_cp_noncp():
#     eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}' #NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
#     eod_df = read_data_db(for_table=eod_tablename)
#     eod_df.columns = [re.sub(r'Eod|\s','',each) for each in eod_df.columns]
#     # Underlying	Strike	Option Type	Expiry	Net Quantity	Settlement Price
#     eod_df.drop(columns=['NetQuantity','buyQty','buyAvgPrice','sellQty','sellAvgPrice','IntradayVolume','ClosingPrice'], inplace=True)
#     eod_df.rename(columns={'FinalNetQty':'NetQuantity','FinalSettlementPrice':'ClosingPrice'}, inplace=True)
#     eod_df = eod_df.add_prefix('Eod')
#     eod_df.EodExpiry = eod_df.EodExpiry.astype('datetime64[ns]')
#     eod_df.EodExpiry = eod_df.EodExpiry.dt.date
#     eod_df = eod_df.query("EodExpiry >= @today and EodNetQuantity != 0")
#
#     grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum','EodClosingPrice':'mean'})
#     grouped_eod = grouped_eod.drop_duplicates()
#     # ================================================================
#     # tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
#     tablenam = f'NOTIS_DESK_WISE_NET_POSITION'
#     desk_db_df = read_data_db(for_table=tablenam)
#     desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
#     desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
#     desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
#     desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
#     desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
#     # desk_db_df['broker'] = desk_db_df['brokerID'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')
#     grouped_desk_db_df = desk_db_df.groupby(by=['broker','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
#     grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
#     grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'}, inplace=True)
#     # ================================================================
#
#     merged_df = grouped_eod.merge(grouped_desk_db_df, left_on=['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], right_on=["broker","symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
#     merged_df.fillna(0, inplace=True)
#     merged_df = merged_df.drop_duplicates()
#
#     coltd1 = ['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
#     coltd2 = ["broker","symbol", "expiryDate", "strikePrice", "optionType"]
#     for i in range(len(coltd1)):
#         merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
#         merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
#     merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
#     merged_df.drop(columns = ['broker','symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace = True)
#
#     if datetime.strptime('16:00:00', '%H:%M:%S').time() < datetime.now().time():
#         bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
#         bhav_matched_files = [f for f in os.listdir(bhav_path) if re.match(bhav_pattern, f)]
#         bhav_df = read_file(os.path.join(bhav_path, bhav_matched_files[0])) # regularBhavcopy_14012025.xlsx
#         bhav_df.columns = bhav_df.columns.str.replace(' ', '')
#         bhav_df.rename(columns={'VWAPclose':'closingPrice'}, inplace=True)
#         bhav_df.columns = bhav_df.columns.str.capitalize()
#         bhav_df = bhav_df.add_prefix('Bhav')
#         bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
#         bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
#         bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#         bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
#         bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
#         col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
#         bhav_df = bhav_df[col_keep]
#         bhav_df = bhav_df.drop_duplicates()
#
#         merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
#         merged_bhav_df.drop(columns = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace = True)
#     else:
#         merged_bhav_df = merged_df.copy()
#         merged_bhav_df['BhavClosingprice'] = 0
#
#     merged_bhav_df.fillna(0,inplace=True)
#     merged_bhav_df.buyAvgPrice = merged_bhav_df.buyAvgPrice.astype('int64')
#     merged_bhav_df.sellAvgPrice = merged_bhav_df.sellAvgPrice.astype('int64')
#     merged_bhav_df.BhavClosingprice = merged_bhav_df.BhavClosingprice.astype('int64')
#
#
#     # filtered_merged = merged_bhav_df.query("FinalNetQty != 0 and EodExpiry != @today")
#     # filtered_merged = merged_bhav_df.query("EodExpiry != @today")
#
#     merged_bhav_df.EodExpiry = merged_bhav_df.EodExpiry.astype('str')
#     merged_bhav_df.rename(columns = {'BhavClosingprice':'FinalSettlementPrice'}, inplace = True)
#     # write_notis_data(merged_bhav_df, os.path.join(test_dir, f'final_cp_ncp_net_pos_{today.strftime("%d_%m_%Y")}.xlsx'))
#     write_notis_postgredb(merged_bhav_df,f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}')
#
# # calc_eod_cp_noncp()