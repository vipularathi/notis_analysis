import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from sqlalchemy import text, create_engine

from db_config import engine_str
from sqlalchemy import create_engine
from common import get_date_from_non_jiffy, read_data_db, write_notis_postgredb, today, read_file, write_notis_data


pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
table_dir = os.path.join(root_dir, 'table_data')
test_dir = os.path.join(root_dir,'testing')
# today = datetime(year=2025, month=3, day=7).date()

# # -------------------------------------------------------------------------------------------------------------
# table_list = ['NOTIS_EOD_NET_POS_CP_NONCP_2025-03-20','NOTIS_TRADE_BOOK_2025-02-28','NOTIS_DESK_WISE_NET_POSITION_2025-02-28', 'NOTIS_NNF_WISE_NET_POSITION_2025-02-28', 'NOTIS_USERID_WISE_NET_POSITION_2025-02-28']
table_list = ['notis_raw_data_2025-03-17']
# today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
# today = datetime.now().date().strftime('%Y_%m_%d').upper()
for table in table_list:
    df = read_data_db(for_table=table)
    # df = read_db(table)
    # df.to_excel(f'{table}_{today}.xlsx', index=False)
    write_notis_data(df,os.path.join(test_dir, f'{table}.xlsx'))
    # df.to_excel(os.path.join(test_dir, f'{table}.xlsx'), index=False)
    print(f'Data fetched from {table}:\n{df.head()}')
    # print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}_{today.strftime("%Y_%m_%d").upper()}.xlsx')}")
# # -------------------------------------------------------------------------------------------------------------

# table_list = [f'NOTIS_EOD_NET_POS_CP_NONCP_2025-03-18']
# # table_list = [f'NOTIS_DESK_WISE_NET_POSITION', f'NOTIS_NNF_WISE_NET_POSITION', f'NOTIS_USERID_WISE_NET_POSITION']
# for each in table_list:
#     # df = read_file(rf"D:\notis_analysis\table_data\{each}_{today.strftime('%Y_%m_%d')}.csv")
#     df = read_file(rf"D:\notis_analysis\overall_net_position_output\final_cp_ncp_net_pos_18_03_2025_all.xlsx")
#     # list_str_int64 = [1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 16, 17, 18, 23, 27, 28,46]
#     # list_str_int64 = [1, 2, 3, 6, 7, 10, 11, 12,17,18,20,22,24,29,40,47]
#     # for i, col in enumerate(df.columns.tolist(),start=1):
#     #     if i in list_str_int64:
#     #         df[col] = df[col].astype('int64')
#     # df['CreateDate'] = pd.to_datetime(today)
#     p=0
#     # write_notis_postgredb(df, f'{each}_{today.strftime("%Y-%m-%d")}')
#     write_notis_postgredb(df, f'{each}')