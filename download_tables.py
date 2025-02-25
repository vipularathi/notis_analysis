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
from common import get_date_from_non_jiffy, read_data_db, write_notis_postgredb, today


pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
table_dir = os.path.join(root_dir, 'table_data')
# today = datetime(year=2025, month=2, day=4).date()

# -------------------------------------------------------------------------------------------------------------
# table_list = ['NOTIS_DESK_WISE_NET_POSITION', 'NOTIS_NNF_WISE_NET_POSITION', 'NOTIS_USERID_WISE_NET_POSITION']
# # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
# # today = datetime.now().date().strftime('%Y_%m_%d').upper()
# for table in table_list:
#     df = read_data_db(for_table=table)
#     # df = read_db(table)
#     # df.to_excel(f'{table}_{today}.xlsx', index=False)
#     df.to_excel(os.path.join(table_dir, f'{table}_{today.strftime("%Y_%m_%d").upper()}.xlsx'), index=False)
#     # print(f'Data fetched from {table}:\n{df.head()}')
#     # print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}_{today.strftime("%Y_%m_%d").upper()}.xlsx')}")
# -------------------------------------------------------------------------------------------------------------

# n_tbl_notis_trade_book = "NOTIS_TRADE_BOOK_2025-02-18"
# n_tbl_notis_trade_book = "NOTIS_DESK_WISE_NET_POSITION_2025-02-18"
# n_tbl_notis_trade_book = "NOTIS_NNF_WISE_NET_POSITION_2025-02-18"
n_tbl_notis_trade_book = "NOTIS_USERID_WISE_NET_POSITION_2025-02-18"
df = read_notis_file(rf"D:\notis_analysis\table_data\NOTIS_USERID_WISE_NET_POSITION_2025_02_18.xlsx")
write_notis_postgredb(df, n_tbl_notis_trade_book)