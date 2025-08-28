import pandas as pd
import os
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
for_date = datetime(year=2025, month=8, day=8).date()
# table_list = ['NOTIS_EOD_NET_POS_CP_NONCP_2025-03-20','NOTIS_TRADE_BOOK_2025-02-28','NOTIS_DESK_WISE_NET_POSITION_2025-02-28', 'NOTIS_NNF_WISE_NET_POSITION_2025-02-28', 'NOTIS_USERID_WISE_NET_POSITION_2025-02-28']
# table_list = [
#     f'NOTIS_DESK_WISE_NET_POSITION_{for_date}',f'test_net_pos_desk_{for_date}',
#     f'test_cp_noncp_{for_date}',
#     f'test_mod_{for_date}',f'NOTIS_TRADE_BOOK_{for_date}',
#     f'test_net_pos_nnf_{for_date}', f'NOTIS_NNF_WISE_NET_POSITION_{for_date}',
#     f'test_raw_{for_date}', f'notis_raw_data_{for_date}',
#     f'test_bse_{for_date}', f'BSE_TRADE_DATA_{for_date}'
# ]
table_list = [f"NOTIS_DESK_WISE_NET_POSITION_{for_date}",f"NOTIS_EOD_NET_POS_CP_NONCP_{for_date}",
              f"NOTIS_NNF_WISE_NET_POSITION_{for_date}", f"BSE_TRADE_DATA_{for_date}",
              f"NOTIS_TRADE_BOOK_{for_date}", f"NOTIS_DEAL_SHEET_{for_date}", f"NOTIS_DELTA_{for_date}"]
# # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
# # today = datetime.now().date().strftime('%Y_%m_%d').upper()
for table in table_list:
    df = read_data_db(for_table=table)
#     # df = read_db(table)
#     # df.to_excel(f'{table}_{today}.xlsx', index=False)
    write_notis_data(df,os.path.join(table_dir, f'{table}.xlsx'))
#     # df.to_excel(os.path.join(test_dir, f'{table}.xlsx'), index=False)
#     print(f'Data fetched from {table}:\n{df.head()}')
#     # print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}_{today.strftime("%Y_%m_%d").upper()}.xlsx')}")
# # -------------------------------------------------------------------------------------------------------------
# df_desk = read_data_db(for_table=table_list[0])
# df_desk_test = read_data_db(for_table=table_list[1])
# df_cp = read_data_db(for_table=table_list[2])
# df_mod_test = read_data_db(for_table=table_list[3])
# df_mod = read_data_db(for_table=table_list[4])
# df_nnf_test = read_data_db(for_table=table_list[5])
# df_nnf = read_data_db(for_table=table_list[6])
# df_raw_test = read_data_db(for_table=table_list[7])
# df_raw = read_data_db(for_table=table_list[8])
# df_bse_test = read_data_db(for_table=table_list[9])
# df_bse = read_data_db(for_table=table_list[10])
p=0