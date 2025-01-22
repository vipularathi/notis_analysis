import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from sqlalchemy import text, create_engine

pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
table_dir = os.path.join(root_dir, 'table_data')

from db_config import engine_str, n_tbl_notis_desk_wise_final_net_position
from sqlalchemy import create_engine
from main import get_date_from_non_jiffy

def read_notis_file(filepath):
    wb = load_workbook(filepath, read_only=True)
    sheet = wb.active
    total_rows = sheet.max_row
    print('Reading Notis file...')
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                           progressbar.Bar(marker='=', left='[', right=']'),
                                                           progressbar.ETA()])

    data = []
    pbar.update(0)
    for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
        data.append(row)
        pbar.update(i)
    pbar.finish()
    df = pd.DataFrame(data[1:], columns=data[0])
    print('Notis file read')
    return df

def read_db(table_name):
    engine = create_engine(engine_str)
    df = pd.read_sql_table(table_name, con=engine)
    return df

def write_notis_data(df, filepath):
    print('Writing Notis file to excel...')
    wb = Workbook()
    ws = wb.active
    ws.title = 'Net position'
    rows = list(dataframe_to_rows(df, index=False, header=True))
    total_rows = len(rows)
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                           progressbar.Bar(marker='=', left='[', right=']'),
                                                           progressbar.ETA()])
    for i, row in enumerate(rows, start=1):
        ws.append(row)
        pbar.update(i)
    pbar.finish()
    # df.to_excel(os.path.join(modified_dir, file_name))
    print('Saving the file...')
    # wb.save(filepath)
    wb.save(filepath)
    print('New Notis excel file created')

def get_date_from_non_jiffy(dt_val):
    """
    Converts the 1980 format date time to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Assuming dt_val is seconds since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    # date_time = int(base_date.timestamp() + dt_val)
    date_time = base_date.timestamp() + dt_val
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date

def write_notis_postgredb(df, table_name, raw=False):
    start_time = time.time()
    engine = create_engine(engine_str)

    with engine.connect() as conn:
        res = conn.execute(text(f'select count(*) from "{table_name}"'))
        row_count = res.scalar()
        if row_count > 0:
            conn.execute(text(f'delete from "{table_name}"'))
            print(f'Existing data from table {table_name} deleted')
    print(f'Writing {"Raw" if raw else "Modified"} data to database...')
    total_rows = len(df)
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[
        progressbar.Percentage(), ' ',
        progressbar.Bar(marker='=', left='[', right=']'),
        progressbar.ETA()
    ])

    if raw:
        list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
        list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
        list_none_str = [38]
        for i in list_str_int64:
            # df_db.loc[:, f'Column{i}'] = df_db.loc[:, f'Column{i}'].astype('int64')
            column_name = f'Column{i}'
            df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
        for i in list_str_none:
            df[f'Column{i}'] = None
        for i in list_none_str:
            df[f'Column{i}'] = df[f'Column{i}'].astype('str')
    chunk_size = 1000
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i + chunk_size]
        chunk.to_sql(table_name, engine, index=False, if_exists='append', method='multi')
        pbar.update(min(i + chunk_size, total_rows))

    pbar.finish()
    print(f'{"Raw" if raw else "Modified"} Data successfully inserted into database')
    end_time = time.time()
    print(f'Total time taken: {end_time - start_time} seconds')

# -------------------------------------------------------------------------------------------------------------
table_list = ['NOTIS_DESK_WISE_NET_POSITION', 'NOTIS_NNF_WISE_NET_POSITION', 'NOTIS_USERID_WISE_NET_POSITION']
# today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
today = datetime.now().date().strftime('%Y_%m_%d').upper()
for table in table_list:
    # df = read_db(f'{table}_{today}')
    df = read_db(table)
    # df.to_excel(f'{table}_{today}.xlsx', index=False)
    df.to_excel(os.path.join(table_dir, f'{table}_{today}.xlsx'), index=False)
    # print(f'Data fetched from {table}:\n{df.head()}')
    print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}_{today}.xlsx')}")
# -------------------------------------------------------------------------------------------------------------

# n_tbl_notis_trade_book = "NOTIS_DESK_WISE_NET_POSITION_2025-01-20"
# df = read_notis_file(rf"D:\notis_analysis\table_data\NOTIS_DESK_WISE_NET_POSITION_2025_01_20.xlsx")
#
# write_notis_postgredb(df, n_tbl_notis_trade_book)