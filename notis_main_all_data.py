import pandas as pd
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context, Manager, Pipe
from time import sleep
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text


from main import modify_file
from common import read_data_db, today, write_notis_postgredb, engine_str, root_dir
from db_config import n_tbl_notis_nnf_data, n_tbl_notis_trade_book, n_tbl_notis_raw_data
from net_position_cp_noncp import calc_eod_cp_noncp

# run_times = [
#     (9, 30), (10, 0), (10, 30), (11, 0), (11, 30), (12, 0),
#     (12, 30), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0)
# ]
run_times = [
    (9, 20), (9, 30), (9, 40), (9, 50),
    (10, 0), (10, 10), (10, 20), (10, 30), (10, 40), (10, 50),
    (11, 0), (11, 10), (11, 20), (11, 30), (11, 40), (11, 50),
    (12, 0), (12, 10), (12, 20), (12, 27), (12, 40), (12, 50),
    (13, 0), (13, 10), (13, 20), (13, 30), (13, 40), (13, 50),
    (14, 0), (14, 10), (14, 20), (14, 30), (14, 40), (14, 50),
    (15, 0), (15, 15), (15, 50)
]
def truncate_tables():
    table_name = ["notis_raw_data","NOTIS_TRADE_BOOK","NOTIS_DESK_WISE_NET_POSITION","NOTIS_NNF_WISE_NET_POSITION","NOTIS_USERID_WISE_NET_POSITION",f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}']
    engine = create_engine(engine_str)
    # with engine.connect() as conn:
    with engine.begin() as conn:
        for each in table_name:
            res = conn.execute(text(f'select count(*) from "{each}"'))
            row_count = res.scalar()
            if row_count > 0:
                # conn.execute(text(f'delete from "{each}"'))
                conn.execute(text(f'truncate table "{each}"'))
                print(f'Existing data from table {each} deleted')
            else:
                print(f'No data in table {each}, no need to delete')

def main():
    stt=datetime.now()
    truncate_tables()
    df_db = read_data_db()
    print(f'trade data fetched for {datetime.now().time()}')
    write_notis_postgredb(df_db, table_name=n_tbl_notis_raw_data, raw=True)
    # df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
    # df_nnf = df_nnf.drop_duplicates()
    nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    if not os.path.exists(nnf_file_path):
        raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")
    readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    if readable_mod_time.date() == today:
        print(f'New NNF Data found, modifying the nnf data in db . . .')
        df_nnf = pd.read_excel(nnf_file_path, index_col=False)
        df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
        df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
        df_nnf.dropna(how='all', inplace=True)
        df_nnf = df_nnf.drop_duplicates()
        write_notis_postgredb(df_nnf, n_tbl_notis_nnf_data, raw=False)
    else:
        df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
        df_nnf = df_nnf.drop_duplicates()
    modified_df = modify_file(df_db, df_nnf)
    print('data modified')
    write_notis_postgredb(modified_df, table_name=n_tbl_notis_trade_book, raw=False)
    print('Data fetched, modified and added to the db')
    ett=datetime.now()
    print(f'total time taken to update {len(df_db)} rows: {(ett-stt).seconds} seconds')
    print(f'Updating cp-noncp eod table...')
    stt=datetime.now()
    calc_eod_cp_noncp()
    ett=datetime.now()
    print(f'Eod(cp-noncp) updation completed. Total time taken: {(ett-stt).seconds} seconds')

if __name__ == '__main__':
    print('Notis Main Started . . .')
    for each in run_times:
        # truncate_tables()
        # current_time = datetime.now()
        target_time = datetime.now().replace(hour=each[0], minute=each[1], second=0, microsecond=0)
        if datetime.now() > target_time:
            continue
        truncated_time = target_time - timedelta(seconds=30)
        while datetime.now() < truncated_time:
            sleep(1)
        truncate_tables()
        while datetime.now() < target_time:
            sleep(1)
        main()
    while datetime.now() < datetime.now().replace(hour=15,minute=15,second=0,microsecond=0):
        sleep(1)
    truncate_tables()
    print(f'Intraday trade execution completed. Exiting...')