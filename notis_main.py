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

run_times = [
    (9, 30), (10, 0), (10, 30), (11, 0), (11, 30), (12, 0),
    (12, 30), (13, 0), (13, 30), (14, 0), (14, 30), (15, 0)
]
def truncate_tables():
    table_name = ['NOTIS_TRADE_BOOK','NOTIS_DESK_WISE_NET_POSITION','NOTIS_NNF_WISE_NET_POSITION','NOTIS_USERID_WISE_NET_POSITION']
    engine = create_engine(engine_str)
    with engine.connect() as conn:
        for each in table_name:
            res = conn.execute(text(f'select count(*) from "{each}"'))
            row_count = res.scalar()
            if row_count > 0:
                conn.execute(text(f'delete from "{each}"'))
                print(f'Existing data from table {each} deleted')

def main():
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
# if __name__ == '__main__':
#     while True:
#         current_time = datetime.now()
#         if current_time.hour == 9 and current_time.minute == 30:
#             target_time = datetime.now().replace(hour=15,minute=5,second=0,microsecond=0)
#             while datetime.now() < target_time:
#                 main()
#                 sleep(1800)
#                 truncate_tables()
#             break
#         sleep(10)

if __name__ == '__main__':
    print('Notis Main Started . . .')
    for each in run_times:
        # current_time = datetime.now()
        target_time = datetime.now().replace(hour=each[0], minute=each[1], second=0, microsecond=0)
        if datetime.now() > target_time:
            continue
        truncated_time = target_time - timedelta(minutes=2)
        while datetime.now() < truncated_time:
            sleep(1)
        truncate_tables()
        while datetime.now() < target_time:
            sleep(1)
        main()
    while datetime.now() < datetime.now().replace(hour=15,minute=5,second=0,microsecond=0):
        sleep(1)
    truncate_tables()
    print(f'Intraday trade execution completed. Exiting...')