import numpy as np
import pandas as pd
import os, re
# from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone, date
import pytz
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from sqlalchemy import create_engine
from urllib.parse import quote
import warnings
from fastapi import FastAPI, Query, status, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from db_config import n_tbl_notis_trade_book, s_tbl_notis_trade_book, n_tbl_notis_raw_data, s_tbl_notis_raw_data, n_tbl_notis_nnf_data, s_tbl_notis_nnf_data
# from main import modify_file
from common import get_date_from_non_jiffy, read_data_db, read_notis_file, write_notis_data, today, yesterday, write_notis_postgredb, read_file

# today = datetime(year=2025, month=1, day=24).date()
# yesterday = datetime(year=2025, month=1, day=23).date()
pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

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


class ServiceApp:
    def __init__(self):
        super().__init__()
        self.app = FastAPI(title='NOTIS_Net_Position', description='Notis_net_position', docs_url='/docs', openapi_url='/openapi.json')
        self.app.add_middleware(CORSMiddleware, allow_origins = ['*'], allow_credentials = True, allow_methods=['*'], allow_headers=['*'])
        self.add_routes()

    def add_routes(self):
        # self.app.add_api_route('/netPosition/intraday', methods=['GET'], endpoint=self.get_intraday_net_position)
        self.app.add_api_route('/get_oi', methods=['GET'], endpoint=self.get_oi)

    def get_intraday_net_position(self):
        # df_db = read_data_db()
        # nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
        # if not os.path.exists(nnf_file_path):
        #     raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")
        # readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
        # if readable_mod_time.date() == today:  # Check if the NNF file is modified today or not
        #     print(f'New NNF Data found, modifying the nnf data in db . . .')
        #     df_nnf = pd.read_excel(nnf_file_path, index_col=False)
        #     df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
        #     df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
        #     df_nnf.dropna(how='all', inplace=True)
        #     df_nnf = df_nnf.drop_duplicates()
        #     write_notis_postgredb(df_nnf, n_tbl_notis_nnf_data, raw=False)
        # else:
        #     df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
        #     df_nnf = df_nnf.drop_duplicates()
        # modified_df = modify_file(df_db, df_nnf)
        # modified_df.expiryDate = modified_df.expiryDate.astype('datetime64[ns]')
        # modified_df.expiryDate = modified_df.expiryDate.dt.date
        # modified_df.loc[modified_df['optionType'] == 'XX', 'strikePrice'] = 0
        # modified_df.strikePrice = modified_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
        # modified_df.strikePrice = modified_df.strikePrice.astype('int64')
        # grouped_modified_df = modified_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
        #     {'buyAvgQty': 'sum', 'buyAvgPrice': 'sum', 'sellAvgQty': 'sum', 'sellAvgPrice': 'sum'}).reset_index()
        # grouped_modified_df['IntradayVolume'] = grouped_modified_df['buyAvgQty'] - grouped_modified_df['sellAvgQty']
        # grouped_modified_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
        # return grouped_modified_df.to_dict(orient='records')

        # tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
        tablenam = f'NOTIS_DESK_WISE_NET_POSITION_2025-02-14'
        desk_db_df = read_data_db(for_table=tablenam)
        desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
        desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
        desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
        desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
        desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
        grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
            {'buyAvgQty': 'sum', 'buyAvgPrice': 'sum', 'sellAvgQty': 'sum', 'sellAvgPrice': 'sum'}).reset_index()
        grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
        grouped_desk_db_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
        return grouped_desk_db_df.to_dict(orient='records')

    def get_zerodha_data(self, from_time:datetime=Query(), to_time:datetime=Query()):
        db_name = f'data_analytics'
        pg_user = 'postgres'
        pg_pass = 'Rathi@321'
        pg_host = 'localhost'
        pg_port = '5432'
        pg_pass_encoded = quote(pg_pass)

        ist = pytz.timezone('Asia/Kolkata')
        from_datetime = ist.localize(datetime.strptime(f'{today} {from_time}', '%Y-%m-%d %H:%M:%S'))
        to_datetime = ist.localize(datetime.strptime(f'{today} {to_time}', '%Y-%m-%d %H:%M:%S'))
        minute_list = []
        start_time = from_time
        while start_time <= to_datetime:
            minute_list.append(start_time)
            start_time += timedelta(minutes=1)

        engine_str = f"postgresql+psycopg2://{pg_user}:{pg_pass_encoded}@{pg_host}:{pg_port}/{db_name}"
        engine = create_engine(engine_str)
        query = f"""
            SELECT * FROM snap
            WHERE timestamp >= '{from_datetime}' 
            AND timestamp <= '{to_datetime}' 
            ORDER BY id ASC
        """
        df = pd.read_sql(query)
        return df

    def get_oi(self, for_date=Query()):
        for_date = datetime.today().date().strftime('%Y-%m-%d')
        table_to_read = f'NOTIS_EOD_NET_POS_CP_NONCP_{for_date}'
        eod_df = read_data_db(for_table=table_to_read)
        eod_df.columns = [re.sub(r'Eod|\s', '', each) for each in eod_df.columns]
        grouped_df = eod_df.groupby(by=['Broker', 'Underlying', 'Expiry'], as_index=False).agg(
            {'FinalNetQty': lambda x: x.abs().sum()})
        json_data = grouped_df.to_json(orient='records')
        return Response(json_data, media_type='application/json')


service = ServiceApp()
app = service.app

if __name__ == '__main__':
    uvicorn.run('intrim_app:app', host='0.0.0.0', port=8891, workers=5)