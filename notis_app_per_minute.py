import io, re, csv, os, json, warnings, xlsxwriter, progressbar, gzip, uvicorn, time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone, date
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, Query, status, Response, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from db_config import (n_tbl_notis_trade_book, s_tbl_notis_trade_book,
                       n_tbl_notis_raw_data, s_tbl_notis_raw_data,
                       n_tbl_notis_nnf_data, s_tbl_notis_nnf_data,
                       engine_str, notis_engine_str, bse_engine_str)
from common import (get_date_from_non_jiffy,get_date_from_jiffy,
                    read_data_db, read_notis_file, read_file,
                    write_notis_data, write_notis_postgredb,
                    today, yesterday,
                    logger, volt_dir, zipped_dir)

pd.set_option('display.max_columns', None)
warnings.filterwarnings('ignore')

engine = create_engine(engine_str, pool_pre_ping=True, pool_recycle=900)
sessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

notis_engine = create_engine(notis_engine_str, pool_pre_ping=True, pool_recycle=900)
sessionLocalNotis = sessionmaker(autocommit=False, autoflush=False, bind=notis_engine)

bse_engine = create_engine(bse_engine_str, pool_pre_ping=True, pool_recycle=900)
sessionLocalBSE = sessionmaker(autocommit=False, autoflush=False, bind=bse_engine)

def conv_str(obj):
    if isinstance(obj, datetime):
        # return obj.strftime('%Y-%m-%d %H:%M:%S')
        return obj.isoformat()
    return obj
def get_db(for_table:str=Query()):
    if for_table == f'sourcenotisraw':
        db = sessionLocalNotis()
    elif for_table == f'sourcebseraw':
        db = sessionLocalBSE()
    else:
        db = sessionLocal()
    try:
        yield db
    finally:
        db.close()
def get_notis_db():
    db = sessionLocalNotis()
    try:
        yield db
    finally:
        db.close()

def get_bse_db():
    db = sessionLocalBSE()
    try:
        yield db
    finally:
        db.close()

class ServiceApp:
    def __init__(self):
        self.app = FastAPI(title='NOTIS_Net_Position', description='Notis_net_position', docs_url='/docs', openapi_url='/openapi.json')
        self.app.add_middleware(CORSMiddleware, allow_origins = ['*'], allow_credentials = True, allow_methods=['*'], allow_headers=['*'])
        self.add_routes()
        self.main_mod_df = pd.DataFrame()

    def add_routes(self):
        self.app.add_api_route('/netPosition/intraday', methods=['GET'], endpoint=self.get_intraday_net_position)
        self.app.add_api_route('/netPosition/eod', methods=['GET'], endpoint=self.get_intraday_net_position)
        self.app.add_api_route('/data', methods=['GET'], endpoint=self.get_data)
        self.app.add_api_route('/netPosition/raw', methods=['GET'], endpoint=self.get_raw_net_position)
        self.app.add_api_route('/download', methods=['GET'], endpoint=self.download_data)
        self.app.add_api_route('/exposure', methods=['GET'], endpoint=self.get_exposure)
        self.app.add_api_route('/sourceData', methods=['GET'], endpoint=self.get_source_data)
        self.app.add_api_route('/downloadSourceData', methods=['GET'], endpoint=self.download_source_data)
        self.app.add_api_route('/get_oi', methods=['GET'], endpoint=self.get_oi)

    def get_data(self, for_date:date=Query(), for_table:str=Query(), page:int=Query(1), page_size:int=Query(1000),db:Session=Depends(get_db)):
        for_dt = pd.to_datetime(for_date).date()
        if for_table == 'modifiedtradebook':
            tablename = f"NOTIS_TRADE_BOOK_{today}" if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
        elif for_table == 'nnfwise':
            tablename = f'NOTIS_NNF_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_NNF_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'useridwise': #to_remove
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'deskwise':
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'rawtradebook':
            tablename = f'notis_raw_data_{today}' if for_dt == today else f'notis_raw_data_{for_dt}'
        elif for_table == 'eodnetposcp':
            tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{today}' if for_dt == today else f'NOTIS_EOD_NET_POS_CP_NONCP_{for_dt.strftime("%Y-%m-%d")}'
        elif for_table == f'bsetradebook':
            tablename = f'BSE_TRADE_DATA_{today.strftime("%Y-%m-%d")}' if for_dt == today else f'BSE_TRADE_DATA_{for_dt.strftime("%Y-%m-%d")}'
        query=text(rf'Select * from "{tablename}" limit {page_size} offset {(page -1)*page_size}')
        result = db.execute(query).fetchall()
        total_rows = db.execute(text(rf'Select count(*) from "{tablename}"')).scalar()
        json_data = {
            'data':[{k: conv_str(v) for k, v in row._mapping.items()} for row in result],
            'total_rows':total_rows,
            'page':page,
            'page_size':page_size
        }
        if not len(result):
            json_data = pd.DataFrame(columns=['data','total_rows','page','page_size']).to_json(orient='records')
            return Response(content=json_data, media_type='application/json')
        else:
            compressed_data = gzip.compress(json.dumps(json_data).encode('utf-8'))
            logger.info(f'\ntotal_rows={json_data["total_rows"]}\tpage={json_data["page"]}\tpage_size={json_data["page_size"]}\n')
            return Response(content=compressed_data, media_type='application/gzip')

    def download_data(self,for_date:date=Query(),for_table:str=Query(), db:Session=Depends(get_db)):
        for_dt = pd.to_datetime(for_date).date()
        if for_table == 'modifiedtradebook':
            tablename = f'NOTIS_TRADE_BOOK_{today}' if for_dt == today else f'NOTIS_TRADE_BOOK_{for_dt}'
        elif for_table == 'nnfwise':
            tablename = f'NOTIS_NNF_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_NNF_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'useridwise':
            tablename = f'NOTIS_NNF_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_NNF_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'deskwise':
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'rawtradebook':
            tablename = f'notis_raw_data_{today}' if for_dt == today else f'notis_raw_data_{for_dt}'
        # netPosition eodNetPosition rawtradebooknetposi
        elif for_table == 'modifiedtradebooknetposi':
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        elif for_table == 'eodNetPosition':
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}' if for_dt == today else f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        elif for_table == f'rawtradebooknetposi':
            tablename = f'notis_raw_data_{today}' if for_dt == today else f'notis_raw_data_{for_dt}'
        elif for_table == f'eodnetposcp':
            tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{today}' if for_dt == today else f'NOTIS_EOD_NET_POS_CP_NONCP_{for_dt.strftime("%Y-%m-%d")}'
        elif for_table == f'bsetradebook':
            tablename = f'BSE_TRADE_DATA_{today.strftime("%Y-%m-%d")}' if for_dt == today else f'BSE_TRADE_DATA_{for_dt.strftime("%Y-%m-%d")}'
        # logger.info(f'tablename is {tablename}')
        if for_dt == today:
            zip_path = os.path.join(zipped_dir, f'zipped_{tablename}_{for_dt}.xlsx.gz')
        else:
            zip_path = os.path.join(zipped_dir, f'zipped_{tablename}.xlsx.gz')

        if for_table in ['modifiedtradebooknetposi','eodNetPosition','rawtradebooknetposi']:
            if for_table == 'modifiedtradebooknetposi' or for_table=='eodNetPosition':
                desk_db_df = read_data_db(for_table=tablename)
                desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
                desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
                desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
                desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
                desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
                grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
                    {'buyAvgQty': 'sum', 'buyAvgPrice': 'mean', 'sellAvgQty': 'sum',
                     'sellAvgPrice': 'mean'}).reset_index()
                grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df[
                    'sellAvgQty']
                grouped_desk_db_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
                grouped_desk_db_df.expiryDate = grouped_desk_db_df.expiryDate.astype(str)
                if grouped_desk_db_df.empty:
                    return JSONResponse(content={"message": "No data available"}, status_code=204)
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    grouped_desk_db_df.to_excel(writer, index=False)
                buffer.seek(0)
                with gzip.open(zip_path, 'wb') as f:
                    f.write(buffer.getvalue())
                return FileResponse(zip_path,media_type='application/gzip')
            else:
                df = read_data_db(for_table=tablename)
                if not len(df):
                    return JSONResponse(content={"message": "No data available"}, status_code=204)
                list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
                list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
                list_none_str = [38]
                for i in list_str_int64:
                    column_name = f'Column{i}'
                    df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
                for i in list_str_none:
                    df[f'Column{i}'] = None
                for i in list_none_str:
                    df[f'Column{i}'] = df[f'Column{i}'].astype('str')
                logger.info('Starting file modification...')
                df.rename(columns={
                    'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
                    'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
                    'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
                    'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
                    'Column13': 'cliActNo', 'Column14': 'cpCD', 'Column15': 'remarks',
                    'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
                    'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
                    'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
                    'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
                    'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
                    'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
                    'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
                }, inplace=True)
                df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
                df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
                df.expDt = df.expDt.astype('datetime64[ns]').dt.date
                df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
                df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
                pivot_df = df.pivot_table(
                    index=['ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
                    columns='bsFlg',
                    values=['trdQty', 'trdPrc'],
                    aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
                    fill_value=0
                )
                pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
                pivot_df = pivot_df.reset_index()
                pivot_df = pivot_df[
                    ['ctclid', 'sym', 'expDt', 'strPrc', 'optType', 'BuyVol', 'BuyPrc', 'SellVol', 'SellPrc']]
                pivot_df['expDt'] = pivot_df['expDt'].astype(str)
                pivot_df['IntradayVolume'] = pivot_df['BuyVol'] - pivot_df['SellVol']

                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                    pivot_df.to_excel(writer, index=False)
                buffer.seek(0)
                with gzip.open(zip_path, 'wb') as f:
                    f.write(buffer.getvalue())
                return FileResponse(zip_path, media_type='application/gzip')
        else:
            stt = datetime.now()
            total_rows = db.execute(text(rf'select count(*) from "{tablename}"')).scalar()
            page_size = 5_00_000
            num_pages = total_rows // page_size + (1 if (total_rows % page_size) else 0)
            logger.info(f'Total rows in DB: {total_rows}, Splitting into {num_pages} sheets')
            buffer = io.BytesIO()
            wb = xlsxwriter.Workbook(buffer, {'in_memory': True})
            for page in range(num_pages):
                query = f'select * from "{tablename}" limit {page_size} offset {(page) * page_size}'
                logger.info(query)
                pbar = progressbar.ProgressBar(
                    max_value=total_rows + 1,
                    widgets=[
                        progressbar.Percentage(), '',
                        progressbar.Bar(marker='=', left='[', right=']'),
                        progressbar.ETA()
                    ]
                )
                result = db.execute(text(query))
                ws = wb.add_worksheet(f'Sheet{page + 1}')
                for col, header in enumerate(result.keys()):
                    ws.write(0, col, header)
                for rn, row in enumerate(result, start=1):
                    for col, cell in enumerate(row):
                        ws.write(rn, col, cell)
                    pbar.update(rn)
                pbar.finish()
            wb.close()
            logger.info('fetching data from buffer')
            buffer.seek(0)
            logger.info('Writing to xlsx file and zipping . . ')
            with gzip.open(zip_path, 'wb') as f:
                f.write(buffer.getvalue())
            ett = datetime.now()
            logger.info(f'total time taken for zip_path:{(ett - stt).total_seconds()}')
            return FileResponse(path=zip_path, media_type='application/gzip')

    def get_intraday_net_position(self, for_date:date=Query()):
        for_dt = pd.to_datetime(for_date).date()
        if for_dt == today:
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{today}'
        else:
            tablename = f'NOTIS_DESK_WISE_NET_POSITION_{for_dt}'
        desk_db_df = read_data_db(for_table=tablename)
        desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
        desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
        desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
        desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x / 100 if x > 0 else x)
        desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
        grouped_desk_db_df = desk_db_df.groupby(by=['symbol', 'expiryDate', 'strikePrice', 'optionType']).agg(
            {'buyAvgQty': 'sum', 'buyAvgPrice': 'mean', 'sellAvgQty': 'sum', 'sellAvgPrice': 'mean'}).reset_index()
        grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
        grouped_desk_db_df.rename(columns={'buyAvgQty': 'buyQty', 'sellAvgQty': 'sellQty'})
        grouped_desk_db_df.expiryDate = grouped_desk_db_df.expiryDate.astype(str)
        json_data = grouped_desk_db_df.to_json(orient='records')
        if not len(grouped_desk_db_df):
            return Response(content=json_data, media_type='application/json')
        else:
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            return Response(content=compressed_data, media_type='application/gzip')

    def get_raw_net_position(self, for_date:date=Query()):
        for_dt = pd.to_datetime(for_date).date()
        if for_dt == today:
            tablename = f'notis_raw_data_{today}'
        else:
            tablename = f'notis_raw_data_{for_dt}'
        df = read_data_db(for_table=tablename)
        if not len(df):
            json_data = df.to_json(orient='records')
            return Response(content=json_data, media_type='application/json')
        else:
            list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
            list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
            list_none_str = [38]
            for i in list_str_int64:
                column_name = f'Column{i}'
                df[f'Column{i}'] = df[f'Column{i}'].astype('int64')
            for i in list_str_none:
                df[f'Column{i}'] = None
            for i in list_none_str:
                df[f'Column{i}'] = df[f'Column{i}'].astype('str')
            logger.info('Starting file modification...')
            df.rename(columns={
                'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
                'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
                'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
                'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
                'Column13': 'cliActNo', 'Column14': 'cpCD', 'Column15': 'remarks',
                'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
                'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
                'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
                'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
                'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
                'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
                'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
            }, inplace=True)
            df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
            df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
            df.expDt = df.expDt.astype('datetime64[ns]').dt.date
            df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
            df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
            pivot_df = df.pivot_table(
                index=['ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
                columns='bsFlg',
                values=['trdQty', 'trdPrc'],
                aggfunc={'trdQty': 'sum', 'trdPrc': 'mean'},
                fill_value=0
            )
            pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
            pivot_df = pivot_df.reset_index()
            pivot_df = pivot_df[['ctclid', 'sym', 'expDt', 'strPrc', 'optType', 'BuyVol', 'BuyPrc', 'SellVol', 'SellPrc']]
            pivot_df['expDt'] = pivot_df['expDt'].astype(str)
            pivot_df['IntradayVolume'] = pivot_df['BuyVol'] - pivot_df['SellVol']

            json_data = pivot_df.to_json(orient='records')
            # # return pivot_df.to_dict(orient='records')
            # compressed_data = gzip.compress(json_data.encode('utf-8'))
            # return Response(content=compressed_data, media_type='application/gzip')
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            return Response(content=compressed_data, media_type='application/gzip')

    def get_source_data(self, for_date='', for_table:str=Query(), page:int=Query(1), page_size:int=Query(1000), db:Session=Depends(get_db)):
        offset = (page - 1) * page_size
        if for_table == f'sourcenotisraw':
            tablename = "[ENetMIS].[dbo].[NSE_FO_AA100_view]"
            query = text(f"""
                WITH CTE AS (
                    SELECT *,
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                    FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]
                )
                SELECT *
                FROM CTE
                WHERE RowNum > {offset} AND RowNum <= {offset + page_size};
                """)
            result = db.execute(query).fetchall()
            total_rows = db.execute(text(rf'Select count(*) from [ENetMIS].[dbo].[NSE_FO_AA100_view]')).scalar()
        elif for_table == f'sourcebseraw':
            query = text(f"""
                WITH CTE AS (
                    SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker,
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                    FROM [OMNE_ARD_PRD].[dbo].[TradeHist]
                    WHERE mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')
                )
                SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker
                FROM CTE
                WHERE RowNum > {offset} AND RowNum <= {offset + page_size};
            """)
            query2 = text(f"""
                WITH CTE AS (
                    SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker,
                           ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                    FROM [OMNE_ARD_PRD_HNI].[dbo].[TradeHist]
                    WHERE mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')
                )
                SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker
                FROM CTE
                WHERE RowNum > {offset} AND RowNum <= {offset + page_size};
            """)
            result1 = db.execute(query).fetchall()
            result2 = db.execute(query2).fetchall()
            result = result1 + result2
            total_rows1 = db.execute(text(rf"""Select count(*) from [OMNE_ARD_PRD].[dbo].[TradeHist] WHERE mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')""")).scalar()
            total_rows2 = db.execute(text(rf"""Select count(*) from [OMNE_ARD_PRD_HNI].[dbo].[TradeHist] WHERE mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')""")).scalar()
            total_rows = total_rows1 + total_rows2
        json_data = {
            'data': [{k: conv_str(v) for k, v in row._mapping.items()} for row in result],
            'total_rows': total_rows,
            'page': page,
            'page_size': page_size
        }
        if not len(result):
            json_data = pd.DataFrame(columns=['data', 'total_rows', 'page', 'page_size']).to_json(orient='records')
            return Response(content=json_data, media_type='application/json')
        else:
            compressed_data = gzip.compress(json.dumps(json_data).encode('utf-8'))
            logger.info(
                f'\ntotal_rows={json_data["total_rows"]}\tpage={json_data["page"]}\tpage_size={json_data["page_size"]}\n')
            return Response(content=compressed_data, media_type='application/gzip')

    def download_source_data(self, for_date = '', for_table:str=Query(), db:Session=Depends(get_db)):
        stt = datetime.now()
        zip_path = os.path.join(zipped_dir, f'zipped_{for_table}.xlsx.gz')
        if for_table == f'sourcenotisraw':
            total_rows = db.execute(text(rf'Select count(*) from [ENetMIS].[dbo].[NSE_FO_AA100_view]')).scalar()
        elif for_table == f'sourcebseraw':
            total_rows1 = db.execute(text(
                rf"""Select count(*) from [OMNE_ARD_PRD].[dbo].[TradeHist] WHERE mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'""")).scalar()
            total_rows2 = db.execute(text(
                rf"""Select count(*) from [OMNE_ARD_PRD_HNI].[dbo].[TradeHist] WHERE mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'""")).scalar()
            total_rows = total_rows1 + total_rows2
        page_size = 5_00_000
        num_pages = total_rows // page_size + (1 if (total_rows % page_size) else 0)
        logger.info(f'Total rows in DB: {total_rows}, Splitting into {num_pages} sheets')
        buffer = io.BytesIO()
        wb = xlsxwriter.Workbook(buffer, {'in_memory': True})
        for page in range(num_pages):
            query2,query='',''
            offset = (page - 1) * page_size
            if for_table == f'sourcenotisraw':
                query = (f"""
                    WITH CTE AS (
                        SELECT *,
                               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                        FROM [ENetMIS].[dbo].[NSE_FO_AA100_view]
                    )
                    SELECT *
                    FROM CTE
                    WHERE RowNum > {page * page_size} AND RowNum <= {(page + 1) * page_size};
                """)
            elif for_table == f'sourcebseraw':
                query = (f"""
                    WITH CTE1 AS (
                        SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker,
                               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                        FROM [OMNE_ARD_PRD].[dbo].[TradeHist]
                        WHERE mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'
                    ),
                    CTE2 AS (
                        SELECT mnmFillPrice, mnmSegment, mnmTradingSymbol, mnmTransactionType, mnmAccountId, mnmUser, mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker,
                               ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                        FROM [OMNE_ARD_PRD_HNI].[dbo].[TradeHist]
                        WHERE mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'
                    )
                    SELECT * FROM CTE1
                    WHERE RowNum > {page * page_size} AND RowNum <= {(page + 1) * page_size}
                    UNION ALL
                    SELECT * FROM CTE2
                    WHERE RowNum > {page * page_size} AND RowNum <= {(page + 1) * page_size};
                """)
            logger.info(query)
            pbar = progressbar.ProgressBar(
                max_value=total_rows + 1,
                widgets=[
                    progressbar.Percentage(), '',
                    progressbar.Bar(marker='=', left='[', right=']'),
                    progressbar.ETA()
                ]
            )
            result = db.execute(text(query))
            ws = wb.add_worksheet(f'Sheet{page + 1}')
            for col, header in enumerate(result.keys()):
                ws.write(0, col, header)
            for rn, row in enumerate(result, start=1):
                for col, cell in enumerate(row):
                    ws.write(rn, col, cell)
                pbar.update(rn)
            pbar.finish()
        wb.close()
        logger.info('fetching data from buffer')
        buffer.seek(0)
        logger.info('Writing to xlsx file and zipping . . ')
        with gzip.open(zip_path, 'wb') as f:
            f.write(buffer.getvalue())
        ett = datetime.now()
        logger.info(f'total time taken for zip_path:{(ett - stt).total_seconds()}')
        return FileResponse(path=zip_path, media_type='application/gzip')

    def get_exposure(self, for_date:date=Query()):
        for_date = datetime.today().date().strftime('%Y-%m-%d')
        volt_df = read_file(os.path.join(volt_dir, f'FOVOLT_{yesterday.strftime("%d%m%Y")}.csv'))
        volt_df.columns = [re.sub(r'\s', '', each) for each in volt_df.columns]
        volt_df = volt_df.iloc[:, 1:3]
        volt_df.rename(columns={'UnderlyingClosePrice(A)': 'SpotClosePrice'}, inplace=True)
        sym_list = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY','SENSEX']
        volt_df = volt_df.query("Symbol in @sym_list")

        tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{for_date}'
        cp_df = read_data_db(for_table=tablename)
        cp_df.columns = [re.sub(r'Eod|\s', '', each) for each in cp_df.columns]

        merged_df = cp_df.merge(volt_df, how='left', left_on=['Underlying'], right_on=['Symbol'])
        merged_df.drop(columns=['Symbol'], inplace=True)
        merged_df = merged_df.query("OptionType == 'CE' or OptionType == 'PE'")
        merged_df.drop_duplicates(inplace=True)

        pivot_df = merged_df.pivot_table(
            index=['Broker', 'Underlying', 'SpotClosePrice'],
            columns=['OptionType'],
            values=['FinalNetQty'],
            aggfunc={'FinalNetQty': 'sum'},
            fill_value=0
        )
        pivot_df.columns = ['CE', 'PE']
        pivot_df.reset_index(inplace=True)
        pivot_df.SpotClosePrice = pivot_df.SpotClosePrice.astype('float64')
        pivot_df['NetQty'] = pivot_df['CE'] - pivot_df['PE']
        pivot_df['Exposure(in Crs)'] = (pivot_df['NetQty'] * pivot_df['SpotClosePrice']) / 10000000
        print(f'exposure table shape: {pivot_df.shape}')
        json_data = pivot_df.to_json(orient='records')
        if not pivot_df.empty:
            return Response(content=json_data, media_type='application/json')

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
    uvicorn.run('notis_app_per_minute:app', host='172.16.47.81', port=8871, workers=6)

