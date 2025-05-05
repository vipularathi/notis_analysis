import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
from dateutil.relativedelta import relativedelta
import progressbar
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pyodbc
from sqlalchemy import create_engine, text, insert
import psycopg2
import time
import warnings
from db_config import engine_str, n_tbl_notis_nnf_data
from common import get_date_from_non_jiffy, get_date_from_jiffy, today, yesterday, root_dir, logger, read_data_db, read_file, bhav_dir

class NSEUtility:
    @staticmethod
    def modify_file(df, df_nnf):
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
        # --------------------------------------------------------------------------------------------------------------------------------
        pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
        logger.info('Starting file modification...')
        # pbar.update(0)
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
        df['ctclid'] = df['ctclid'].astype('float64')
        df_nnf['NNFID'] = df_nnf['NNFID'].astype('float64')
        # proceed only if all ctclid from notis file is present in nnf file or not
        missing_ctclid = set(df['ctclid'].unique()) - set(df_nnf['NNFID'].unique())
        if missing_ctclid:
            logger.info(f"Missing ctclid(s) from NNF file: {missing_ctclid}")
            # raise ValueError(f'The ctclid values are not matching the NNFID values - {missing_ctclid}')
        else:
            logger.info('All ctclid values are present in NNF file.\n')
        pbar.update(20)

        df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(int(x)))
        pbar.update(40)

        df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(int(x)))
        pbar.update(60)

        df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(int(x)))
        pbar.update(80)
        df.ordTm = df.ordTm.astype('datetime64[ns]')
        df.ordTm = df.ordTm.dt.strftime('%d-%m-%Y %H:%M:%S')
        df.expDt = df.expDt.astype('datetime64[ns]')
        df.expDt = df.expDt.dt.date
        df.trdTm = df.trdTm.astype('datetime64[ns]')
        df.trdTm = df.trdTm.dt.strftime('%d-%m-%Y %H:%M:%S')
        # --------------------------------------------------------------------------------------------------------------------------------
        df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
        pbar.update(90)

        df['remarks'] = df['cpCD'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')
        df.rename(columns={'remarks': 'broker'}, inplace=True)
        pbar.update(95)

        merged_df = pd.merge(df, df_nnf, left_on='ctclid', right_on='NNFID', how='left')
        merged_df.drop(columns=['NNFID'], axis=1, inplace=True)
        pbar.update(100)
        # --------------------------------------------------------------------------------------------------------------------------------
        pbar.finish()
        merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']] = merged_df[['TerminalID', 'TerminalName', 'UserID', 'SubGroup', 'MainGroup', 'NeatID']].fillna('NONE')
        merged_df['CreateDate'] = merged_df['CreateDate'].astype(str)
        merged_df = merged_df.drop_duplicates()
        return merged_df

    @staticmethod
    def calc_eod_cp_noncp(desk_db_df):
        eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}' #NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
        eod_df = read_data_db(for_table=eod_tablename)
        eod_df.columns = [re.sub(r'Eod|\s','',each) for each in eod_df.columns]
        # Underlying	Strike	Option Type	Expiry	Net Quantity	Settlement Price
        eod_df.drop(columns=['NetQuantity','buyQty','buyAvgPrice','sellQty','sellAvgPrice','IntradayVolume','ClosingPrice'], inplace=True)
        eod_df.rename(columns={'FinalNetQty':'NetQuantity','FinalSettlementPrice':'ClosingPrice'}, inplace=True)
        eod_df = eod_df.add_prefix('Eod')
        # eod_df.EodExpiry = eod_df.EodExpiry.astype('datetime64[ns]')
        # eod_df.EodExpiry = eod_df.EodExpiry.dt.date
        eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True, format='mixed').dt.date
        # eod_df['EodExpiry'] = eod_df['EodExpiry'].dt.date
        eod_df = eod_df.query("EodExpiry >= @today and EodNetQuantity != 0")

        grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum','EodClosingPrice':'mean'})
        grouped_eod = grouped_eod.query("EodNetQuantity != 0")
        grouped_eod = grouped_eod.drop_duplicates()

        desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
        desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
        desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
        # desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
        # desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
        desk_db_df['expiryDate'] = pd.to_datetime(desk_db_df['expiryDate'], dayfirst=True, format='mixed').dt.date
        # desk_db_df['broker'] = desk_db_df['brokerID'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')

        grouped_desk_db_df = desk_db_df.groupby(by=['broker','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
        grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
        grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'}, inplace=True)
        # ================================================================
        merged_df = grouped_eod.merge(grouped_desk_db_df, left_on=['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], right_on=["broker","symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
        merged_df.fillna(0, inplace=True)
        merged_df = merged_df.drop_duplicates()

        coltd1 = ['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
        coltd2 = ["broker","symbol", "expiryDate", "strikePrice", "optionType"]
        for i in range(len(coltd1)):
            merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
            merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
        merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
        merged_df.drop(columns = ['broker','symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace = True)

        if datetime.strptime('16:00:00', '%H:%M:%S').time() < datetime.now().time():
            bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
            bhav_matched_files = [f for f in os.listdir(bhav_dir) if re.match(bhav_pattern, f)]
            bhav_df = read_file(os.path.join(bhav_dir, bhav_matched_files[0])) # regularBhavcopy_14012025.xlsx
            bhav_df.columns = bhav_df.columns.str.replace(' ', '')
            bhav_df.rename(columns={'VWAPclose':'closingPrice'}, inplace=True)
            bhav_df.columns = bhav_df.columns.str.capitalize()
            bhav_df = bhav_df.add_prefix('Bhav')
            bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
            bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
            bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
            bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
            col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
            bhav_df = bhav_df[col_keep]
            bhav_df = bhav_df.drop_duplicates()

            merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
            merged_bhav_df.drop(columns = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace = True)
        else:
            merged_bhav_df = merged_df.copy()
            merged_bhav_df['BhavClosingprice'] = 0

        merged_bhav_df.fillna(0,inplace=True)
        merged_bhav_df.buyAvgPrice = merged_bhav_df.buyAvgPrice.astype('int64')
        merged_bhav_df.sellAvgPrice = merged_bhav_df.sellAvgPrice.astype('int64')
        merged_bhav_df.BhavClosingprice = merged_bhav_df.BhavClosingprice.astype('int64')
        merged_bhav_df.rename(columns = {'BhavClosingprice':'FinalSettlementPrice'}, inplace = True)
        logger.info(f'cp noncp length at {datetime.now()} is {merged_bhav_df.shape}')
        # for col in merged_bhav_df.columns:
        #     if type(merged_bhav_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
        #         print(f'nse_utility changing col- {col}')
        #         merged_bhav_df[col] = pd.to_datetime(merged_bhav_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
        return merged_bhav_df

    @staticmethod
    def calc_deskwise_net_pos(pivot_df):
        pivot_df.rename(columns ={'MainGroup':'mainGroup','SubGroup':'subGroup'}, inplace=True)
        desk_db_df = pivot_df.groupby(by=['mainGroup', 'subGroup', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
        # for col in desk_db_df.columns:
        #     if type(desk_db_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
        #         print(f'nse_utility changing col- {col}')
        #         desk_db_df[col] = pd.to_datetime(desk_db_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
        return desk_db_df

    @staticmethod
    def calc_nnfwise_net_pos(pivot_df):
        nnf_db_df = pivot_df.groupby(by=['ctclid', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
        nnf_db_df.rename(columns={'ctclid':'nnfID'}, inplace=True)
        # for col in nnf_db_df.columns:
        #     if type(nnf_db_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
        #         print(f'nse_utility changing col- {col}')
        #         nnf_db_df[col] = pd.to_datetime(nnf_db_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
        return nnf_db_df