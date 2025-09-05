import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
import progressbar

from common import (get_date_from_non_jiffy, get_date_from_jiffy,
                    today, yesterday,
                    root_dir, logger, bhav_dir,
                    read_data_db, read_file)

class NSEUtility:
    @staticmethod
    def modify_file(df, df_nnf):
        list_str_int64 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 21, 23, 27, 28]
        list_str_none = [15, 20, 25, 30, 31, 32, 33, 34, 35, 36, 37]
        list_none_str = [38]
        for i in list_str_int64:
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
        df['ctclid'] = df['ctclid'].astype(np.int64)
        df_nnf['NNFID'] = df_nnf['NNFID'].astype(np.int64)
        missing_ctclid = set(df['ctclid'].unique()) - set(df_nnf['NNFID'].unique())
        if missing_ctclid: # logs the missing ctclids
            logger.info(f"Missing ctclid(s) from NNF file: {missing_ctclid}")
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
        merged_df.rename(columns={'CreateDate':'TrdDtTm'}, inplace=True)
        merged_df.drop(columns=['trdTm','ordTm'], inplace=True)
        merged_df = merged_df.drop_duplicates()
        return merged_df

    @staticmethod
    def calc_eod_cp_noncp(desk_db_df):
        # eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}' #NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
        # eod_df = read_data_db(for_table=eod_tablename)
        # eod_df.columns = [re.sub(r'Eod|\s|Expired','',each) for each in eod_df.columns]
        # eod_df.drop(columns=['NetQuantity','buyQty','buyAvgPrice','buyValue','sellQty','sellAvgPrice','sellValue',
        #                      'PreFinalNetQty','Spot_close','Rate','Assn_value','SellValue','BuyValue','Qty'],
        #             inplace=True)
        # eod_df.rename(columns={'FinalNetQty':'NetQuantity'}, inplace=True)
        # eod_df = eod_df.add_prefix('Eod')
        # eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True, format='mixed').dt.date
        # nse_underlying_list = ['NIFTY','BANKNIFTY','MIDCPNIFTY','FINNIFTY']
        # eod_df = eod_df.query("EodUnderlying in @nse_underlying_list and EodExpiry >= @today and EodNetQuantity != 0 "
        #                       "and EodBroker != 'SRSPL'")
        #
        # grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum'})
        underlying_list = ['NIFTY', 'BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY']
        yest_eod_df = read_data_db(for_table=f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}')
        yest_eod_df.EodExpiry = pd.to_datetime(yest_eod_df.EodExpiry, dayfirst=True, format='mixed').dt.date
        
        yest_eod_df = yest_eod_df.query("EodUnderlying in @underlying_list and EodExpiry >= @today and FinalNetQty != 0 and EodBroker in ['CP','non CP']")
        yest_eod_df['EodNetQuantity'] = yest_eod_df['FinalNetQty']
        yest_eod_df['PreFinalNetQty'] = yest_eod_df['FinalNetQty']
        exclude_columns = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType',
                           'EodNetQuantity', 'PreFinalNetQty', 'FinalNetQty']
        yest_eod_df.loc[:, ~yest_eod_df.columns.isin(exclude_columns)] = 0
        yest_eod_df = yest_eod_df.query('FinalNetQty != 0')
        
        today_eod_df = read_data_db(for_table=f'NOTIS_EOD_NET_POS_CP_NONCP_{today.strftime("%Y-%m-%d")}')
        today_eod_df.EodExpiry = pd.to_datetime(today_eod_df.EodExpiry, dayfirst=True, format='mixed').dt.date
        today_eod_df = today_eod_df.query(
            "EodUnderlying in @underlying_list and EodExpiry >= @today and EodBroker in ['CP','non CP']"
        )
        if len(desk_db_df) == 0 or desk_db_df.empty:
            if today_eod_df.empty or len(today_eod_df) == 0:
                return yest_eod_df
            return today_eod_df
        yest_eod_df.columns = [re.sub(rf'Eod|\s|Expired', '', each) for each in yest_eod_df.columns]
        yest_eod_df.Expiry = pd.to_datetime(yest_eod_df.Expiry, dayfirst=True, format='mixed').dt.date
        yest_eod_df.drop(
            columns=['NetQuantity', 'buyQty', 'buyAvgPrice', 'buyValue', 'sellQty', 'sellAvgPrice', 'sellValue',
                     'PreFinalNetQty', 'Spot_close', 'Rate', 'Assn_value', 'SellValue', 'BuyValue', 'Qty'],
            inplace=True
        )
        yest_eod_df.rename(columns={'FinalNetQty': 'NetQuantity'}, inplace=True)
        yest_eod_df = yest_eod_df.add_prefix('Eod')
        yest_eod_df = yest_eod_df.query(
            "EodUnderlying in @underlying_list and EodExpiry >= @today and EodNetQuantity != 0 and EodBroker in ['CP','non CP']")
        grouped_eod = yest_eod_df.groupby(
            by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
            as_index=False).agg({'EodNetQuantity': 'sum'})
        grouped_eod = grouped_eod.query("EodNetQuantity != 0")
        grouped_eod = grouped_eod.drop_duplicates()

        desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
        desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
        desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
        desk_db_df['expiryDate'] = pd.to_datetime(desk_db_df['expiryDate'], dayfirst=True, format='mixed').dt.date

        grouped_desk_db_df = desk_db_df.groupby(by=['broker','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({
            'buyAvgQty':'sum','buyAvgPrice':'mean','buyValue':'sum','sellAvgQty':'sum','sellAvgPrice':'mean','sellValue':'sum'
        }).reset_index()
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
        merged_df.fillna(0,inplace=True)
        logger.info(f'cp noncp length at {datetime.now()} is {merged_df.shape}')
        return merged_df

    @staticmethod
    def calc_deskwise_net_pos(pivot_df):
        pivot_df = pivot_df.copy()
        pivot_df.rename(columns ={'MainGroup':'mainGroup','SubGroup':'subGroup'}, inplace=True)
        desk_db_df = pivot_df.groupby(by=['mainGroup', 'subGroup', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
        return desk_db_df

    @staticmethod
    def calc_nnfwise_net_pos(pivot_df):
        pivot_df = pivot_df.copy()
        nnf_db_df = pivot_df.groupby(by=['ctclid', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
        nnf_db_df.rename(columns={'ctclid':'nnfID'}, inplace=True)
        return nnf_db_df
    
    @staticmethod
    def calc_nse_deal_sheet(pivot_df):
        pivot_df = pivot_df.copy()
        grouped_df = pd.DataFrame()
        if not pivot_df.empty or len(pivot_df) != 0:
            pivot_df.rename(
                columns=
                {'broker':'Broker','symbol':'Underlying','expiryDate':'Expiry','strikePrice':'Strike',
                 'optionType':'OptionType','buyAvgQty':'BuyQty','sellAvgQty':'SellQty',
                 'buyValue':'BuyValue','sellValue':'SellValue'},
                inplace=True)
            grouped_df = pivot_df.groupby(
                by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                as_index=False).agg(
                {'BuyMax':'max','SellMax':'max',
                 'BuyMin':lambda x: x[x > 0].min() if any(x > 0) else 0,
                 'SellMin':lambda x: x[x > 0].min() if any(x > 0) else 0,
                 'BuyQty':'sum','SellQty':'sum',
                 'BuyValue':'sum','SellValue':'sum'}
            )
            div_100_list = ['Strike','BuyMax','SellMax','BuyMin','SellMin']
            for each in div_100_list:
                grouped_df[each] = grouped_df[each].astype(np.float64)
                grouped_df[each] = grouped_df[each] / 100
            # grouped_df['Strike'] = np.where(grouped_df['OptionType'] == 'XX', 0, grouped_df['Strike'])
            mask = grouped_df['OptionType'] == 'XX'
            grouped_df.loc[mask,'Strike'] = 0
        return grouped_df