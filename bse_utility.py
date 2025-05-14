import re,pyodbc,psycopg2,os, warnings
import pandas as pd
import numpy as np
from urllib.parse import quote
from datetime import datetime

from common import (today,yesterday,write_notis_postgredb,
                    write_notis_data, bse_dir, get_date_from_non_jiffy,
                    read_file, read_data_db, logger)

warnings.filterwarnings('ignore')

class BSEUtility:
    # @staticmethod
    # def get_bse_trade_data(from_time:str='',to_time:str=''):
    #     df_bse = read_data_db(for_table='TradeHist', from_time=from_time, to_time=to_time)
    #     df_bse = df_bse.query("mnmTransactionType != 'L'")
    #     if df_bse.empty:
    #         # print(f'No data for {from_time} hence skipping')
    #         logger.info("No data for today hence skipping")
    #         return
    #     df_bse.replace('', 0, inplace=True)
    #     df_bse.columns = [re.sub(r'mnm|\s', '', each) for each in df_bse.columns]
    #     # df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x:pd.to_datetime(int(x), unit='s').date() if x !='' else x)
    #     df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x: pd.to_datetime(int(x), unit='s').date())
    #     # df_bse.replace('', 0, inplace=True)
    #     to_int_list = ['FillPrice', 'FillSize', 'StrikePrice']
    #     for each in to_int_list:
    #         df_bse[each] = df_bse[each].astype(np.int64)
    #     df_bse['AvgPrice'] = df_bse['AvgPrice'].astype(float).round(2)
    #     df_bse['StrikePrice'] = (df_bse['StrikePrice'] / 100).astype(np.int64)
    #     df_bse['Symbol'] = df_bse['TradingSymbol'].apply(lambda x: 'SENSEX' if x.upper().startswith('SEN') else x)
    #     df_bse.rename(columns={'User': 'TerminalID'}, inplace=True)
    #     pivot_df = df_bse.pivot_table(
    #         index=['TerminalID', 'Symbol', 'TradingSymbol', 'ExpiryDate', 'OptionType', 'StrikePrice',
    #                'ExecutingBroker'],
    #         columns=['TransactionType'],
    #         values=['FillSize', 'AvgPrice'],
    #         aggfunc={'FillSize': 'sum', 'AvgPrice': 'mean'},
    #         fill_value=0
    #     )
    #     if len(df_bse.TransactionType.unique()) == 1:
    #         if df_bse.TransactionType.unique().tolist()[0] == 'B':
    #             pivot_df['SellAvgPrc'] = 0;
    #             pivot_df['SellQty'] = 0
    #             pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    #         elif df_bse.TransactionType.unique().tolist()[0] == 'S':
    #             pivot_df['BuyAvgPrc'] = 0;
    #             pivot_df['BuyQty'] = 0
    #             pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    #     elif len(df_bse) == 0 or len(pivot_df) == 0:
    #         pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    #     else:
    #         pivot_df.columns = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    #     pivot_df.reset_index(inplace=True)
    #     pivot_df['BSEIntradayVol'] = pivot_df.BuyVol - pivot_df.SellVol
    #     pivot_df.ExpiryDate = pivot_df.ExpiryDate.astype(str)
    #     pivot_df['ExpiryDate'] = [re.sub(r'1970.*', '', each) for each in pivot_df['ExpiryDate']]
    #     to_int_list = ['BuyPrc', 'SellPrc', 'BuyVol', 'SellVol']
    #     for col in to_int_list:
    #         pivot_df[col] = pivot_df[col].astype(np.int64)
    #     logger.info(f'pivot shape: {pivot_df.shape}')
    #     return pivot_df

    @staticmethod
    def calc_bse_eod_net_pos(desk_bse_df):
        # read prev day eod table and group it
        # read today's data and group it
        # merge both grouped data, yesterday>today
        eod_df = read_data_db(for_table=f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}')
        # eod_df = eod_df.replace(' ', '', regex=True)
        if len(desk_bse_df) == 0:
            eod_df.Expiry = pd.to_datetime(eod_df.Expiry, dayfirst=True, format='mixed').dt.date
            eod_df = eod_df.query("EodUnderlying == 'SENSEX' and EodExpiry >= @today and FinalNetQty != 0")
            grouped_eod_df = eod_df.groupby(
                by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                as_index=False).agg({'FinalNetQty': 'sum', 'EodClosingPrice': 'sum'})
            return grouped_eod_df
        eod_df.columns = [re.sub(rf'Eod|\s', '', each) for each in eod_df.columns]
        eod_df.Expiry = pd.to_datetime(eod_df.Expiry, dayfirst=True, format='mixed').dt.date
        # eod_df = eod_df.query("Underlying == 'SENSEX' and Expiry >= @today and FinalNetQty != 0")
        # if len(desk_bse_df) == 0:
        #     return eod_df
        eod_df.drop(
            columns=['NetQuantity', 'buyQty', 'buyAvgPrice', 'sellQty', 'sellAvgPrice', 'IntradayVolume',
                     'ClosingPrice'],
            inplace=True
        )
        eod_df.rename(columns={'FinalNetQty': 'NetQuantity', 'FinalSettlementPrice': 'ClosingPrice'}, inplace=True)
        eod_df = eod_df.add_prefix('Eod')
        eod_df = eod_df.query("EodUnderlying == 'SENSEX' and EodExpiry >= @today and EodNetQuantity != 0")
        grouped_eod_df = eod_df.groupby(by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                        as_index=False).agg({'EodNetQuantity': 'sum', 'EodClosingPrice': 'mean'})
        grouped_eod_df = grouped_eod_df.query("EodUnderlying == 'SENSEX' and EodExpiry >= @today and EodNetQuantity != 0")
        # ============================================================================================
        grouped_desk_df = desk_bse_df.groupby(by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                                              as_index=False).agg(
            {'BuyQty': 'sum', 'SellQty': 'sum', 'buyAvgPrice': 'mean', 'sellAvgPrice': 'mean', 'IntradayVolume': 'sum'})
        # grouped_desk_df['IntradayVolume'] = grouped_desk_df['BuyQty'] - grouped_desk_df['SellQty']
        # ============================================================================================
        if len(grouped_eod_df) > len(grouped_desk_df):
            merged_df = grouped_eod_df.merge(
                grouped_desk_df,
                left_on=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                right_on=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                how='outer'
            )
        else:
            merged_df = grouped_desk_df.merge(
                grouped_eod_df,
                right_on=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                left_on=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                how='outer'
            )
        merged_df.fillna(0, inplace=True)
        merged_df.drop_duplicates(inplace=True)
        # ============================================================================================
        coltd1 = ['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
        coltd2 = ['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType']
        for i in range(len(coltd1)):
            merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
            merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
        merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
        merged_df['FinalSettlementPrice'] = 0
        merged_df.drop(columns=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'], inplace=True)
        # ============================================================================================
        col_to_int = ['BuyQty', 'SellQty', 'FinalSettlementPrice']
        for col in col_to_int:
            merged_df[col] = merged_df[col].astype(np.int64)
        merged_df.rename(columns={'BuyQty':'buyQty','SellQty':'sellQty'},inplace=True)
        print(f'length of cp noncp for {today} is {merged_df.shape}')
        return merged_df

    @staticmethod
    def bse_modify_file(bse_raw_df):
        bse_raw_df = bse_raw_df.query("mnmTransactionType != 'L'")
        bse_raw_df.replace('', 0, inplace=True)
        bse_raw_df.columns = [re.sub(r'mnm|\s', '', each) for each in bse_raw_df.columns]
        bse_raw_df.ExpiryDate = bse_raw_df.ExpiryDate.apply(lambda x: pd.to_datetime(int(x), unit='s').date())
        to_int_list = ['FillPrice', 'FillSize', 'StrikePrice']
        for each in to_int_list:
            bse_raw_df[each] = bse_raw_df[each].astype(np.int64)
        bse_raw_df['AvgPrice'] = bse_raw_df['AvgPrice'].astype(float).round(2)
        bse_raw_df['StrikePrice'] = (bse_raw_df['StrikePrice'] / 100).astype(np.int64)
        bse_raw_df['Symbol'] = bse_raw_df['TradingSymbol'].apply(
            lambda x: 'SENSEX' if x.upper().startswith('SEN') else x)
        bse_raw_df['Broker'] = bse_raw_df['AccountId'].apply(lambda x: 'non CP' if x.upper().startswith('AA') else 'CP')
        bse_raw_df.rename(
            columns={'User': 'TerminalID', 'Symbol': 'Underlying', 'ExpiryDate': 'Expiry', 'StrikePrice': 'Strike'},
            inplace=True)
        return bse_raw_df

    @staticmethod
    def add_to_bse_eod_net_pos(for_date: str = ''):
        if not for_date:
            print(f'for_date is empty')
        else:
            sent_df = read_file(
                rf"D:\notis_analysis\eod_original\EOD Net position {for_date.strftime('%d%m%Y')} BSE.xlsx")
            sent_df.columns = [re.sub(rf'\s|\.', '', each) for each in sent_df.columns]
            sent_df.ExpiryDate = pd.to_datetime(sent_df.ExpiryDate, dayfirst=True, format='mixed').dt.date
            sent_df['Broker'] = sent_df.apply(lambda row: 'CP' if row['PartyCode'].upper().endswith('CP') else 'non CP',
                                              axis=1)
            sent_df['OptionType'] = sent_df.apply(
                lambda row: 'XX' if row['OptionType'].upper().startswith('F') else row['OptionType'], axis=1)
            sent_df.drop(columns=['PartyCode'], inplace=True)
            sent_df.rename(columns={'Symbol': 'Underlying', 'ExpiryDate': 'Expiry', 'StrikePrice': 'Strike'},
                           inplace=True)
            sent_df = sent_df.add_prefix('Eod')
            sent_df.rename(columns={'EodNetQty': 'FinalNetQty'}, inplace=True)
            col_to_add = ['EodNetQuantity', 'EodClosingPrice', 'buyQty', 'buyAvgPrice', 'sellQty', 'sellAvgPrice',
                          'IntradayVolume', 'FinalSettlementPrice']
            for col in col_to_add:
                sent_df[col] = 0
            truncated_sent_df = sent_df.query('EodUnderlying == "SENSEX"')

            eod_df = read_data_db(for_table=f'NOTIS_EOD_NET_POS_CP_NONCP_{for_date}')
            eod_df.EodExpiry = pd.to_datetime(eod_df.EodExpiry, dayfirst=True, format='mixed').dt.date

            concat_eod_df = pd.concat([eod_df, truncated_sent_df], ignore_index=True)
            write_notis_postgredb()
        u = 0

