import re, warnings
import pandas as pd
import numpy as np
from datetime import datetime

from common import (today,yesterday,write_notis_postgredb,
                    write_notis_data, bse_dir, get_date_from_non_jiffy,
                    read_file, read_data_db, logger)

warnings.filterwarnings('ignore')

class BSEUtility:
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
        bse_raw_df['ExchangeTime'] = bse_raw_df['ExchangeTime'].astype(str)
        return bse_raw_df
    @staticmethod
    def bse_modify_file_v2(bse_raw_df):
        def convert_expiry(val):
            if len(val) == 5:
                val=val[:2]+'0'+val[2:]
            elif len(val) != 6:
                return None
            return datetime.strptime(val,'%y%m%d').date()
        
        pattern = r'^([A-Z]+)(\d+)(\d{5})([A-Z]{2})$'
        bse_raw_df[['Underlying','temp_expiry','Strike','OptionType']] = bse_raw_df['scid'].str.extract(pattern)
        bse_raw_df['Expiry'] = bse_raw_df['temp_expiry'].apply(convert_expiry)
        bse_raw_df['Segment'] = 'FO'
        bse_raw_df['SymbolName'] = 'BSXOPT'
        bse_raw_df['AvgPrice'] = bse_raw_df['rt']
        bse_raw_df['ExecutingBroker'] = 0
        bse_raw_df['Broker'] = 'non CP'
        bse_raw_df.rename(
            columns={'rt':'FillPrice','buy/sell':'TransactionType','clid':'AccountId','tdrid':'TerminalID',
                     'qty':'FillSize','scid':'TradingSymbol'},
            inplace=True
        )
        bse_raw_df['ExchangeTime'] = bse_raw_df['date'] + ' ' + bse_raw_df['time']
        col_keep = ['FillPrice','Segment','TradingSymbol','TransactionType','AccountId','TerminalID','FillSize','SymbolName','Expiry','OptionType','Strike','AvgPrice','ExecutingBroker','ExchangeTime','Underlying','Broker']
        bse_raw_df.drop(
            columns=[col for col in bse_raw_df.columns.tolist() if col not in col_keep],
            inplace=True
        )
        col_to_int = ['Strike', 'FillSize']
        col_to_float = ['FillPrice', 'AvgPrice']
        for col in col_to_int:
            bse_raw_df[col] = bse_raw_df[col].astype(np.int64)
        for col in col_to_float:
            bse_raw_df[col] = bse_raw_df[col].astype(float)
        return bse_raw_df
    
    @staticmethod
    def calc_bse_eod_net_pos(desk_bse_df):
        eod_df = read_data_db(for_table=f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}')
        # eod_df = eod_df.replace(' ', '', regex=True)
        # if len(desk_bse_df) == 0:
        #     eod_df.EodExpiry = pd.to_datetime(eod_df.EodExpiry, dayfirst=True, format='mixed').dt.date
        #     eod_df = eod_df.query("EodUnderlying == 'SENSEX' and EodExpiry >= @today and FinalNetQty != 0")
        #     grouped_eod_df = eod_df.groupby(
        #         by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
        #         as_index=False).agg({'FinalNetQty': 'sum'})
        #     return grouped_eod_df
        eod_df.columns = [re.sub(rf'Eod|\s|Expired', '', each) for each in eod_df.columns]
        eod_df.Expiry = pd.to_datetime(eod_df.Expiry, dayfirst=True, format='mixed').dt.date
        eod_df.drop(
            columns=['NetQuantity', 'buyQty', 'buyAvgPrice', 'buyValue','sellQty', 'sellAvgPrice', 'sellValue',
                     'PreFinalNetQty','Spot_close','Rate','Assn_value','SellValue','BuyValue','Qty'],
            inplace=True
        )
        eod_df.rename(columns={'FinalNetQty': 'NetQuantity'}, inplace=True)
        eod_df = eod_df.add_prefix('Eod')
        bse_underlying_list = ['SENSEX','BANKEX']
        eod_df = eod_df.query("EodUnderlying in @bse_underlying_list and EodExpiry >= @today and EodNetQuantity != 0 "
                              "and EodBroker != 'SRSPL'")
        grouped_eod_df = eod_df.groupby(by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
                                        as_index=False).agg({'EodNetQuantity': 'sum'})
        grouped_eod_df = grouped_eod_df.query("EodNetQuantity != 0")
        grouped_eod_df = grouped_eod_df.drop_duplicates()
        # ============================================================================================
        desk_bse_df.Expiry = pd.to_datetime(desk_bse_df.Expiry, dayfirst=True, format='mixed').dt.date
        grouped_desk_df = desk_bse_df.groupby(by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                                              as_index=False).agg({
                                                'BuyQty': 'sum', 'SellQty': 'sum', 'buyAvgPrice': 'mean', 'sellAvgPrice': 'mean',
                                                'buyValue':'sum', 'sellValue':'sum'
                                              })
        grouped_desk_df['IntradayVolume'] = grouped_desk_df['BuyQty'] - grouped_desk_df['SellQty']
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
        merged_df.drop(columns=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'], inplace=True)
        # ============================================================================================
        col_to_int = ['BuyQty', 'SellQty']
        for col in col_to_int:
            merged_df[col] = merged_df[col].astype(np.int64)
        merged_df.rename(columns={'BuyQty':'buyQty','SellQty':'sellQty'},inplace=True)
        print(f'length of cp noncp for {today} is {merged_df.shape}')
        return merged_df

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
            # write_notis_postgredb()