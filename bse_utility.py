import re, warnings, calendar
import pandas as pd
import numpy as np
from datetime import datetime

from common import (today, yesterday, holidays_25,
                    read_file, read_data_db)

warnings.filterwarnings('ignore')


def convert_expiry(val):
    # print(val)
    if re.fullmatch(r'\d{5}', val):
        val = val[:2] + '0' + val[2:]
        return datetime.strptime(val, '%y%m%d').date()
    elif re.fullmatch(r'\d{6}', val):
        return datetime.strptime(val, '%y%m%d').date()
    elif re.fullmatch(r'\d{2}[A-Z]{3}', val):
        # Find last tuesday(SENSEX) weekday=1
        year = 2000 + int(val[:2])
        month = datetime.strptime(val[2:], '%b').month
        start_date = datetime.today().replace(day=1).date()
        end_date = datetime(year=year, month=month, day=calendar.monthrange(year=year, month=month)[1]).date()
        
        b_days = pd.bdate_range(start=start_date, end=end_date, freq='C', weekmask='1111100',
                                holidays=holidays_25).date.tolist()
        # last_tuesday = find_tuesday(year=year, month=month)
        offset = (end_date.weekday() - 3) % 7
        last_thu = end_date.replace(day=end_date.day - offset)
        if last_thu in b_days:
            return last_thu
        else:
            b_days = [each for each in b_days if each < last_thu]
            return b_days[-1]

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
        pattern = r'^([A-Z]+)(\d{5}|\d{6}|\d{2}[A-Z]{3})(\d{5})?([A-Z]{2}|[A-Z]{3})$'
        bse_raw_df[['Underlying','temp_expiry','Strike','OptionType']] = bse_raw_df['scid'].str.extract(pattern)
        mask = bse_raw_df['OptionType'] == 'FUT'
        bse_raw_df.loc[mask,'Strike'] = 0
        bse_raw_df.loc[mask,'OptionType'] = 'XX'
        bse_raw_df['Expiry'] = bse_raw_df['temp_expiry'].apply(convert_expiry)
        bse_raw_df['Segment'] = 'FO'
        bse_raw_df['SymbolName'] = 'BSXOPT'
        bse_raw_df['AvgPrice'] = bse_raw_df['rt']
        # bse_raw_df['ExecutingBroker'] = 0
        # bse_raw_df['Broker'] = 'non CP'
        bse_raw_df['Broker'] = np.where(bse_raw_df['CpCode'], 'CP', 'non CP')
        
        bse_raw_df.rename(
            columns={'rt':'FillPrice','buy/sell':'TransactionType','clid':'AccountId','tdrid':'TraderID',
                     'qty':'FillSize','scid':'TradingSymbol'},
            inplace=True
        )
        bse_raw_df['ExchangeTime'] = bse_raw_df['date'] + ' ' + bse_raw_df['time']
        col_keep = ['FillPrice','Segment','TradingSymbol','TransactionType','AccountId','TerminalID','FillSize',
                    'SymbolName','Expiry','OptionType','Strike','AvgPrice','ExecutingBroker','ExchangeTime','Underlying','Broker','TraderID']
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
        underlying_list = ['SENSEX','BANKEX']
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
        today_eod_df = today_eod_df.query("EodUnderlying in @underlying_list and EodExpiry >= @today and EodBroker in ['CP','non CP']")
        
        if len(desk_bse_df) == 0 or desk_bse_df.empty:
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
        grouped_eod_df = yest_eod_df.groupby(
            by=['EodBroker', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'],
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
            
    @staticmethod
    def calc_bse_deal_sheet(pivot_df):
        pivot_df = pivot_df.copy()
        grouped_pivot_df = pd.DataFrame()
        if not pivot_df.empty or len(pivot_df) != 0:
            pivot_df.rename(
                columns={'buyMax':'BuyMax','sellMax':'SellMax',
                         'buyMin':'BuyMin','sellMin':'SellMin',
                         'buyValue':'BuyValue','sellValue':'SellValue'},
                inplace=True
            )
            grouped_pivot_df = pivot_df.groupby(
                by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
                as_index=False).agg(
                {'BuyMax': 'max', 'SellMax': 'max',
                 'BuyMin': lambda x: x[x > 0].min() if any(x > 0) else 0,
                 'SellMin': lambda x: x[x > 0].min() if any(x > 0) else 0,
                 'BuyQty': 'sum','SellQty': 'sum',
                 'BuyValue':'sum','SellValue':'sum'}
            )
            div_100_list = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin']
            for each in div_100_list:
                grouped_pivot_df[each] = grouped_pivot_df[each].astype(np.float64)
                grouped_pivot_df[each] = grouped_pivot_df[each] / 100
        return grouped_pivot_df