import re,pyodbc,psycopg2,os
import pandas as pd
import numpy as np
from urllib.parse import quote
import warnings
from datetime import datetime

from common import today,yesterday,write_notis_postgredb, write_notis_data, bse_dir, get_date_from_non_jiffy,read_file

warnings.filterwarnings('ignore')
def get_bse_trade():
    sql_server = '172.30.100.41'
    sql_port = '1450'
    sql_db = 'OMNE_ARD_PRD'
    sql_userid = 'Pos_User'
    sql_paswd = 'Pass@Word1'
    sql_paswd_encoded = quote(sql_paswd)
    # sql_query = "select * from [OMNE_ARD_PRD].[dbo].[TradeHist]"
    # sql_query = ("select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser,mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice"
    #              "from TradeHist "
    #              "where mnmAccountId='AA100' and mnmExchange='BSE'")
    # sql_query = ("select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from TradeHist where mnmAccountId='AA100' and mnmExchange='BSE'")
    sql_query = ("select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from TradeHist where (mnmSymbolName = 'BSXOPT' or mnmSymbolName = 'BSE')")
    # sql_query = ("select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from TradeHist where (mnmSymbolName = 'BSXOPT' or mnmSymbolName = 'BSE') and mnmExchangeTime between '27-Mar-2025 12:18:00' and '27-Mar-2025 12:19:00'")
    # sql_query = ("select top 10000 * from TradeHist where mnmSymbolName = 'BSXOPT' or mnmSymbolName = 'BSE'")
    try:
        sql_engine_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_server},{sql_port};"
            f"DATABASE={sql_db};"
            f"UID={sql_userid};"
            f"PWD={sql_paswd};"
        )
        with pyodbc.connect(sql_engine_str) as sql_conn:
            df_bse=pd.read_sql_query(sql_query,sql_conn)
        print(f'data fetched for bse: {df_bse.shape}')
    except (pyodbc.Error, psycopg2.Error) as e:
        print(f'Error in fetching data: {e}')
    df_bse = df_bse.query("mnmTransactionType != 'L'")
    df_bse.replace('', 0, inplace=True)
    # df_bse = read_file(os.path.join(bse_dir,'test_bse172025_1.xlsx'))
    df_bse.columns = [re.sub(r'mnm|\s','',each) for each in df_bse.columns]
    df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x:pd.to_datetime(int(x), unit='s').date())
    to_int_list = ['FillPrice', 'FillSize','StrikePrice']
    for each in to_int_list:
        df_bse[each] = df_bse[each].astype(np.int64)
    df_bse['AvgPrice'] = df_bse['AvgPrice'].astype(float).astype(np.int64)
    df_bse['StrikePrice'] = (df_bse['StrikePrice']/100).astype(np.int64)
    df_bse['Symbol'] = df_bse['TradingSymbol'].apply(lambda x:'SENSEX' if x.upper().startswith('SEN') else x)
    df_bse.rename(columns={'User':'TerminalID'}, inplace=True)
    pivot_df = df_bse.pivot_table(
        index=['TerminalID','Symbol','TradingSymbol','ExpiryDate','OptionType','StrikePrice','ExecutingBroker'],
        columns=['TransactionType'],
        values=['FillSize','AvgPrice'],
        aggfunc={'FillSize':'sum','AvgPrice':'mean'},
        fill_value=0
    )
    pivot_df.columns = ['BuyPrc','SellPrc','BuyVol','SellVol']
    pivot_df.reset_index(inplace=True)
    pivot_df['BSEIntradayVol'] = pivot_df.BuyVol - pivot_df.SellVol
    pivot_df.ExpiryDate = pivot_df.ExpiryDate.astype(str)
    pivot_df['ExpiryDate'] = [re.sub(r'1970.*','',each) for each in pivot_df['ExpiryDate']]
    to_int_list = ['BuyPrc','SellPrc','BuyVol','SellVol']
    for col in to_int_list:
        pivot_df[col] = pivot_df[col].astype(np.int64)
    write_notis_data(pivot_df,os.path.join(bse_dir,f'BSE_TRADE_DATA_{today.strftime("%Y-%m-%d")}.xlsx'))
    # write_notis_data(pivot_df,os.path.join(bse_dir,f'BSE_TRADE_DATA_{datetime(year=2025,month=3,day=17).strftime("%Y-%m-%d")}.xlsx'))
    write_notis_postgredb(pivot_df,table_name=f'BSE_TRADE_DATA_{today.strftime("%Y-%m-%d")}')
    # write_notis_postgredb(pivot_df,table_name=f'BSE_TRADE_DATA_{datetime(year=2025,month=3,day=17).strftime("%Y-%m-%d")}')

# get_bse_trade()