import re, os, progressbar, pyodbc, warnings, psycopg2, time, warnings
import pandas as pd
#
warnings.filterwarnings('ignore')
# from common import (read_data_db, read_file, write_notis_data, write_notis_postgredb, download_bhavcopy,
#                     root_dir, bhav_dir, modified_dir, table_dir, bse_dir)
#
# today=datetime.now().replace(day=9).date()
# # yesterday=datetime.now().replace(day=4).date()
# yesterday=today-timedelta(days=1)
# warnings.filterwarnings('ignore', message="pandas only supports SQLAlchemy connectable.*")
# pd.set_option('display.float_format', lambda a:'%.2f' %a)
#
# n_tbl_notis_desk_wise_net_position = f"NOTIS_DESK_WISE_NET_POSITION_{today}"
# n_tbl_notis_eod_net_pos_cp_noncp=f"NOTIS_EOD_NET_POS_CP_NONCP_{today}"
# n_tbl_notis_nnf_wise_net_position=f"NOTIS_NNF_WISE_NET_POSITION_{today}"
# class NSEUtility:
#     @staticmethod
#     def calc_eod_cp_noncp(desk_db_df):
#         eod_tablename = f'NOTIS_EOD_NET_POS_CP_NONCP_{yesterday.strftime("%Y-%m-%d")}' #NOTIS_EOD_NET_POS_CP_NONCP_2025-03-17
#         eod_df = read_data_db(for_table=eod_tablename)
#         eod_df.columns = [re.sub(r'Eod|\s','',each) for each in eod_df.columns]
#         # Underlying	Strike	Option Type	Expiry	Net Quantity	Settlement Price
#         eod_df.drop(columns=['NetQuantity','buyQty','buyAvgPrice','sellQty','sellAvgPrice','IntradayVolume','ClosingPrice'], inplace=True)
#         eod_df.rename(columns={'FinalNetQty':'NetQuantity','FinalSettlementPrice':'ClosingPrice'}, inplace=True)
#         eod_df = eod_df.add_prefix('Eod')
#
#         eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True, format='mixed').dt.date
#         # eod_df['EodExpiry'] = eod_df['EodExpiry'].dt.date
#         eod_df = eod_df.query("EodExpiry >= @today and EodNetQuantity != 0")
#
#         grouped_eod = eod_df.groupby(by=['EodBroker','EodUnderlying','EodExpiry','EodStrike','EodOptionType'], as_index=False).agg({'EodNetQuantity':'sum','EodClosingPrice':'mean'})
#         grouped_eod = grouped_eod.query("EodNetQuantity != 0")
#         grouped_eod = grouped_eod.drop_duplicates()
#
#         desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
#         desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
#         desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
#         desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
#         desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
#         # desk_db_df['broker'] = desk_db_df['brokerID'].apply(lambda x: 'CP' if x.startswith('Y') else 'non CP')
#
#         grouped_desk_db_df = desk_db_df.groupby(by=['broker','symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
#         grouped_desk_db_df['IntradayVolume'] = grouped_desk_db_df['buyAvgQty'] - grouped_desk_db_df['sellAvgQty']
#         grouped_desk_db_df.rename(columns={'buyAvgQty':'buyQty','sellAvgQty':'sellQty'}, inplace=True)
#         # ================================================================
#         merged_df = grouped_eod.merge(grouped_desk_db_df, left_on=['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], right_on=["broker","symbol", "expiryDate", "strikePrice", "optionType"], how='outer')
#         merged_df.fillna(0, inplace=True)
#         merged_df = merged_df.drop_duplicates()
#
#         coltd1 = ['EodBroker','EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType']
#         coltd2 = ["broker","symbol", "expiryDate", "strikePrice", "optionType"]
#         for i in range(len(coltd1)):
#             merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
#             merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
#         merged_df['FinalNetQty'] = merged_df['EodNetQuantity'] + merged_df['IntradayVolume']
#         merged_df.drop(columns = ['broker','symbol', 'expiryDate', 'strikePrice', 'optionType'], inplace = True)
#
#         # if datetime.strptime('16:00:00', '%H:%M:%S').time() < datetime.now().time():
#         #     bhav_pattern = rf'regularNSEBhavcopy_{today.strftime("%d%m%Y")}.(xlsx|csv)'
#         #     bhav_matched_files = [f for f in os.listdir(bhav_dir) if re.match(bhav_pattern, f)]
#         #     bhav_df = read_file(os.path.join(bhav_dir, bhav_matched_files[0])) # regularBhavcopy_14012025.xlsx
#         #     bhav_df.columns = bhav_df.columns.str.replace(' ', '')
#         #     bhav_df.rename(columns={'VWAPclose':'closingPrice'}, inplace=True)
#         #     bhav_df.columns = bhav_df.columns.str.capitalize()
#         #     bhav_df = bhav_df.add_prefix('Bhav')
#         #     bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x)).date())
#         #     bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
#         #     bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
#         #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
#         #     bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
#         #     col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
#         #     bhav_df = bhav_df[col_keep]
#         #     bhav_df = bhav_df.drop_duplicates()
#         #
#         #     merged_bhav_df = merged_df.merge(bhav_df, left_on=["EodUnderlying", "EodExpiry", "EodStrike", "EodOptionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
#         #     merged_bhav_df.drop(columns = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], inplace = True)
#         # else:
#         #     merged_bhav_df = merged_df.copy()
#         #     merged_bhav_df['BhavClosingprice'] = 0
#         merged_bhav_df = merged_df.copy()
#         merged_bhav_df['BhavClosingprice'] = 0
#
#         merged_bhav_df.fillna(0,inplace=True)
#         merged_bhav_df.buyAvgPrice = merged_bhav_df.buyAvgPrice.astype('int64')
#         merged_bhav_df.sellAvgPrice = merged_bhav_df.sellAvgPrice.astype('int64')
#         merged_bhav_df.BhavClosingprice = merged_bhav_df.BhavClosingprice.astype('int64')
#         merged_bhav_df.rename(columns = {'BhavClosingprice':'FinalSettlementPrice'}, inplace = True)
#         print(f'cp noncp length at {datetime.now()} is {merged_bhav_df.shape}')
#         # for col in merged_bhav_df.columns:
#         #     if type(merged_bhav_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
#         #         print(f'nse_utility changing col- {col}')
#         #         merged_bhav_df[col] = pd.to_datetime(merged_bhav_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
#         return merged_bhav_df
#
#     @staticmethod
#     def calc_deskwise_net_pos(pivot_df):
#         pivot_df.rename(columns ={'MainGroup':'mainGroup','SubGroup':'subGroup'}, inplace=True)
#         desk_db_df = pivot_df.groupby(by=['mainGroup', 'subGroup', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
#         # for col in desk_db_df.columns:
#         #     if type(desk_db_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
#         #         print(f'nse_utility changing col- {col}')
#         #         desk_db_df[col] = pd.to_datetime(desk_db_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
#         return desk_db_df
#
#     @staticmethod
#     def calc_nnfwise_net_pos(pivot_df):
#         nnf_db_df = pivot_df.groupby(by=['ctclid', 'symbol', 'expiryDate', 'strikePrice', 'optionType']).agg({'buyAvgQty':'sum','buyAvgPrice':'mean','sellAvgQty':'sum','sellAvgPrice':'mean'}).reset_index()
#         nnf_db_df.rename(columns={'ctclid':'nnfID'}, inplace=True)
#         # for col in nnf_db_df.columns:
#         #     if type(nnf_db_df[col][0]) == type(pd.to_datetime('2025-04-04').date()):
#         #         print(f'nse_utility changing col- {col}')
#         #         nnf_db_df[col] = pd.to_datetime(nnf_db_df[col], dayfirst=True, format='mixed').dt.strftime('%d/%m/%Y')
#         return nnf_db_df
#
# def download_tables():
#     table_list = [n_tbl_notis_desk_wise_net_position, n_tbl_notis_nnf_wise_net_position,n_tbl_notis_eod_net_pos_cp_noncp]
#     # today = datetime(year=2025, month=1, day=10).date().strftime('%Y_%m_%d').upper()
#     for table in table_list:
#         df = read_data_db(for_table=table)
#         # df.to_csv(os.path.join(table_dir, f"{table}.xlsx"), index=False)
#         write_notis_data(df=df, filepath=os.path.join(table_dir,f'{table}.xlsx'))
#         print(f"{table} data fetched and written at path: {os.path.join(table_dir, f'{table}.xlsx')}")
# def main():
#     modified_df = read_file(os.path.join(modified_dir, f'NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')) #sample=NOTIS_TRADE_DATA_08APR2025
#     modified_df.ctclid = modified_df.ctclid.astype(np.float64)
#     modified_df.trdQty = modified_df.trdQty.astype(np.int64)
#     modified_df.trdPrc = modified_df.trdPrc.astype(np.float64)
#     modified_df.expDt = pd.to_datetime(modified_df.expDt, dayfirst=True, format='mixed').dt.date
#     modified_df.strPrc = modified_df.strPrc.astype(np.int64)
#     modified_df['trdQtyPrc'] = modified_df['trdQty'] * modified_df['trdPrc']
#     pivot_df = modified_df.pivot_table(
#         index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
#         columns=['bsFlg'],
#         values=['trdQty', 'trdQtyPrc'],
#         aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum'},
#         fill_value=0
#     )
#     if len(modified_df.bsFlg.unique()) == 1:
#         if modified_df.bsFlg.unique().tolist()[0] == 'B':
#             pivot_df['SellTrdQtyPrc'] = 0;
#             pivot_df['SellQty'] = 0
#         elif modified_df.bsFlg.unique().tolist()[0] == 'S':
#             pivot_df['BuyTrdQtyPrc'] = 0;
#             pivot_df['BuyQty'] = 0
#     elif len(modified_df) == 0 or len(pivot_df) == 0:
#         pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
#     pivot_df.columns = ['BuyQty', 'SellQty', 'BuyTrdQtyPrc', 'SellTrdQtyPrc']
#     pivot_df['BuyAvgPrc'] = pivot_df.apply(lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0,
#                                            axis=1)
#     pivot_df['SellAvgPrc'] = pivot_df.apply(
#         lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0, axis=1)
#     pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
#     pivot_df.reset_index(inplace=True)
#     pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
#                              'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice',
#                              'SellAvgPrc': 'sellAvgPrice', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty'},
#                     inplace=True)
#     pivot_df['volume'] = pivot_df.buyAvgQty - pivot_df.sellAvgQty
#     # DESK
#     dsk_db_df = NSEUtility.calc_deskwise_net_pos(pivot_df)
#     write_notis_postgredb(dsk_db_df, table_name=n_tbl_notis_desk_wise_net_position, truncate_required=True)
#     # NNF
#     nnf_db_df = NSEUtility.calc_nnfwise_net_pos(pivot_df)
#     write_notis_postgredb(nnf_db_df, table_name=n_tbl_notis_nnf_wise_net_position, truncate_required=True)
#     # CP NONCP
#     cp_noncp_db_df = NSEUtility.calc_eod_cp_noncp(pivot_df)
#     write_notis_postgredb(cp_noncp_db_df, table_name=n_tbl_notis_eod_net_pos_cp_noncp, truncate_required=True)
#     p=0
# if __name__ == '__main__':
#     # download_bhavcopy()
#     # print(f'Today\'s bhavcopy downloaded and stored at {bhav_dir}')
#     stt = time.time()
#     main()
#     ett = time.time()
#     print(f'total time taken for modifying, adding data in db and writing in local directory - {ett - stt} seconds')
#     # print(f'fetching BSE trades...')
#     # stt = datetime.now()
#     # df_bse = BSEUtility.get_bse_trade_data()
#     # write_notis_data(df_bse, os.path.join(bse_dir, f'BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx'))
#     # write_notis_postgredb(df=df_bse, table_name=n_tbl_bse_trade_data, truncate_required=True)
#     # ett = datetime.now()
#     # print(f'BSE trade fetched. Total time taken: {(ett - stt).seconds} seconds')
#     # pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
#     # pbar.update(1)
#     # for i in range(100):
#     #     time.sleep(1)
#     #     pbar.update(i + 1)
#     # pbar.finish()
#     # download_tables()
o=0
from common import logger, bse_dir, write_notis_data, today
def read_data_db_1(nnf=False, for_table='ENetMIS', from_time:str='', to_time:str='', from_source=False):
    if not nnf and for_table == 'BSE_ENetMIS':
        # Sql connection parameters
        sql_server = "rms.ar.db"
        sql_database = "ENetMIS"
        sql_username = "notice_user"
        sql_password = "Notice@2024"
        if not from_time:
            sql_query = f"SELECT * FROM [ENetMIS].[dbo].[BSE_FO_AA100_view] where scid like 'SENSEX%' or scid like 'BANKEX%'"
        else:
            sql_query = f"SELECT * FROM [ENetMIS].[dbo].[BSE_FO_AA100_view] WHERE CreateDate BETWEEN '{from_time}' AND '{to_time}';"
        try:
            sql_connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={sql_server};"
                f"DATABASE={sql_database};"
                f"UID={sql_username};"
                f"PWD={sql_password}"
            )
            with pyodbc.connect(sql_connection_string) as sql_conn:
                df = pd.read_sql_query(sql_query, sql_conn)
            logger.info(f"Data fetched from SQL Server. Shape:{df.shape}")
            return df

        except (pyodbc.Error, psycopg2.Error) as e:
            logger.info("Error occurred:", e)
    elif nnf and for_table != 'ENetMIS':
        # engine = create_engine(engine_str)
        with engine.begin() as conn:
            df = pd.read_sql_table(n_tbl_notis_nnf_data, con=conn)
        logger.info(f"Data fetched from {for_table} table. Shape:{df.shape}")
        return df
    elif not nnf and for_table == 'TradeHist':
        sql_server = '172.30.100.41'
        sql_port = '1450'
        sql_db = 'OMNE_ARD_PRD'
        sql_userid = 'Pos_User'
        sql_paswd = 'Pass@Word'
        if not from_time:
            logger.info(f'Fetching today\'s BSE trade data till now.')
            # sql_query = (
            #     f"select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from [OMNE_ARD_PRD].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'")
            # sql_query2 = (
            #     f"select mnmFillPrice,mnmSegment, mnmTradingSymbol,mnmTransactionType,mnmAccountId,mnmUser , mnmFillSize, mnmSymbolName, mnmExpiryDate, mnmOptionType, mnmStrikePrice, mnmAvgPrice, mnmExecutingBroker from [OMNE_ARD_PRD_HNI].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and mnmAccountId = 'AA100'")
            sql_query = (
                f"select * from [OMNE_ARD_PRD].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')"
                f"union all "
                f"select * from [OMNE_ARD_PRD_HNI].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')"
            )
        else:
            logger.info(f'Fetching BSE trade data from {from_time} to {to_time}')
            sql_query = (
                f"select * from [OMNE_ARD_PRD].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')"
                f"union all"
                f"select * from [OMNE_ARD_PRD_HNI].[dbo].[TradeHist] where mnmExchSeg = 'bse_fo' and (mnmAccountId = 'AA100' or mnmAccountId = 'CPAA100')"
            )
        try:
            sql_engine_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={sql_server},{sql_port};"
                f"DATABASE={sql_db};"
                f"UID={sql_userid};"
                f"PWD={sql_paswd};"
            )
            with pyodbc.connect(sql_engine_str) as sql_conn:
                df_bse = pd.read_sql_query(sql_query, sql_conn)
                # df_bse_hni = pd.read_sql_query(sql_query2,sql_conn)
            logger.info(f'data fetched for bse: {df_bse.shape}')
            # final_bse_df = pd.concat([df_bse,df_bse_hni], ignore_index=True)
            return df_bse
        except (pyodbc.Error, psycopg2.Error) as e:
            logger.info(f'Error in fetching data: {e}')
    elif not nnf and for_table!='ENetMIS':
        # engine = create_engine(engine_str)
        with engine.begin() as conn:
            df = pd.read_sql_table(for_table, con=conn)
        logger.info(f"Data fetched from {for_table} table. Shape:{df.shape}")
        return df

df_bse = read_data_db_1(for_table='BSE_ENetMIS')
# df_bse = df_bse.query("mnmTransactionType != 'L'")
# df_bse.replace('',0, inplace=True)
# df_bse.columns = [re.sub(r'mnm|\s','',each) for each in df_bse.columns]
# df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x:pd.to_datetime(x, unit='s').date().strftime('%d/%m/%Y'))
# df_bse.ExpiryDate = df_bse.ExpiryDate.apply(lambda x: x if x.endswith('2025') else '')
write_notis_data(df=df_bse,filepath=os.path.join(bse_dir,f'BSE_TRADE_DATA_ALL_{today.strftime("%d%b%Y").upper()}.xlsx'))
write_notis_data(df=df_bse,filepath=os.path.join(rf"C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\BSE",f'BSE_TRADE_DATA_ALL_{today.strftime("%d%b%Y").upper()}.xlsx'))
p=0