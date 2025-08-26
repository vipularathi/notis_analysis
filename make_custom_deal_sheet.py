from common import read_data_db, today, write_notis_postgredb
from nse_utility import NSEUtility
from bse_utility import BSEUtility


def get_nse_data():
    # # logger.info(f'fetching NSE trades...')
    # df_db = read_data_db()
    # if df_db is None or df_db.empty:
    #     # logger.info(f'No NSE trade done today hence skipping')
    #     df = pd.DataFrame()
    #     return df
    # # logger.info(f'Notis trade data fetched, shape={df_db.shape}')
    # # write_notis_postgredb(df=df_db, table_name=n_tbl_notis_raw_data, raw=True, truncate_required=True)
    # # modify_filepath = os.path.join(modified_dir, f'NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    # nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
    # if not os.path.exists(nnf_file_path):
    #     raise FileNotFoundError("NNF File not found. Please add the NNF file and try again.")
    # readable_mod_time = datetime.fromtimestamp(os.path.getmtime(nnf_file_path))
    # if readable_mod_time.date() == today:  # Check if the NNF file is modified today or not
    #     # logger.info(f'New NNF Data found, modifying the nnf data in db . . .')
    #     df_nnf = pd.read_excel(nnf_file_path, index_col=False)
    #     df_nnf = df_nnf.loc[:, ~df_nnf.columns.str.startswith('Un')]
    #     df_nnf.columns = df_nnf.columns.str.replace(' ', '', regex=True)
    #     df_nnf.dropna(how='all', inplace=True)
    #     df_nnf = df_nnf.drop_duplicates()
    #     # write_notis_postgredb(df=df_nnf, table_name=n_tbl_notis_nnf_data, truncate_required=True)
    # else:
    #     df_nnf = read_data_db(nnf=True, for_table=n_tbl_notis_nnf_data)
    #     df_nnf = df_nnf.drop_duplicates()
    modified_df = read_data_db(for_table='NOTIS_TRADE_BOOK_2025-08-22')
    # write_notis_postgredb(modified_df, table_name=n_tbl_notis_trade_book, truncate_required=True)
    # write_notis_data(modified_df, modify_filepath)
    # write_notis_data(modified_df, rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\NOTIS_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.csv')
    # logger.info('file saved in modified_data folder')
    modified_df['trdQtyPrc'] = modified_df['trdQty'] * (modified_df['trdPrc'] / 100)
    pivot_df = modified_df.pivot_table(
        index=['MainGroup', 'SubGroup', 'broker', 'ctclid', 'sym', 'expDt', 'strPrc', 'optType'],
        columns=['bsFlg'],
        values=['trdQty', 'trdQtyPrc', 'trdPrc'],
        aggfunc={'trdQty': 'sum', 'trdQtyPrc': 'sum', 'trdPrc': ['min', 'max']},
        fill_value=0
    )
    if len(modified_df.bsFlg.unique()) == 1:
        if modified_df.bsFlg.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = pivot_df['SellQty'] = pivot_df['SellMax'] = pivot_df['SellMin'] = 0
            pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
        elif modified_df.bsFlg.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = pivot_df['BuyQty'] = pivot_df['BuyMax'] = pivot_df['BuyMin'] = 0
            pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
    elif len(modified_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['BuyMax', 'SellMax', 'BuyMin', 'SellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                            'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['BuyAvgPrc'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0.0,
        axis=1)
    pivot_df['SellAvgPrc'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0.0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df.rename(columns={'MainGroup': 'mainGroup', 'SubGroup': 'subGroup', 'sym': 'symbol', 'expDt': 'expiryDate',
                             'strPrc': 'strikePrice', 'optType': 'optionType', 'BuyAvgPrc': 'buyAvgPrice',
                             'BuyTrdQtyPrc': 'buyValue', 'BuyQty': 'buyAvgQty', 'SellQty': 'sellAvgQty',
                             'SellAvgPrc': 'sellAvgPrice', 'SellTrdQtyPrc': 'sellValue'},
                    inplace=True)
    pivot_df.volume = pivot_df.buyAvgQty - pivot_df.sellAvgQty
    return pivot_df
def get_bse_data():
    # # stt = datetime.now()
    # # # logger.info(f'fetching BSE trades...')
    # # df_bse1 = read_data_db(for_table='BSE_ENetMIS')
    # # if df_bse1 is None or df_bse1.empty:
    # #     # logger.info(f'No BSE trade done today hence skipping')
    # #     df = pd.DataFrame()
    # #     return df
    # # # logger.info(f'BSE trade data fetched, shape={df_bse1.shape}')
    # modified_bse_df1 = read_data_db(for_table='BSE_TRADE_DATA_2025-08-08')
    # modified_bse_df1.TraderID = modified_bse_df1.TraderID.astype(np.int64)
    #
    # df_bse2 = read_data_db(for_table='TradeHist')
    # modified_bse_df2 = BSEUtility.bse_modify_file(df_bse2)
    # modified_bse_df2 = modified_bse_df2[
    #     ['TerminalID', 'TradingSymbol', 'FillSize', 'TransactionType', 'ExchUser', 'Underlying', 'Strike', 'OptionType',
    #      'Expiry']]
    # modified_bse_df2.ExchUser = modified_bse_df2.ExchUser.astype(np.int64)
    # modified_bse_df2.ExchUser = modified_bse_df2.ExchUser % 10000
    # grouped_modified_bse_df2 = (
    #     modified_bse_df2
    #     .groupby(['ExchUser', 'TradingSymbol', 'FillSize', 'TransactionType',
    #               'Underlying', 'Strike', 'OptionType', 'Expiry'], as_index=False)
    #     .agg({'TerminalID': 'first'})
    # )
    #
    # modified_bse_df = pd.merge(modified_bse_df1, grouped_modified_bse_df2,
    #                            left_on=['TraderID', 'TradingSymbol', 'FillSize', 'TransactionType', 'Underlying',
    #                                     'Strike', 'OptionType', 'Expiry'],
    #                            right_on=['ExchUser', 'TradingSymbol', 'FillSize', 'TransactionType', 'Underlying',
    #                                      'Strike', 'OptionType', 'Expiry'],
    #                            how='left'
    #                            )
    # modified_bse_df['TerminalID'] = np.where(modified_bse_df['TraderID'] == 1011, '945440A',
    #                                          modified_bse_df['TerminalID'])
    # modified_bse_df.drop(columns=['ExchUser'], axis=1, inplace=True)
    # modified_bse_df.fillna(0, inplace=True)
    # # write_notis_postgredb(df=modified_bse_df, table_name=n_tbl_bse_trade_data, truncate_required=True)
    # # write_notis_data(modified_bse_df, os.path.join(bse_dir, f'BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx'))
    # # write_notis_data(modified_bse_df,rf'C:\Users\vipulanand\Documents\Anand Rathi Financial Services Ltd (Synced)\OneDrive - Anand Rathi Financial Services Ltd\notis_files\BSE_TRADE_DATA_{today.strftime("%d%b%Y").upper()}.xlsx')
    modified_bse_df = read_data_db(for_table='BSE_TRADE_DATA_2025-08-22')
    modified_bse_df['trdQtyPrc'] = modified_bse_df['FillSize'] * (modified_bse_df['AvgPrice'] / 100)
    pivot_df = modified_bse_df.pivot_table(
        index=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType', 'TerminalID', 'TraderID'],
        columns=['TransactionType'],
        values=['FillSize', 'trdQtyPrc', 'AvgPrice'],
        aggfunc={'FillSize': 'sum', 'trdQtyPrc': 'sum', 'AvgPrice': ['min', 'max']},
        fill_value=0
    )
    if len(modified_bse_df.TransactionType.unique()) == 1:
        if modified_bse_df.TransactionType.unique().tolist()[0] == 'B':
            pivot_df['SellTrdQtyPrc'] = pivot_df['SellQty'] = pivot_df['sellMax'] = pivot_df['sellMin'] = 0
            pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
        elif modified_bse_df.TransactionType.unique().tolist()[0] == 'S':
            pivot_df['BuyTrdQtyPrc'] = pivot_df['BuyQty'] = pivot_df['buyMax'] = pivot_df['buyMin'] = 0
            pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                                'SellTrdQtyPrc']
    elif len(modified_bse_df) == 0 or len(pivot_df) == 0:
        pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
    else:
        pivot_df.columns = ['buyMax', 'sellMax', 'buyMin', 'sellMin', 'BuyQty', 'SellQty', 'BuyTrdQtyPrc',
                            'SellTrdQtyPrc']
    pivot_df.reset_index(inplace=True)
    pivot_df['buyAvgPrice'] = pivot_df.apply(
        lambda row: row['BuyTrdQtyPrc'] / row['BuyQty'] if row['BuyQty'] > 0 else 0, axis=1)
    pivot_df['sellAvgPrice'] = pivot_df.apply(
        lambda row: row['SellTrdQtyPrc'] / row['SellQty'] if row['SellQty'] > 0 else 0, axis=1)
    # pivot_df.drop(columns=['BuyTrdQtyPrc', 'SellTrdQtyPrc'], inplace=True)
    pivot_df['IntradayVolume'] = pivot_df['BuyQty'] - pivot_df['SellQty']
    pivot_df.rename(columns={'BuyTrdQtyPrc': 'buyValue', 'SellTrdQtyPrc': 'sellValue'}, inplace=True)
    # pivot_df = pivot_df.round(2)
    ett = datetime.now()
    # logger.info(f'BSE trade fetched. Total time taken: {(ett - stt).seconds} seconds')
    return pivot_df

nse_pivot_df = get_nse_data()
bse_pivot_df = get_bse_data()

nse_deal_df = NSEUtility.calc_nse_deal_sheet(nse_pivot_df)
bse_deal_df = BSEUtility.calc_bse_deal_sheet(bse_pivot_df)
final_deal_df = pd.concat([nse_deal_df, bse_deal_df], ignore_index=True)
grouped_main_deal_df = final_deal_df.groupby(
    by=['Broker', 'Underlying', 'Expiry', 'Strike', 'OptionType'],
    as_index=False
).agg(
    {'BuyMax': 'max', 'SellMax': 'max',
     'BuyMin': 'min', 'SellMin': 'min',
     'BuyQty': 'sum', 'SellQty': 'sum',
     'BuyValue':'sum', 'SellValue':'sum'}
)
write_notis_postgredb(df=grouped_main_deal_df, table_name='NOTIS_DEAL_SHEET_2025-08-22', truncate_required=True)