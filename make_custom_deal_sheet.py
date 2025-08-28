from common import read_data_db, today, write_notis_postgredb
from nse_utility import NSEUtility
from bse_utility import BSEUtility


def get_nse_data():
    modified_df = read_data_db(for_table='NOTIS_TRADE_BOOK_2025-08-22')
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