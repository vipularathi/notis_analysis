import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar
from db_config import engine_str, n_tbl_notis_desk_wise_final_net_position
from sqlalchemy import create_engine
from main import get_date_from_non_jiffy, read_data_db, read_notis_file, write_notis_data

pd.set_option('display.max_columns', None)
holidays_25 = ['2025-02-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14', '2025-04-18', '2025-05-01', '2025-08-15', '2025-08-27', '2025-10-02', '2025-10-21', '2025-10-22', '2025-11-05', '2025-12-25']
# holidays_25.append('2024-03-20') #add unusual holidays
# today = datetime.now().date()
today = datetime(year=2025, month=1, day=21).date()
b_days = pd.bdate_range(start=today-timedelta(days=7), end=today, freq='C', weekmask='1111100', holidays=holidays_25).date.tolist()
# b_days = b_days.append(pd.DatetimeIndex([pd.Timestamp(year=2024, month=1, day=20)])) #add unusual trading days

yesterday = today-timedelta(days=1)
# yesterday = datetime(year=2025, month=1, day=13).date()
today, yesterday = sorted(b_days)[-1], sorted(b_days)[-2]

root_dir = os.path.dirname(os.path.abspath(__file__))
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
eod_test_dir = os.path.join(root_dir, 'eod_testing')
eod_input_dir = os.path.join(root_dir, 'eod_original')
eod_output_dir = os.path.join(root_dir, 'eod_data')
table_dir = os.path.join(root_dir, 'table_data')
bhav_path = os.path.join(root_dir, 'bhavcopy')
test_dir = os.path.join(root_dir, 'testing')

# mod_time = os.path.getmtime(nnf_file_path)
# # readable_mod_time = time.ctime(mod_time)
# readable_mod_time = datetime.fromtimestamp(mod_time)
#
# df = pd.read_excel(nnf_file_path, index_col=False)
# df_new = pd.read_excel(new_nnf_file_path, index_col=False)
# # df1 = read_notis_file(r"D:\notis_analysis\modified_data\NOTIS_DATA_12DEC2024.xlsx")
# df = df.loc[:, ~df.columns.str.startswith('Un')]
# df_new = df_new.loc[:, ~df_new.columns.str.startswith('Un')]
# df.columns = df.columns.str.replace(' ', '', regex = True)
# df_new.columns = df_new.columns.str.replace(' ', '', regex = True)
# df.dropna(how='all', inplace=True)
# df_new.dropna(how='all', inplace=True)
# # df[['NNFID', 'NeatID']] = df[['NNFID', 'NeatID']].astype(int)
# # df.NeatID = df.NNFID.astype(int)
# list_col = [col for col in df.columns if not col.startswith('NNF')]
# grouped_df = df.groupby(['NNFID'])[list_col].sum()
# # for index, row in grouped_df.iterrows():
# #     print('indx-',int(index),'\n', 'row-\n', row, '\n')
#
# merged_df = pd. merge(df1, df, left_on='ctclid', right_on='NNFID', how='left')
# print(merged_df)

# def read_notis_file1(filepath):
#     wb = load_workbook(filepath, read_only=True)
#     sheet = wb.active
#     total_rows = sheet.max_row
#     print('Reading Notis file...')
#     pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
#                                                            progressbar.Bar(marker='=', left='[', right=']'),
#                                                            progressbar.ETA()])
#
#     data = []
#     pbar.update(0)
#     for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
#         data.append(row)
#         pbar.update(i)
#     pbar.finish()
#     df = pd.DataFrame(data[1:], columns=data[0])
#     print('Notis file read')
#     return df

# def read_db(table_name):
#     engine = create_engine(engine_str)
#     df = pd.read_sql_table(table_name, con=engine)
#     return df

def write_notis_data1(df, filepath):
    print('Writing Notis file to excel...')
    wb = Workbook()
    ws = wb.active
    ws.title = 'Net position'
    rows = list(dataframe_to_rows(df, index=False, header=True))
    total_rows = len(rows)
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                           progressbar.Bar(marker='=', left='[', right=']'),
                                                           progressbar.ETA()])
    for i, row in enumerate(rows, start=1):
        ws.append(row)
        pbar.update(i)
    pbar.finish()
    # df.to_excel(os.path.join(modified_dir, file_name))
    print('Saving the file...')
    # wb.save(filepath)
    wb.save(filepath)
    print('New Notis excel file created')

# def get_date_from_non_jiffy(dt_val):
#     """
#     Converts the 1980 format date time to a readable format.
#     :param dt_val: long
#     :return: long (epoch time in seconds)
#     """
#     # Assuming dt_val is seconds since Jan 1, 1980
#     base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
#     # date_time = int(base_date.timestamp() + dt_val)
#     date_time = base_date.timestamp() + dt_val
#     new_date = datetime.fromtimestamp(date_time, timezone.utc)
#     formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
#     return formatted_date


# eod_df = read_notis_file(os.path.join(root_dir, 'EOD Position 08-Jan-25.xlsx'))
# eod_df = read_notis_file(os.path.join(eod_input_dir, 'EOD Position_10_Jan_2025_1.xlsx'))
eod_df = read_notis_file(os.path.join(eod_input_dir, f'EOD Position_{yesterday.strftime("%d_%b_%Y")}_1.xlsx'))
# eod_df = read_notis_file(os.path.join(eod_dir, rf'NOTIS_DESK_WISE_FINAL_NET_POSITION_{yesterday.strftime("%Y-%m-%d")}_testing_1.xlsx'))
eod_df.columns = eod_df.columns.str.replace(' ', '')
eod_df = eod_df.add_prefix('Eod')
# eod_df = read_notis_file(rf"C:\Users\vipulanand\Downloads\Book1.xlsx")
eod_df.EodExpiry = eod_df.EodExpiry.dt.date
eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
eod_df = eod_df.drop_duplicates()

tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
desk_db_df = read_data_db(for_table=tablenam)
desk_db_df1 = read_notis_file(os.path.join(table_dir, f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y_%m_%d")}.xlsx'))
desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
desk_db_df = desk_db_df.drop_duplicates()


bhav_df = read_notis_file(os.path.join(bhav_path, rf'regularBhavcopy_{today.strftime("%d%m%Y")}.xlsx')) # regularBhavcopy_14012025.xlsx
bhav_df.columns = bhav_df.columns.str.replace(' ', '')
bhav_df.columns = bhav_df.columns.str.capitalize()
bhav_df = bhav_df.add_prefix('Bhav')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
# desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
# eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
# desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
# eod_df['EodClosingQty'] = eod_df['EodClosingQty'].astype('int64')
# eod_df['EodMTM'] = eod_df['EodClosingQty'] * eod_df['EodClosingPrice']
col_keep = ['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype','BhavClosingprice']
bhav_df = bhav_df[col_keep]
# col_drop = ['BhavTotalValue','BhavOpenInterest','BhavChangeInOpenInterest']
# bhav_df = bhav_df.drop(columns=[col for col in bhav_df.columns if col in col_drop])
bhav_df = bhav_df.drop_duplicates()

# merged_df = desk_db_df.merge(eod_df, left_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], right_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
merged_df = eod_df.merge(desk_db_df, right_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], left_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='outer')
merged_df.fillna(0, inplace=True)
merged_df = merged_df.drop_duplicates()

coltd1 = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry', 'EodMainGroup']
coltd2 = ['symbol', 'strikePrice', 'optionType', 'expiryDate', 'mainGroup']
for i in range(len(coltd1)):
    merged_df.loc[merged_df[coltd1[i]] == 0, coltd1[i]] = merged_df[coltd2[i]]
    merged_df.loc[merged_df[coltd2[i]] == 0, coltd2[i]] = merged_df[coltd1[i]]
merged_df['NetQty'] = merged_df['EodClosingQty'] + merged_df['volume']


merged_bhav_df = merged_df.merge(bhav_df, left_on=["symbol", "expiryDate", "strikePrice", "optionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
# for index, row in merged_bhav_df.iterrows():
#     if abs(row['volume']) > 0:
#         if (row['MTM'] > 0 and abs(row['MTM'])>abs(row['EodMTM'])) or (row['EodMTM'] > 0 and abs(row['EodMTM'])>abs(row['MTM'])):
#             sign = 1
#         else:
#             sign = -1
#         # merged_bhav_df.loc[index, 'NetAvgPrice'] = (abs(row['MTM']) + abs(
#         #     (row['BhavClosingprice'] * row['EodClosingQty']))) / (abs(row['volume']) + abs(row['EodClosingQty']))
#         merged_bhav_df.loc[index, 'NetAvgPrice'] = abs(row['NetQty']) / abs(row['BhavClosingprice'])
merged_bhav_df = merged_bhav_df.drop_duplicates()

def find_expired_mtm(row):
    if row['expired'] == True:
            # if (row['MTM'] > 0 and abs(row['MTM'])>abs(row['EodMTM'])) or (row['EodMTM'] > 0 and abs(row['EodMTM'])>abs(row['MTM'])):
            #     sign = 1
            #     return row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice'])
            # else:
            #     sign = -1
            #     return -1*(row['MTM']+row['EodMTM']+(row['NetQty']*row['BhavClosingprice']))
            return (-1*row['MTM'])+row['EodMTM']+(row['NetQty']*row['BhavClosingprice'])

merged_bhav_df.loc[merged_bhav_df['expiryDate'] == today, 'expired'] = True
merged_bhav_df['NetAvgPrice'] = merged_bhav_df.apply(lambda row: abs(row['NetQty'])/abs(row['BhavClosingprice']) if abs(row['volume'])>0 else None, axis=1)
merged_bhav_df['expiredMTM'] = merged_bhav_df.apply(find_expired_mtm, axis=1)
# col_to_keep = desk_db_df.columns.tolist()+['EodLong', 'EodShort','EodClosingQty','EodClosingPrice','EodSubGroup','EodMainGroup', 'EodMTM', 'expired', 'NetQty','BhavClosingprice', 'NetAvgPrice', 'expiredMTM']
# merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_to_keep], axis=1, inplace=True)
# col_drop = ['EodMTM','mainGroup', 'account', 'brokerID', 'tokenNumber','MTM', 'symbol', 'expiryDate', 'strikePrice', 'optionType','BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype', 'NetAvgPrice']
# merged_bhav_df = merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col in col_drop])
merged_bhav_df['Long'] = merged_bhav_df['EodLong'] + merged_bhav_df['buyAvgQty']
merged_bhav_df['Short'] = merged_bhav_df['EodShort'] + merged_bhav_df['sellAvgQty']
col_keep = ['EodUnderlying', 'EodStrike', 'EodOptionType', 'EodExpiry','Long','Short','NetQty','BhavClosingprice','EodSubGroup', 'EodMainGroup']
merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)
merged_bhav_df.columns = merged_bhav_df.columns.str.replace('Eod','')
merged_bhav_df.rename(columns={'NetQty':'ClosingQty','BhavClosingprice':'ClosingPrice'}, inplace=True )
merged_bhav_df = merged_bhav_df[['Underlying', 'Strike', 'OptionType', 'Expiry', 'Long', 'Short', 'ClosingQty', 'ClosingPrice', 'SubGroup', 'MainGroup']]
merged_bhav_df = merged_bhav_df.drop_duplicates()
# # merged_df.drop(columns=eod_df.columns.tolist(), axis=1, inplace=True)
# merged_bhav_df['Long'] = merged_bhav_df['buyAvgQty'] + merged_bhav_df['EodLong']
# merged_bhav_df['Short'] = merged_bhav_df['sellAvgQty'] + merged_bhav_df['EodShort']
# col_keep = ['symbol', 'strikePrice', 'optionType', 'expiryDate', 'Long', 'Short', 'NetQty', 'BhavClosingprice', 'EodSubGroup', 'mainGroup']
# merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_keep], axis=1, inplace=True)
# merged_bhav_df.rename(columns={'symbol':'Underlying', 'strikePrice':'Strike', 'optionType':'OptionType', 'NetQty':'ClosingQty', 'BhavClosingprice':'ClosingPrice', 'EodSubGroup':'SubGroup', 'mainGroup':'mainGroup'})
# merged_bhav_df = merged_bhav_df[col_keep]
a=0
def update_qty(row):
    if row.Long > row.Short:
        row.Long = row.ClosingQty
        row.Short = 0
    elif row.Long < row.Short:
        row.Short = row.ClosingQty
        row.Long = 0
    return row
merged_bhav_df = merged_bhav_df.apply(update_qty, axis=1)
# write_notis_postgredb(desk_db_df1, table_name=n_tbl_notis_desk_wise_final_net_position, raw=False)
write_notis_data(merged_bhav_df, os.path.join(eod_output_dir, f'Eod_{today.strftime("%Y_%m_%d")}.xlsx'))
print(f'file made for {today}')
# write_notis_data(desk_db_df, f'desk_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(eod_df, f'eod_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(bhav_df, f'bhav_{today.strftime("%Y-%m-%d")}.xlsx')
# print(eod_df.head(),'\n',desk_db_df.head())
b=0