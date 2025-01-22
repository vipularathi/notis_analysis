import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime, timedelta, timezone
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import progressbar

pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')
eod_dir = os.path.join(root_dir, 'eod_data')
mod_dir = os.path.join(root_dir, 'modified_data')
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

from db_config import engine_str, n_tbl_notis_desk_wise_final_net_position
from sqlalchemy import create_engine
from main import get_date_from_non_jiffy
import calendar

def read_notis_file(filepath):
    wb = load_workbook(filepath, read_only=True)
    sheet = wb.active
    total_rows = sheet.max_row
    print('Reading Notis file...')
    pbar = progressbar.ProgressBar(max_value=total_rows, widgets=[progressbar.Percentage(), ' ',
                                                           progressbar.Bar(marker='=', left='[', right=']'),
                                                           progressbar.ETA()])

    data = []
    pbar.update(0)
    for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
        data.append(row)
        pbar.update(i)
    pbar.finish()
    df = pd.DataFrame(data[1:], columns=data[0])
    print('Notis file read')
    return df

def read_db(table_name):
    engine = create_engine(engine_str)
    df = pd.read_sql_table(table_name, con=engine)
    return df

def write_notis_data(df, filepath):
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

def get_date_from_non_jiffy(dt_val):
    """
    Converts the 1980 format date time to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Assuming dt_val is seconds since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    # date_time = int(base_date.timestamp() + dt_val)
    date_time = base_date.timestamp() + dt_val
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date

def calc_date(for_date, holidays):
    now = datetime.now()
    year = for_date.year
    month = for_date .month
    cal = calendar.Calendar()
    explist = []
    for day in cal.itermonthdays(year, month):
        if day != 0:  # to skip days outside the current month
            date = datetime(year, month, day)
            if date.weekday() == 3:  # check if weekday is thursday or not
                # explist.append(date.strftime('%Y-%m-%d'))
                explist.append(date.date())

    # for last friday
    # last_day = calendar.monthrange(year, month)[1]
    # last_friday = datetime(year, month, last_day)
    # while last_friday.weekday() != 4:  # Check if it's Friday (weekday 4)
    #     last_friday -= timedelta(days=1)
    # explist.append(last_friday.strftime('%Y-%m-%d'))
    for i in range(len(explist)): # modifying expiry as per holidays
        if explist[i] in holidays:
            explist[i] = explist[i] - timedelta(days=1)
    explist = sorted([exp for exp in explist if exp <= for_date])
    return explist[-1]

def check_holiday(explist, holidays): #to check and replace the date if it falls on a holiday
    # exp_list = []
    # for i in range(len(explist)):
    #     while explist[i] in holidays:
    #         date_obj = datetime.strptime(explist[i], '%Y-%m-%d')
    #         new_date_obj = date_obj - timedelta(days=1)
    #         explist[i] = new_date_obj.strftime('%Y-%m-%d')
    for i in range(len(explist)):
        if explist[i] in holidays:
            explist[i] = explist[i] - timedelta(days=1)
    return explist

holidays_25 = ['2025-02-26', '2025-03-14', '2025-03-31', '2025-04-10', '2025-04-14', '2025-04-18', '2025-05-01', '2025-08-15', '2025-08-27', '2025-10-02', '2025-10-21', '2025-10-22', '2025-11-05', '2025-12-25']
today = datetime(year=2025, month=1, day=13).date()
b_days = pd.bdate_range(start=today-timedelta(days=7), end=today, freq='C', weekmask='1111100', holidays=holidays_25).date.tolist()
yesterday = today-timedelta(days=1)
# yesterday = datetime(year=2025, month=1, day=13).date()
today, yesterday = sorted(b_days)[-1], sorted(b_days)[-2]

latest_expiry = calc_date(today, pd.to_datetime(holidays_25).date.tolist())
# final_date_list = check_date(date_list, pd.to_datetime(holidays_25).date.tolist())
expiry_flag = False
if latest_expiry == today:
    expiry_flag = True

# now create column to store expiry flag and then calc expiry PnL whereever the expiry flag is true

# tablenam = f'NOTIS_DESK_WISE_NET_POSITION'
tablenam = f'NOTIS_DESK_WISE_NET_POSITION_{today.strftime("%Y-%m-%d")}'
final_net_position_path = os.path.join(eod_dir, f'NOTIS_DESK_WISE_FINAL_NET_POSITION_{today.strftime("%Y-%m-%d")}_untitled.xlsx')

# # eod_df = read_notis_file(os.path.join(root_dir, 'EOD Position 08-Jan-25.xlsx'))
# eod_df = read_notis_file(os.path.join(root_dir, 'EOD Position 08-Jan-2025.xlsx'))
# eod_df.columns = eod_df.columns.str.replace(' ', '')
# eod_df = eod_df.add_prefix('Eod')
# # eod_df = read_notis_file(rf"C:\Users\vipulanand\Downloads\Book1.xlsx")
# eod_df.EodExpiry = eod_df.EodExpiry.dt.date
# eod_df['EodClosingqty'] = eod_df['EodClosingqty'].astype('int64')
# eod_df['EodMTM'] = eod_df['EodClosingqty'] * eod_df['EodClosingPrice']

desk_db_df = read_notis_file(os.path.join(mod_dir, f'NOTIS_DATA_{today.strftime("%d%b%Y")}_1.xlsx'))
# desk_db_df = read_db(table_name=tablenam)
col_keep = ['MainGroup', 'SubGroup','UserID','cpCD','sym','expDt','strPrc','optType','bsFlg','trdQty','trdPrc']
desk_db_df = desk_db_df[col_keep]
desk_db_df.expDt = desk_db_df.expDt.astype('datetime64[ns]')
desk_db_df.expDt = desk_db_df.expDt.dt.date
q=0
grouped_desk_df = desk_db_df.groupby(by=['MainGroup', 'SubGroup','UserID','cpCD','sym','expDt','strPrc','optType','bsFlg']).agg({'trdQty':'sum','trdPrc':'mean'})
desk_db_df.expiryDate = desk_db_df.expiryDate.astype('datetime64[ns]')
desk_db_df.expiryDate = desk_db_df.expiryDate.dt.date
desk_db_df.loc[desk_db_df['optionType'] == 'XX', 'strikePrice'] = 0
desk_db_df.strikePrice = desk_db_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
desk_db_df.strikePrice = desk_db_df.strikePrice.astype('int64')
# desk_db_df.drop(columns=['MTM'], axis=1, inplace=True)

bhav_df = read_notis_file(os.path.join(root_dir, 'regularBhavcopy_09012025.xlsx'))
bhav_df.columns = bhav_df.columns.str.replace(' ', '')
bhav_df.columns = bhav_df.columns.str.capitalize()
bhav_df = bhav_df.add_prefix('Bhav')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
# eod_grouped_df = eod_df.groupby(['Main Group', 'Underlying', 'Expiry', 'Strike', 'Option Type'])['Closing qty'].sum().reset_index()
# desk_grouped_df = desk_db_df.groupby(["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"])["volume"].sum().reset_index()
# desk_grouped_df.expiryDate = desk_grouped_df.expiryDate.astype('datetime64[ns]')
# eod_grouped_df.Expiry = eod_grouped_df.Expiry.dt.date
# desk_grouped_df.expiryDate = desk_grouped_df.expiryDate.dt.date
# desk_grouped_df.strikePrice = desk_grouped_df.strikePrice.apply(lambda x: x/100 if x>0 else x)
# desk_grouped_df.strikePrice = desk_grouped_df.strikePrice.astype('int64')

merged_df = desk_db_df.merge(eod_df, left_on=["mainGroup", "symbol", "expiryDate", "strikePrice", "optionType"], right_on=['EodMainGroup', 'EodUnderlying', 'EodExpiry', 'EodStrike', 'EodOptionType'], how='left')
merged_df.fillna(0, inplace=True)
merged_df['NetQty'] = merged_df['EodClosingqty'] + merged_df['volume']
# merged_df['Net Avg Price'] = ((merged_df['buyAvgPrice']))

merged_bhav_df = merged_df.merge(bhav_df, left_on=["symbol", "expiryDate", "strikePrice", "optionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
col_to_keep = desk_db_df.columns.tolist()+['EodLong', 'EodShort','EodClosingqty','EodClosingPrice','EodSubGroup','EodMainGroup','EodMTM','NetQty','BhavClosingprice']
merged_bhav_df.drop(columns=[col for col in merged_bhav_df.columns if col not in col_to_keep], axis=1, inplace=True)
for index, row in merged_bhav_df.iterrows():
    # merged_bhav_df.loc[index, 'MTM'] = (row['buyAvgPrice']*row['buyAvgQty'])-(row['sellAvgPrice']*row['sellAvgQty'])
    if abs(row['volume']) > 0:
        # merged_bhav_df.loc[index, 'MTM'] = row['BhavClosingprice'] * row['volume']
        # merged_bhav_df.loc[index, 'NetAvgPrice'] = (abs(row['EodMTM']) + abs(row['volume'] * row['BhavClosingprice'])) / (abs(row['volume']) + abs(row['EodClosingqty']))
        merged_bhav_df.loc[index, 'NetAvgPrice'] = abs(row['NetQty']) / abs(row['BhavClosingprice'])
# # merged_df['Net PnL'] = merged_df['MTM'] + merged_df['MTM eod']
# merged_df.drop(columns=eod_df.columns.tolist(), axis=1, inplace=True)
# merged_df.drop(columns=['volume', 'MTMeod'], axis=1, inplace=True)

# bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(get_date_from_non_jiffy(x))).dt.strftime('%Y-%m-%d')
# bhav_df.rename(columns={'BhavExpiry':'BhavExpiryDate'}, inplace=True)
# # bhav_df.expiryDate = bhav_df.expiryDate.astype('datetime64[ns]')
# bhav_df.BhavExpiry = bhav_df.BhavExpiry.apply(lambda x: pd.to_datetime(x).date())
# bhav_df.loc[bhav_df['BhavOptiontype'] == 'XX', 'BhavStrikeprice'] = 0
# bhav_df = bhav_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.apply(lambda x: x/100 if x>0 else x)
# bhav_df.BhavStrikeprice = bhav_df.BhavStrikeprice.astype('int64')
# for each in ['symbol','optionType']:
#     bhav_df[each] = bhav_df[each].str.strip()
#     merged_df[each] = merged_df[each].str.strip()
# merged_bhav_df = merged_df.merge(bhav_df, left_on=["symbol", "expiryDate", "strikePrice", "optionType"], right_on=['BhavSymbol', 'BhavExpiry', 'BhavStrikeprice', 'BhavOptiontype'], how='left')
# merged_bhav_df = merged_df.merge(bhav_df, on=["symbol", "expiryDate", "strikePrice", "optionType"], how='left')
# # desk_db_df1 = desk_db_df.copy()
# # desk_db_df1['volume'] = merged_df['Closing qty'] + merged_df['volume']
# # desk_db_df1.rename(columns={'volume':'Net Qty'}, inplace=True)
# merged_bhav_df1 = merged_df.copy()
# merged_bhav_df1['bhav_close']=0
# for index, row in merged_df.iterrows():
#     sym = row.symbol
#     exp = row.expiryDate
#     stPrice = row.strikePrice
#     optType = row.optionType
#     bhav_close = row.closingPrice
#     for bhav_index, bhav_row in bhav_df.iterrows():
#         if sym == bhav_row.symbol and exp == bhav_row.expiryDate and stPrice == bhav_row.strikePrice and optType == bhav_row.optionType:
#             merged_bhav_df1.at[index, 'bhav_close'] = bhav_close
merged_bhav_df.loc[merged_bhav_df['expiryDate'] == today, 'expired'] = True
merged_bhav_df = merged_bhav_df.drop_duplicates()
a=0
# write_notis_postgredb(desk_db_df1, table_name=n_tbl_notis_desk_wise_final_net_position, raw=False)
write_notis_data(merged_bhav_df, final_net_position_path)
# write_notis_data(desk_db_df, f'desk_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(eod_df, f'eod_{today.strftime("%Y-%m-%d")}.xlsx')
# write_notis_data(bhav_df, f'bhav_{today.strftime("%Y-%m-%d")}.xlsx')
# print(eod_df.head(),'\n',desk_db_df.head())
b=0