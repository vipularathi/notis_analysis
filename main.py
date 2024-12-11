import re

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import os
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import progressbar
from openpyxl import load_workbook, Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import os

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

def write_notis_data(df, file_name):
    print('Writing Notis file...')
    wb = Workbook()
    ws = wb.active
    ws.title = file_name
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
    wb.save(os.path.join(modified_dir, file_name))
    print('New Notis excel file created')

def get_date_from_jiffy(dt_val):
    """
    Converts the Jiffy format date to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Jiffy is 1/65536 of a second since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    date_time = int((base_date.timestamp() + (dt_val / 65536)))
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date

def get_date_from_non_jiffy(dt_val):
    """
    Converts the 1980 format date time to a readable format.
    :param dt_val: long
    :return: long (epoch time in seconds)
    """
    # Assuming dt_val is seconds since Jan 1, 1980
    base_date = datetime(1980, 1, 1, tzinfo=timezone.utc)
    date_time =  int(base_date.timestamp() + dt_val)
    new_date = datetime.fromtimestamp(date_time, timezone.utc)
    formatted_date = new_date.astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %I:%M:%S")
    return formatted_date
def modify_file(df):
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar = progressbar.ProgressBar(max_value=100, widgets=[progressbar.Percentage(), ' ', progressbar.Bar(marker='=', left='[', right=']'), progressbar.ETA()])
    print('Starting file modification...')
    # pbar.update(0, 'Starting file modification...')
    pbar.update(0)
    df.rename(columns={
        'Column1': 'seqNo', 'Column2': 'mkt', 'Column3': 'trdNo',
        'Column4': 'trdTm', 'Column5': 'Tkn', 'Column6': 'trdQty',
        'Column7': 'trdPrc', 'Column8': 'bsFlg', 'Column9': 'ordNo',
        'Column10': 'brnCd', 'Column11': 'usrId', 'Column12': 'proCli',
        'Column13': 'cliActNo', 'Column14': 'cpCd', 'Column15': 'remarks',
        'Column16': 'actTyp', 'Column17': 'TCd', 'Column18': 'ordTm',
        'Column19': 'Booktype', 'Column20': 'oppTmCd', 'Column21': 'ctclid',
        'Column22': 'status', 'Column23': 'TmCd', 'Column24': 'sym',
        'Column25': 'ser', 'Column26': 'inst', 'Column27': 'expDt',
        'Column28': 'strPrc', 'Column29': 'optType', 'Column30': 'sessionID',
        'Column31': 'echoback', 'Column32': 'Fill1', 'Column33': 'Fill2',
        'Column34': 'Fill3', 'Column35': 'Fill4', 'Column36': 'Fill5', 'Column37': 'Fill6'
    }, inplace=True)
    # pbar.update(20, 'Column renamed')
    pbar.update(20)
    # --------------------------------------------------------------------------------------------------------------------------------
    # df['ordTm'] = df['ordTm'].apply(lambda x: datetime.fromtimestamp(x)+relativedelta(years=10)-relativedelta(days=1))
    df['ordTm'] = df['ordTm'].apply(lambda x: get_date_from_non_jiffy(x))
    # pbar.update(40, 'Order time modified')
    pbar.update(40)
    # --------------------------------------------------------------------------------------------------------------------------------
    # df['expDt'] = df['expDt'].apply(lambda x: datetime.fromtimestamp(x)+relativedelta(years=10)-relativedelta(days=1))
    df['expDt'] = df['expDt'].apply(lambda x: get_date_from_non_jiffy(x))
    # pbar.update(60, 'Expiry date modified')
    pbar.update(60)
    # --------------------------------------------------------------------------------------------------------------------------------
    # df['trdTm'] = df['trdTm'].apply(lambda x: datetime.fromtimestamp(x/100000)+timedelta(days = 25*365.25+6*29.5))
    df['trdTm'] = df['trdTm'].apply(lambda x: get_date_from_jiffy(x))
    # pbar.update(80, 'Trade time modified')
    pbar.update(80)
    # --------------------------------------------------------------------------------------------------------------------------------
    df['bsFlg'] = np.where(df['bsFlg'] == 1, 'B', 'S')
    # pbar.update(90, 'Buy Sell Flag modified')
    pbar.update(90)
    # --------------------------------------------------------------------------------------------------------------------------------
    # df['User Name'] = df['ctclid'].apply(lambda x: )
    conditions = [
        (df['ctclid'] == 400013041065130) | (df.ctclid == 400013041076130) | (df.ctclid == 400013041123012) | (df.ctclid == 400013041168130) | (df.ctclid == 400013041196030) | (df.ctclid == 400013041196130),
        (df.ctclid == 400013041217130),
        (df.ctclid == 400013041087000) | (df.ctclid == 400013041202130) | (df.ctclid == 400013055025000) | (df.ctclid == 400013041172030),
        (df.ctclid == 400013041161030) | (df.ctclid == 400013041161130) | (df.ctclid == 400013041208030) | (df.ctclid == 400013041208130) | (df.ctclid == 400013041148030),
        (df.ctclid == 400013055027030)
    ]
    user_choices = ['Shubham Gagrani','Ria Shah','Rajeev Thakthani', 'Mohit Vajpayee', 'Harshit Arora']
    desk_choices = ['Desk1', 'Desk3', 'Desk3', 'Desk2', 'Desk3']
    default = ('')
    # df[['UserName', 'Desk']] = pd.DataFrame(np.select(conditions, choices, default=default), index = df.index)
    df['UserName'] = np.select(conditions, user_choices, default=default)
    df['Desk'] = np.select(conditions, desk_choices, default=default)
    pbar.update(100)
    # --------------------------------------------------------------------------------------------------------------------------------
    pbar.finish()
    return df

def main():
    # today = datetime.now().date().strftime("%d%b%Y").upper()
    today = datetime(year=2024, month=12, day=10).date().strftime("%d%b%Y").upper()
    # filepath = rf'D:\notis_analysis\NOTIS_DATA_{today}.xlsx'
    pattern = rf'NOTIS_(DATA|API)_{today}.xlsx'
    matched_file = [f for f in os.listdir(data_dir) if re.match(pattern, f)]
    filepath = os.path.join(data_dir, matched_file[0])
    # df = pd.read_excel(filepath, index_col=False)
    df = read_notis_file(filepath)
    modified_df = modify_file(df)
    # # modified_df.to_excel(rf'modified_NOTIS_DATA_{today}.xlsx', index=False)
    # with pd.ExcelWriter(rf'D:\notis_analysis\NOTIS_DATA_{today}.xlsx', engine='openpyxl') as writer:
    #     if 'NOTIS_DATA' in writer.book.sheetnames:
    #         del writer.book['NOTIS_DATA']
    #     modified_df.to_excel(writer, index=False)
    write_notis_data(modified_df, matched_file[0])
    print('file modified')

if __name__ == '__main__':
    root_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(root_dir, 'data')
    modified_dir = os.path.join(root_dir, 'modified_data')
    dir_list = [data_dir, modified_dir]
    status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]
    main()

# df['user_name'] = np.where(df['ctclid'] == 400013041065130, 'Shubham Gagrani', (np.where(df.ctclid == 400013041161030, 'Mohit Vajpayee', 'Harshit Arora')))