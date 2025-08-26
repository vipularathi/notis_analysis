import os, requests, io, base64, warnings
from datetime import datetime,date,time,timedelta
import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from django.utils.encoding import force_bytes, force_str
from common import volt_dir,table_dir,today, logger, read_data_db, calc_delta, write_notis_postgredb
from db_config import n_tbl_notis_delta_table,n_tbl_notis_eod_net_pos_cp_noncp

def change_delta():
    eod_df = read_data_db(for_table=n_tbl_notis_eod_net_pos_cp_noncp)
    eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True).dt.date
    delta_df = calc_delta(eod_df)
    logger.info(f"New delta table made with volatility file of {today}")
    write_notis_postgredb(df=delta_df,table_name=n_tbl_notis_delta_table,truncate_required=True)
    delta_df.to_excel(os.path.join(table_dir, f'delta_{today}_final.xlsx'), index=False)