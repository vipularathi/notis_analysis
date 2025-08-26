import os, requests, io, base64, warnings
from datetime import datetime,date,time,timedelta
import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from django.utils.encoding import force_bytes, force_str
from common import volt_dir,table_dir,today, logger, read_data_db, calc_delta, write_notis_postgredb
from db_config import n_tbl_notis_delta_table,n_tbl_notis_eod_net_pos_cp_noncp

warnings.filterwarnings('ignore')

# today=datetime.today().date().replace(day=2)
SECRET_KEY = "yi91poFLFMiXnkB12j/KY0RjG1fwTO7MwQWXjszcPGE="
value = force_bytes("ARathi@123456")
member_code = '06769'
login_id = '06769APIIT19'
backend = default_backend()
key = force_bytes(base64.urlsafe_b64decode(SECRET_KEY))
session_token = ''
# today = datetime.now().date()
yesterday = today - timedelta(days=1)
base_url = 'https://www.connect2nse.com/extranet-api'

class Crypto:
    def __init__(self):
        self.encryptor = Cipher(algorithms.AES(key), modes.ECB(), backend).encryptor()
        self.decryptor = Cipher(algorithms.AES(key), modes.ECB(), backend).decryptor()

    def encrypt(self):
        padder = padding.PKCS7(algorithms.AES(key).block_size).padder()
        padded_data = padder.update(value) + padder.finalize()
        encrypted_text = self.encryptor.update(padded_data) + self.encryptor.finalize()
        return encrypted_text

    def decrypt(self, value):
        padder = padding.PKCS7(algorithms.AES(key).block_size).unpadder()
        decrypted_data = self.decryptor.update(value)
        unpadded = padder.update(decrypted_data) + padder.finalize()
        return unpadded

def login():
    global session_token
    url = f'{base_url}/login/2.0'

    crypto = Crypto()
    encrypted_password = force_str(base64.urlsafe_b64encode(crypto.encrypt()))
    logger.info(f'Encrypted Password::>>{encrypted_password}')

    payload = {
        "memberCode": member_code,
        "loginId": login_id,
        "password": encrypted_password
    }

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        session_token = response_data.get('token')
        # logger.info("Login successful:", response_data)
        logger.info(f"Login successful.\nSession token: {session_token}")
        return True
    else:
        logger.info(f"Login failed. Status code: {response.status_code}, Message: {response.text}")

def change_delta():
    eod_df = read_data_db(for_table=n_tbl_notis_eod_net_pos_cp_noncp)
    eod_df['EodExpiry'] = pd.to_datetime(eod_df['EodExpiry'], dayfirst=True).dt.date
    delta_df = calc_delta(eod_df)
    logger.info("New delta table made with today's volatility file")
    write_notis_postgredb(df=delta_df,table_name=n_tbl_notis_delta_table,truncate_required=True)
    delta_df.to_excel(os.path.join(table_dir, f'delta_{today}_final.xlsx'), index=False)

def download_volatility_file():
    download_url = f'{base_url}/common/file/download/2.0?'
    segment = 'FO'
    folder_path = '/Volatility'
    file_name = f'FOVOLT_{today.strftime("%d%m%Y")}.csv' #sample=FOVOLT_26032025
    params = {
        "segment" : segment,
        "folderPath" : folder_path,
        "filename" : file_name
    }
    # final_url = f"{download_url}segment={segment}&folderPath={folder_path}&filename={file_name}"
    file_path = os.path.join(volt_dir, f'{file_name}')
    headers = {'Authorization':f'Bearer {session_token}'}

    # logger.info(f"Downloading from URL: {final_url}")
    logger.info(f"Saving to: {file_path}")

    response = requests.get(download_url, headers=headers, params=params)
    logger.info(f'Response status::>>{response}')
    if response.status_code == 200:
        volt_df = pd.read_csv(io.BytesIO(response.content))
        with open(file_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"File downloaded successfully at {file_path}")
        change_delta()
        return
    else:
        logger.info(f"Could not download the file.\nStatus code: {response.status_code}, Message: {response.text}")


if login():
    download_volatility_file()