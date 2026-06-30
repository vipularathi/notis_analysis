import os, requests, io, base64, warnings, traceback
from datetime import datetime, date, time, timedelta
import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from django.utils.encoding import force_bytes, force_str
from common import volt_dir, table_dir, today, logger, read_data_db, calc_delta_v2, write_notis_postgredb, yesterday
import zipfile

warnings.filterwarnings('ignore')

# today=datetime.today().date().replace(day=2)
SECRET_KEY = "yi91poFLFMiXnkB12j/KY0RjG1fwTO7MwQWXjszcPGE="
value = force_bytes("Arssbl@06042026")
member_code = '06769'
login_id = '06769APIIT19'
backend = default_backend()
key = force_bytes(base64.urlsafe_b64decode(SECRET_KEY))
session_token = ''
base_url = 'https://www.connect2nse.com/extranet-api'
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "Referer": "https://www.bseindia.com/"
}
bse_base_url="https://www.bseindia.com/download/BhavCopy/Derivative/"

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


def download_volatility_file():
    download_url = f'{base_url}/common/file/download/2.0?'
    segment = 'FO'
    folder_path = '/Bhavcopy'
    file_name = f'BhavCopy_NSE_FO_0_0_0_{today.strftime("%Y%m%d")}_F_0000.csv.zip'
    # BhavCopy_NSE_FO_0_0_0_yyyymmdd_F_0000.csv.zip
    params = {
        "segment": segment,
        "folderPath": folder_path,
        "filename": file_name
    }
    # final_url = f"{download_url}segment={segment}&folderPath={folder_path}&filename={file_name}"
    file_path = os.path.join(volt_dir, f'{file_name}')
    headers = {'Authorization': f'Bearer {session_token}'}
    
    # logger.info(f"Downloading from URL: {final_url}")
    # logger.info(f"Saving to: {file_path}")
    
    response = requests.get(download_url, headers=headers, params=params)
    logger.info(f'Response status::>>{response}')
    if response.status_code == 200:
        # volt_df = pd.read_csv(io.BytesIO(response.content))
        with open(file_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"File downloaded successfully at {file_path}")
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(volt_dir)
        print(f"ZIP file extracted successfully to: {volt_dir}")
        logger.info(f"ZIP file extracted successfully to: {volt_dir}")
        
        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
            csv_filename = zip_ref.namelist()[0]  # gets the CSV filename inside the ZIP
            with zip_ref.open(csv_filename) as csv_file:
                volt_df = pd.read_csv(csv_file)
        logger.info(f"Data loaded into DataFrame, shape: {volt_df.shape}")
        
        return
    else:
        logger.info(f"Could not download the file.\nStatus code: {response.status_code}, Message: {response.text}")
        
def download_bse_bhavcopy_file():
    filename = f'BhavCopy_BSE_FO_0_0_0_{today.strftime("%Y%m%d")}_F_0000.csv'  #
    # BhavCopy_BSE_FO_0_0_0_yyyymmdd_F_0000.csv
    response = requests.get(url=bse_base_url+filename, headers=headers)
    file_path = os.path.join(volt_dir, f'{filename}')
    try:
        if response.status_code == 200:
            logger.info(f'BSE bhavcopy fetched.')
            with open(file_path, 'wb') as file:
                file.write(response.content)
            logger.info('bse bhavcopy file downloaded successfully')
            bse_bhav_df = pd.read_csv(io.BytesIO(response.content))
            logger.info(f'BSe bhavcopy dataframe shape: {bse_bhav_df.shape}')
            return
    except Exception as e:
        traceback.print_exc()
        print(f'\nexception occured: {e}')

if login():
    download_volatility_file()
    download_bse_bhavcopy_file()