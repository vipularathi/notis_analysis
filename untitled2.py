# import re
# from datetime import datetime, timezone, timedelta
# import pandas as pd
#
# import base64
# from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
# from cryptography.hazmat.primitives import padding
# from cryptography.hazmat.backends import default_backend
# from django.utils.encoding import force_bytes, force_str
# import requests
# import os
#
# from common import today, volt_dir
#
# SECRET_KEY = "yi91poFLFMiXnkB12j/KY0RjG1fwTO7MwQWXjszcPGE="
# value = force_bytes("ARathi@123456")
# backend = default_backend()
# key = force_bytes(base64.urlsafe_b64decode(SECRET_KEY))
# access_token = ''
#
#
# class Crypto:
#     def __init__(self):
#         self.encryptor = Cipher(algorithms.AES(key), modes.ECB(), backend).encryptor()
#         self.decryptor = Cipher(algorithms.AES(key), modes.ECB(), backend).decryptor()
#
#     def encrypt(self):
#         padder = padding.PKCS7(algorithms.AES(key).block_size).padder()
#         padded_data = padder.update(value) + padder.finalize()
#         encrypted_text = self.encryptor.update(padded_data) + self.encryptor.finalize()
#         return encrypted_text
#
#     def decrypt(self, value):
#         padder = padding.PKCS7(algorithms.AES(key).block_size).unpadder()
#         decrypted_data = self.decryptor.update(value)
#         unpadded = padder.update(decrypted_data) + padder.finalize()
#         return unpadded
#
# def login():
#     global access_token
#     host = "https://www.connect2nse.com/extranet-api/"
#     memberCode = '06769'
#     loginId = '06769APIIT19'
#     crypto = Crypto()
#     password = force_str(base64.urlsafe_b64encode(crypto.encrypt()))
#     url = f"{host}/login/2.0"
#     payload = {"memberCode": memberCode, "loginId": loginId, "password": password}
#     headers = {'Content-Type': 'application/json'}
#     response = requests.post(url=url, json=payload, headers=headers)
#     # logger.info(response.content)
#     # data = response.json()
#     # return data
#     if response.status_code == 200:
#         response_data = response.json()
#         access_token = response_data.get('token')  # Store session token
#         print(f"Login successful. Access token: {access_token}")
#     else:
#         print(f"Login failed. Status code: {response.status_code}, Message: {response.text}")
#
# def download_file_from_UAT():
#     global access_token
#     segment = 'FO'
#     folder_path = 'volatility'
#     file_name = 'FOVOLT_26032025'
#
#     base_url = 'https://www.connect2nse.com/extranet-api/common/file/download/2.0'
#     final_url = f"{base_url}segment={segment}&folderPath={folder_path}&filename={file_name}"
#
#     # old code
#     # final_url = f"{base_url}{file_name}"
#     file_path = os.path.join(volt_dir, f'NSE_FO_Volatility_{today}.xlsx')
#     # extracted_file_path = os.path.join(data_dir, saved_file_name.replace('.gz', ''))
#
#     # print(f"Downloading from URL: {final_url}")
#     # print(f'Downloading file::>>{saved_file_name}')
#     # print(f"Saving to: {file_path}")
#     headers = {'Authorization':f'Bearer {access_token}','Content-Type': 'application/json'}
#     # response = requests.get(final_url, headers=headers)
#     try:
#         print(final_url)
#         response = requests.get(final_url, headers=headers)
#         print(response)
#         # print(f'Response status::>>{response},  {saved_file_name} downloaded successfully')
#     except requests.RequestException as e:
#         print(f'Failed to download {file_name}. Error ::::{e}')
#
#     # # file_path = os.path.join(self.data_dir, saved_file_name)
#     # # extracted_file_path = os.path.join(self.data_dir, saved_file_name.replace('.gz', ''))
#     # #
#     # # logger.info(f"Downloading from URL: {final_url}")
#     # # print(f'Downloading file::>>{saved_file_name}')
#     # # logger.info(f"Saving to: {file_path}")
#     #
#     # # Download the file
#     # try:
#     #     # time.sleep(1)
#     #     response = requests.get(final_url, headers=self.headers)
#     #     print(f'Response status::>>{response},  {saved_file_name} downloaded successfully')
#     #     response.raise_for_status()  # Raise an error for bad HTTP status codes
#     #     with open(file_path, 'wb') as file:
#     #         file.write(response.content)
#     #     logger.info(f"File downloaded successfully: {saved_file_name}")
#     #
#     #     # Extract the .gz file if it's a gzip file
#     #     if saved_file_name.endswith('.gz'):
#     #         self.extract_gzip(file_path, extracted_file_path, is_priority)
#     #
#     #         # Check if the file is 'contract.gz'
#     #         if saved_file_name == 'contract.gz' and segment == 'FO':
#     #             print(f'Extracted file path::>>{extracted_file_path}')
#     #             self.process_contract_file(extracted_file_path)
#     #             self.fno_master_inhouse(extracted_file_path)
#     #         if saved_file_name.startswith('CM_NSE_CM_security'):
#     #             self.process_cm_securities(extracted_file_path)
#     #
#     #     # Extract the .rar file if it's a rar file
#     #     elif saved_file_name.endswith('.rar'):
#     #         self.extract_rar(file_path, self.data_dir, is_priority)
#     #
#     #     # Extract .zip files
#     #     elif saved_file_name.endswith('.zip'):
#     #         self.extract_zip(file_path, self.data_dir, is_priority)
#     #         if saved_file_name.startswith('BhavCopy_NSE_FO_0_0_0_'):
#     #             # self.process_bhavcopy_fo(self.data_dir,file_name)
#     #             # user, password, host, port, database
#     #             self.process_bhavcopy_fo_test(self.data_dir, file_name, self.greek_server['user'],
#     #                                           self.greek_server['password'], self.greek_server['host'],
#     #                                           self.greek_server['port'], self.greek_server['dbname'])
#     #             self.process_bhavcopy_fo_test(self.data_dir, file_name, self.db_params_219['user'],
#     #                                           self.db_params_219['password'], self.db_params_219['host'],
#     #                                           self.db_params_219['port'], self.db_params_219['database'])
#     #         if saved_file_name.startswith('CM_BhavCopy_NSE_CM_0_0_0_'):
#     #             # self.process_bhavcopy_cm(self.data_dir,file_name)
#     #             self.process_bhavcopy_cm_test(self.data_dir, file_name, self.db_params_219['user'],
#     #                                           self.db_params_219['password'], self.db_params_219['host'],
#     #                                           self.db_params_219['port'], self.db_params_219['database'])
#     #             self.process_bhavcopy_cm_test(self.data_dir, file_name, self.greek_server['user'],
#     #                                           self.greek_server['password'], self.greek_server['host'],
#     #                                           self.greek_server['port'], self.greek_server['dbname'])
#     #
#     #     elif saved_file_name.startswith('fo_contract_stream_info'):
#     #         self.process_fo_contract_stream_info(self.data_dir, file_name, self.linux_server, self.linux_username,
#     #                                              self.linux_password, self.remote_path)
#     #
#     #
#     #
#     #
#     # except requests.RequestException as e:
#     #     print(f'Failed to download {file_name}. Error ::::{e}')
#     #     logger.error(f"Failed to download the file: {file_name}. Error: {e}\nTraceback: {traceback.format_exc()}")
#     #     if is_priority == 1:
#     #         body = f'Failed to download {file_name}. Error ::::{e}'
#     #         self.send_mail(body)
#     #         raise
#
# res=login()
# res=download_file_from_UAT()
# # crypto = Crypto()
# # text_encp_password_main = force_str(base64.urlsafe_b64encode(crypto.encrypt()))
# # encrypted_password = text_encp_password_main
# # print(f'Encrypted Password::>>{encrypted_password}')
#
# # if __name__ == '__main__':
# #     print('>>>>>>>>>>>')
# #     crypto = Crypto()
# #     text = force_str(base64.urlsafe_b64encode(crypto.encrypt()))
# #     print(text)
# #     # print('<<<<<<<<<<<<<')
# #     # text = force_str(crypto.decrypt(base64.urlsafe_b64decode(text)))
# #     # print(text)
# #     # text = force_str(crypto.decrypt(base64.urlsafe_b64decode("LvRHkSW+8OIMyk51T87KDQ==")))
# #     # print(text)
#
# # info = login()
# # access_token = info['result']['token']
#
#
# # volt_df = pd.read_csv(r"C:\Users\vipulanand\Downloads\FOVOLT_24032025.csv", index_col=False)
# # volt_df.columns = [re.sub(r'\s','',each) for each in df.columns]
# # sym_list = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY']
# # volt_df=volt_df.query("Symbol in @sym_list")
# # volt_df.rename(columns={'UnderlyingClosePrice(A)' : 'UnderlyingClosePrice'}, inplace=True)
# # df = volt_df[['Symbol','UnderlyingClosePrice']]
# # p=0
# # data = {
# #     'symbol': ['NIFTY'] * 5 + ['BANKNIFTY'] * 5 + ['FINNIFTY'] * 5
# # }
# # # df1 = read_data_db()
# # # merged_df = df1.merge(volt_df,how='left',left_on='symbol',right_on='Symbol')
# # p=0
p=0
import os, requests, io, base64
from datetime import datetime,date,time,timedelta
import pandas as pd
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from django.utils.encoding import force_bytes, force_str

SECRET_KEY = "yi91poFLFMiXnkB12j/KY0RjG1fwTO7MwQWXjszcPGE="
value = force_bytes("ARathi@123456")
member_code = '06769'
login_id = '06769APIIT19'
backend = default_backend()
key = force_bytes(base64.urlsafe_b64decode(SECRET_KEY))
session_token = ''
today = datetime.now().date()
yesterday = today - timedelta(days=1)
root_dir = os.getcwd()
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
    print(f'Encrypted Password::>>{encrypted_password}')

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
        # print("Login successful:", response_data)
        print(f"Login successful.\nSession token: {session_token}")
        return True
    else:
        print(f"Login failed. Status code: {response.status_code}, Message: {response.text}")

def download_volatility_file():
    df=pd.DataFrame()
    download_url = f'{base_url}/common/file/download/2.0?'
    segment = 'FO'
    folder_path = '/Volatility'
    file_name = f'FOVOLT_{yesterday.strftime("%d%m%Y")}.csv' #sample=FOVOLT_26032025
    params = {
        "segment" : segment,
        "folderPath" : folder_path,
        "filename" : file_name
    }
    # final_url = f"{download_url}segment={segment}&folderPath={folder_path}&filename={file_name}"
    file_path = os.path.join(root_dir, f'{file_name}')
    headers = {'Authorization':f'Bearer {session_token}'}

    # print(f"Downloading from URL: {final_url}")
    print(f"Saving to: {file_path}")

    response = requests.get(download_url, headers=headers, params=params)
    print(f'Response status::>>{response}')
    if response.status_code == 200:
        df = pd.read_csv(io.BytesIO(response.content))
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully at {file_path}")
    else:
        print(f"Could not download the file.\nStatus code: {response.status_code}, Message: {response.text}")
    # print(df.head())
    return df
if login():
    volt_df = download_volatility_file()
i=0
# import pandas as pd
# import os,re
# import numpy as np
# from common import today, yesterday,volt_dir, read_file, read_data_db
#
# volt_df = read_file(os.path.join(volt_dir,f'FOVOLT_{yesterday.strftime("%d%m%Y")}.csv'))
# volt_df.columns = [re.sub(r'\s','',each) for each in volt_df.columns]
# volt_df = volt_df.iloc[:,1:3]
# volt_df.rename(columns={'UnderlyingClosePrice(A)': 'SpotClosePrice'}, inplace=True)
# sym_list = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY']
# volt_df=volt_df.query("Symbol in @sym_list")
#
# tablename = f'test_cp_noncp_{today}'
# cp_df = read_data_db(for_table=tablename)
# cp_df.columns = [re.sub(r'Eod|\s','',each) for each in cp_df.columns]
#
# merged_df = cp_df.merge(volt_df,how='left',left_on=['Underlying'], right_on=['Symbol'])
# merged_df.drop(columns=['Symbol'], inplace=True)
# merged_df = merged_df.query("OptionType == 'CE' or OptionType == 'PE'")
# merged_df.drop_duplicates(inplace=True)
#
# pivot_df = merged_df.pivot_table(
#     index=['Broker','Underlying','SpotClosePrice'],
#     columns=['OptionType'],
#     values=['FinalNetQty'],
#     aggfunc={'FinalNetQty':'sum'},
#     fill_value=0
# )
# pivot_df.columns = ['CE','PE']
# pivot_df.reset_index(inplace=True)
# pivot_df.SpotClosePrice = pivot_df.SpotClosePrice.astype('float64')
# pivot_df['NetQty'] = pivot_df['CE']-pivot_df['PE']
# pivot_df['Exposure(in Crs)'] = (pivot_df['NetQty']*pivot_df['SpotClosePrice'])/10000000
o=0