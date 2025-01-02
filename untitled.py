import pandas as pd
import os
from main import read_notis_file
import time
from datetime import datetime

pd.set_option('display.max_columns', None)
root_dir = os.path.dirname(os.path.abspath(__file__))
# nnf_file_path = os.path.join(root_dir, 'Final_NNF_old.xlsx')
nnf_file_path = os.path.join(root_dir, "Final_NNF_ID.xlsx")
new_nnf_file_path = os.path.join(root_dir, 'NNF_ID.xlsx')

mod_time = os.path.getmtime(nnf_file_path)
# readable_mod_time = time.ctime(mod_time)
readable_mod_time = datetime.fromtimestamp(mod_time)

df = pd.read_excel(nnf_file_path, index_col=False)
df_new = pd.read_excel(new_nnf_file_path, index_col=False)
# df1 = read_notis_file(r"D:\notis_analysis\modified_data\NOTIS_DATA_12DEC2024.xlsx")
df = df.loc[:, ~df.columns.str.startswith('Un')]
df_new = df_new.loc[:, ~df_new.columns.str.startswith('Un')]
df.columns = df.columns.str.replace(' ', '', regex = True)
df_new.columns = df_new.columns.str.replace(' ', '', regex = True)
df.dropna(how='all', inplace=True)
df_new.dropna(how='all', inplace=True)
# df[['NNFID', 'NeatID']] = df[['NNFID', 'NeatID']].astype(int)
# df.NeatID = df.NNFID.astype(int)
list_col = [col for col in df.columns if not col.startswith('NNF')]
grouped_df = df.groupby(['NNFID'])[list_col].sum()
# for index, row in grouped_df.iterrows():
#     print('indx-',int(index),'\n', 'row-\n', row, '\n')

merged_df = pd. merge(df1, df, left_on='ctclid', right_on='NNFID', how='left')
print(merged_df)