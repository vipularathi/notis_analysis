from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse
import pandas as pd
import numpy as np
import uvicorn
from common import analyze_expired_instruments_v2, write_notis_postgredb

app = FastAPI()
@app.get('/', response_class=HTMLResponse)
def home():
    with open('templates/index.html') as f:
        return f.read()

@app.post('/upload')
async def uplaod_file(date_str : str = Form(...), file : UploadFile = File(...)):
    try:
        # print(date, type(date),'\n', df.head())
        rename_dict = {
            'Party Code': 'EodBroker',
            'Symbol': 'EodUnderlying',
            'Expiry Date': 'EodExpiry',
            'Strike Price': 'EodStrike',
            'Option Type': 'EodOptionType',
            'Opn Qty': 'EodNetQuantity',
            'OpnBuyTradgQty': 'buyQty',
            'OpnBuyTradgVal': 'buyValue',
            'OpnSellTradgQty': 'sellQty',
            'OpnSellTradgVal': 'sellValue',
            'Net qty': 'PreFinalNetQty'
        }
        for_date = pd.to_datetime(date_str).date()
        print(for_date, type(for_date))
        orig_eod_df = pd.read_excel(file.file)
        if not set(rename_dict.keys()) == set(orig_eod_df.columns):
            return {'success':False}
        orig_eod_df.rename(columns=rename_dict, inplace=True)
        orig_eod_df['buyAvgPrice'] = np.where(orig_eod_df['buyQty'] > 0,
                                              orig_eod_df['buyValue'] / orig_eod_df['buyQty'], 0)
        orig_eod_df['sellAvgPrice'] = np.where(orig_eod_df['sellQty'] > 0,
                                               orig_eod_df['sellValue'] / orig_eod_df['sellQty'], 0)
        orig_eod_df['ExpiredSpot_close'] = 0.0
        orig_eod_df['ExpiredRate'] = 0.0
        orig_eod_df['ExpiredAssn_value'] = 0.0
        orig_eod_df['ExpiredSellValue'] = 0.0
        orig_eod_df['ExpiredBuyValue'] = 0.0
        orig_eod_df['ExpiredQty'] = 0.0
        orig_eod_df = analyze_expired_instruments_v2(for_date=for_date, grouped_final_eod=orig_eod_df)
        orig_eod_df['FinalNetQty'] = orig_eod_df['PreFinalNetQty'] + orig_eod_df['ExpiredQty']
        orig_eod_df['EodBroker'] = np.where(orig_eod_df['EodBroker'] == 'AA100', 'non CP', 'CP')
        write_notis_postgredb(
            df=orig_eod_df,
            table_name=f'NOTIS_EOD_NET_POS_CP_NONCP_{for_date}',
            truncate_required=True
        )
        return {'success':True}
    except Exception as e:
        print(e)
        return {'success':False}
    
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8700)