import pytz
import requests
from datetime import datetime, timedelta
import time
from firebase_admin import firestore
import yfinance as yf
import pandas as pd
import numpy as np


# ###############################################################################################################
# ######## Daily Updating firebase finance info with yfinance - OLD VERSION ABOUT TO REPLACED ###################
# ###############################################################################################################

db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

tz_SG = pytz.timezone('Singapore')
datetime_SG = datetime.now(tz_SG)

hoursbeforeextract = 36
secb4extract = hoursbeforeextract * 60 * 60

target_datetime = datetime_SG - timedelta(seconds=secb4extract)

# if what is on record is updated less than 23(hoursbeforeextract) hours ago, we need to get the record for update
tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
print (len(tickerlist), "number of entries to update")

###############################################################################################
################### exclude below except when analyzing the data ##############################
###############################################################################################
# datalist = []
# for i in tickerlist:
#     try:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],i._data['kpi']])
#     except KeyError:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],"does_not_exists"])
# df = pd.DataFrame(datalist)
# df.replace(np.nan, '', inplace=True)
# df.to_csv("testtime.csv")

for i in tickerlist:
    time.sleep(1)
    ticker = i._data['ticker']
    updated_time = i._data['updated_datetime']
    print (updated_time)
    companyinfo = yf.Ticker(ticker)
    data={
        'kpi': companyinfo.info,
        'updated_datetime': datetime_SG,
        'activated': True
        }
    #updating data into firebase
    db.collection('tickerlist').document(i.id).set(data, merge=True)
    print ("Updated " + str(i._data['ticker']))
