import pytz
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import time
from firebase_admin import firestore
import yfinance as yf


# ###############################################################################
# ######## Daily Updating firebase finance info with yfinance ###################
# ###############################################################################

db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

tz_SG = pytz.timezone('Singapore')
datetime_SG = datetime.now(tz_SG)

hoursbeforeextract = 36
secb4extract = hoursbeforeextract * 60 * 60

target_datetime = datetime_SG - timedelta(seconds=secb4extract)

# if what is on record is updated less than 23(hoursbeforeextract) hours ago, we need to get the record for update
tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).get()
print (len(tickerlist), "number of entries to update")

for i in tickerlist:
    time.sleep(1)
    ticker = i._data['ticker']
    companyinfo = yf.Ticker(ticker)
    data={
        'kpi': companyinfo.info,
        'updated_datetime': datetime_SG,
        'activated': True
        }
    #updating data into firebase
    db.collection('tickerlist').document(i.id).set(data, merge=True)
    print ("Updated " + str(i._data['ticker']))
