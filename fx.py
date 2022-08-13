import requests
import time
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
from secret import access_secret
import json
from google.oauth2 import service_account
from google.cloud.firestore import Client

### Run the below first if running on local to connect to secret manager on google cloud
# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"

firebase_database = "blockmacro-7b611"
# firebase_database = "python-firestore-52cfc"
api_key = access_secret("blockmacro_alpha_vantage_api")
firestore_api_key = access_secret("blockmacro_firebase_db")
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

# upload fx to firebase
fx_list = [
    "CNY","INR","CAD","USD","ARS","EUR","KRW","HKD","AUD",
    "MXN","CZK","GBP","TWD","CHF","CLP","BRL","JPY","SGD",
    "NOK","ILS","ZAR","SEK","RUB","NZD","IDR","PLN","SAR",
    "MYR","TRY","THB","PEN","DKK","NGN","HUF","COP","PHP",
    "GBP","ILA","ISK"
]


#######################################################################################################
########################## Refreshing the currency rates ##############################################
#######################################################################################################
# python -c 'from fx import extract_fx; extract_fx()'

def extract_fx():
    
    tz_SG = pytz.timezone('Singapore')
    datetime_SG = datetime.now(tz_SG)

    docs = db.collection('fx').get()
    for doc in docs:
        key = doc.id
        currency = doc._data["currency"]
        try:
            r = requests.get('https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency='+currency+'&apikey=' + api_key)
            dataobj = r.json()
            rate = dataobj['Realtime Currency Exchange Rate']['5. Exchange Rate']
            data = {"rate": rate, "updated_datetime": datetime_SG}
            db.collection('fx').document(key).set(data, merge=True)
            print (currency, " has been updated")

        except Exception as e:
            rate = ""
            data = {"rate": rate, "updated_datetime": datetime_SG}
            db.collection('fx').document(key).set(data, merge=True)
            print (currency, " is skipped because of " + str(e))
    #sleep is at 20 seconds because it will pop errors if limits are breached
        time.sleep(20)

# #######################################################################################################
# ########################## Exporting the currency rates on csv for analysis ###########################
# #######################################################################################################

# # print the datetime to csv to analyse data if the code is working correctly
# obj = db.collection('fx').get()
# print (len(obj))
# datalist = []
# for i in obj:
#     datalist.append([i._data['currency'],i._data['rate']])

# df = pd.DataFrame(datalist)
# # df.replace(np.nan, '', inplace=True)
# df.to_csv("analysis.csv")


########################################################################################################
########################### Adding to the currency list ################################################
########################################################################################################


# #adding any additional new currency in the above list(just add to the list any new ones) and run the below
# for i in fx_list:
#     if not db.collection('fx').where("currency", "==", i).get():
#         data={'currency': i, 'rate': 0, 'activated': True}
#         db.collection('fx').add(data)
