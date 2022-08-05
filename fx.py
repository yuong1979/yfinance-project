from secret import alpha_vantage
from firebase_admin import firestore
import requests
import time
from datetime import datetime, timedelta
import pytz

# update fx values into firebase

api_key = alpha_vantage.api_key

db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")


# upload fx to firebase
fx_list = [
    "CNY","INR","CAD","USD","ARS","EUR","KRW","HKD","AUD",
    "MXN","CZK","GBP","TWD","CHF","CLP","BRL","JPY","SGD",
    "NOK","ILS","ZAR","SEK","RUB","NZD","IDR","PLN","SAR",
    "MYR","TRY","THB","PEN","DKK","NGN","HUF","COP","PHP",
    "GBP","ILA","ISK"
]


########################################################################################################
########################### Adding to the currency list ################################################
########################################################################################################


# #adding any additional new currency in the above list(just add to the list any new ones) and run the below
# for i in fx_list:
#     if not db.collection('fx').where("currency", "==", i).get():
#         data={'currency': i, 'rate': 0, 'activated': True}
#         db.collection('fx').add(data)


########################################################################################################
########################### Refreshing the currency rates ##############################################
########################################################################################################

# tz_SG = pytz.timezone('Singapore')
# datetime_SG = datetime.now(tz_SG)

# docs = db.collection('fx').get()
# for doc in docs:
#     key = doc.id
#     currency = doc._data["currency"]
#     try:
#         r = requests.get('https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency='+currency+'&apikey=' + api_key)
#         dataobj = r.json()
#         rate = dataobj['Realtime Currency Exchange Rate']['5. Exchange Rate']

#     except Exception as e:
#         print (currency, " is skipped because of " + str(e))
#         pass

#     data = {"rate": rate, "updated_datetime": datetime_SG}
#     db.collection('fx').document(key).update(data)
#     print (currency, " has been updated")
#     time.sleep(10)

