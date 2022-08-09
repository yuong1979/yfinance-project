import pytz
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import time
from firebase_admin import firestore
import yfinance as yf
import pandas as pd
import numpy as np
import timeit



# ##################################################################################################
# ######### Updating tickerlist from companiesmarketcap.com into firebase ##########################
# ##################################################################################################
# db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

# #updating the tickerlist by checking if all the tickers in companiesmarketcap is updated.
# #if not it will copy the ticker to firebase collection - tickerlist
# tz_SG = pytz.timezone('Singapore')
# datetime_SG = datetime.now(tz_SG)
# maxpages = 70
# for i in range(1,maxpages):
#     time.sleep(0.5)
#     r = requests.get('https://companiesmarketcap.com/page/' + str(i))
#     soup = BeautifulSoup(r.text, 'html.parser')
#     results1 = soup.find_all(attrs={"class":'company-code'})
#     results2 = soup.find_all(attrs={"class":'company-name'})
#     #market cap
#     results3 = soup.find_all(attrs={"class":'td-right'})
#     print ("page number " + str(i) )

#     if len(results1) > 0:
#         #k count is to get the company name in results2 which is in a different dataset than results1
#         k = 0
#         for j in results1:
#             #get ticker
#             ticker = j.contents[1]
#             #check if ticker is inside firebase
#             if not db.collection('tickerlisttest').where("ticker", "==", ticker).get():
#                 #get name
#                 tickername = str(results2[k].contents[0]).strip()

#                 #loading data into firebase
#                 data={
#                     'ticker': ticker, 
#                     'tickername': tickername,
#                     'created_datetime': datetime_SG,
#                     'updated_datetime': datetime_SG,
#                     'marketcap': 0,
#                     'activated': True
#                     }
#                 db.collection('tickerlisttest').add(data)
#                 print (ticker + " uploaded")
#             else:
#                 print (ticker + " passed")
#             k = k + 1







# #########################################################################
# ####### reading data ####################################################
# #########################################################################

# # select based on a specific criteria like sector and marketcap
# market_cap = 1000_000_000
# docs = db.collection('tickerlisttest').where('marketCap', '>=', market_cap).stream()
# for i in docs:
#     print (i._data['ticker'])

# sector = "Technology"
# docs = db.collection('tickerlisttest').where('sector', '>=', sector).stream()
# for i in docs:
#     print (i._data['ticker'])

# # Reading data into another destination
# tickerlisttest = db.collection('tickerlisttest').get()
# for i in tickerlisttest:
#     obj = db.collection('tickerlisttest').document(i.id).get()

#     print(obj._data['ticker'])
#     print(obj._data['tickername'])
#     print(obj._data['kpi']['sector'])
#     print(obj._data['updated_datetime'])

# # Reading one single ticker
# docs = db.collection("tickerlist").where("ticker", "==", "NENT-B.ST").get()
# print(docs[0]._data['kpi']['currency'])


# # if required to do a sort
# tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()



# ################################################################
# ####### deleting documents and fields  #########################
# ################################################################


# # delete all documents in a collection
# collection = "tickerlisttest"
# docs = db.collection(collection).get()
# for doc in docs:
#     key = doc.id
#     db.collection(collection).document(key).delete()

# # delete kpi fields from a collection
# collection = "tickerlisttest"
# fieldtodelete = "longBusinessSummary"
# docs = db.collection(collection).get()
# for doc in docs:
#     key = doc.id
#     db.collection(collection).document(key).update({
#     fieldtodelete: firestore.DELETE_FIELD
# })

# # delete certain documents in a collection
# collection = "kpilist"
# docs = db.collection(collection).where("rate", "==", 0).get()
# for doc in docs:
#     key = doc.id
#     print (doc.id)
#     db.collection(collection).document(key).delete()




# ######################################################################################
# ####### Extracting whole ticker dataset to csv for analysis ##########################
# ######################################################################################


# # print the datetime to csv to analyse data if the code is working correctly
# obj = db.collection('tickerlist').get()
# print (len(obj))
# datalist = []
# for i in obj:

#     try:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],i._data['kpi']])
#     except KeyError:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],"does_not_exists"])

# df = pd.DataFrame(datalist)
# df.replace(np.nan, '', inplace=True)
# df.to_csv("testtime.csv")




# ######################################################################################
# ####### Migrating the testing ticker data to tickerdatatest ##########################
# ######################################################################################

# tickerlist = db.collection('tickerlist').limit(50).get()

# for i in tickerlist:

#     ticker = i._data['ticker']
#     activated = i._data['activated']
#     created_datetime = i._data['created_datetime']
#     ebitdaUSD = i._data['ebitdaUSD']
#     enterpriseValueUSD = i._data['enterpriseValueUSD']
#     freeCashflowUSD = i._data['freeCashflowUSD']
#     grossProfitsUSD = i._data['grossProfitsUSD']
#     kpi = i._data['kpi']
#     marketCapUSD = i._data['marketCapUSD']
#     operatingCashflowUSD = i._data['operatingCashflowUSD']
#     tickername = i._data['tickername']
#     totalDebtUSD = i._data['totalDebtUSD']
#     totalRevenueUSD = i._data['totalRevenueUSD']
#     updated_datetime = i._data['updated_datetime']

#     data = {

#         'ticker': ticker,
#         'activated': activated,
#         "created_datetime": created_datetime,
#         "ebitdaUSD": ebitdaUSD,
#         "enterpriseValueUSD": enterpriseValueUSD,
#         "grossProfitsUSD": grossProfitsUSD,
#         "kpi": kpi,
#         "marketCapUSD": marketCapUSD,
#         "operatingCashflowUSD": operatingCashflowUSD,
#         "tickername": tickername,
#         "totalDebtUSD": totalDebtUSD,
#         "totalRevenueUSD": totalRevenueUSD,
#         "updated_datetime": updated_datetime
#     }

#     db.collection('tickerlisttest').document(i.id).set(data, merge=True)





# ##################################################################################################
# ###### Extracting from alpha vantage for analysis VANTAGE DOES NOT HAVE ALL I HAVE ###############
# ##################################################################################################

## you might want to concern https://marketstack.com/?utm_source=Geekflare&utm_medium=LeadsAcquisition&utm_content=Listing
## as an alternative


# from secret import alpha_vantage
# api_key = alpha_vantage.api_key
# # ticker = 'D05.SI'
# # r = requests.get('https://www.alphavantage.co/query?function=OVERVIEW&symbol='+ticker+'&apikey=' + api_key)
# # dataobj = r.json()
# # print (dataobj)

# # search = 'dbs'
# # r = requests.get('https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords='+search+'&apikey=' + api_key)
# # dataobj = r.json()
# # print (dataobj)




##############################################################
######## Adding data to firebase #############################
##############################################################

# # reference for updating tickerlist
# testlist = ['apple', 'facebook', 'google', 'netflix']

# for i in testlist:
#     if not db.collection('tickerlisttest').where("ticker", "==", i).get():
#         data={'ticker': i, 'activated': True}
#         db.collection('tickerlisttest').add(data)
#         print (i + " has been added")
#     else:
#         print (i + " is already included")
#         pass

