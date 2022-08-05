import pytz
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import time
from firebase_admin import firestore
import yfinance as yf
import pandas as pd
import numpy as np



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



# #################################################################################################
# ####### Move kpi fields from a dictionary to a field in the main document if ####################
# ####### extraction becomes so huge filter needs to be done on firebase ##########################
# #################################################################################################


# # # move kpi fields from a dictionary within to a field in the main document
# docs = db.collection('tickerlisttest').stream()
# for doc in docs:
#     sector = doc._data['kpi']['sector']
#     marketCap = doc._data['kpi']['marketCap']
#     industry = doc._data['kpi']['industry']
#     longBusinessSummary = doc._data['kpi']['longBusinessSummary']
#     key = doc.id
#     data = {"marketCap": marketCap}
#     db.collection('tickerlisttest').document(key).update(data)
#     data = {"sector": sector}
#     db.collection('tickerlisttest').document(key).update(data)
#     data = {"industry": industry}
#     db.collection('tickerlisttest').document(key).update(data)
#     data = {"longBusinessSummary": longBusinessSummary}
#     db.collection('tickerlisttest').document(key).update(data)





# ##########################################################################################################
# ####### Managing kpilist - NO LONGER REQUIRED??? -TO DELETE IF NOT USED FOR 3 MONTH ######################
# ##########################################################################################################

# #Extracting data from yfinance and comparing to existing kpi inside firebase to
# #check what is the difference - and add the difference to the collection if neccessary

# #extracting data from the whole collection
# fb_kpilist = []
# docs = db.collection('kpilist').get()
# for i in docs:
#     fb_kpilist.append(i._data['kpi'])

# #extracting all kpis from yfinance
# yfinance_kpilist = []
# sample_company = "meta"
# companyticker = yf.Ticker(sample_company)
# info = companyticker.info
# for i in info:
#     yfinance_kpilist.append(i)

# #getting list of kpis in yfinance but not in firebase
# difference = list(set(yfinance_kpilist) - set(fb_kpilist))
# print (difference)

# ##adding the difference to the fb kpilist collection
# # for i in difference:
# #     data={'kpi': i, 'activated': True}
# #     db.collection('kpilist').add(data)


# # # update all collection in kpilist activated to true
# # docs = db.collection('kpilist').where("activated", "==", False).get()
# # for doc in docs:
# #     key = doc.id
# #     db.collection('kpilist').document(key).update({"activated": True})


# ##########################################################################################################
# ####### Count of the collection extracted after / before a certain date ##################################
# ##########################################################################################################

# day = 3
# month = 8
# year = 2022
# hour = 22
# minute = 0

# target_date = datetime(
#     year=year, 
#     month=month, 
#     day=day,
#     hour=hour,
#     minute=minute,
#     tzinfo = pytz.timezone('Singapore')
#     )

# print (target_date)

# # Count of the data that was extracted before after 12pm (based on above)
# tickerlisttest = db.collection('tickerlist').where("updated_datetime", ">", target_date).get()
# print(len(tickerlisttest))



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



# ################################################################
# ####### deleting documents and fields  #########################
# ################################################################


# # delete all documents in a collection
# docs = db.collection('tickerlist').get()
# for doc in docs:
#     key = doc.id
#     db.collection('tickerlist').document(key).delete()

# # delete kpi fields from a collection
# fieldtodelete = "longBusinessSummary"
# docs = db.collection('tickerlisttest').get()
# for doc in docs:
#     key = doc.id
#     db.collection('tickerlisttest').document(key).update({
#     fieldtodelete: firestore.DELETE_FIELD
# })




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

