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



# #################################################################################################
# ####### Extracting data from yfinance into firebase and include usd values (UNDER TESTING) ######
# ####### To replace mgt_fin_exp_fb.py when this is completed #####################################
# #################################################################################################

def currencyclean(currency):
    if currency == "GBp":
        currency_cleaned = "GBP"
    elif currency == "ZAc":
        currency_cleaned = "ZAR"
    else:
        currency_cleaned = currency
    return currency_cleaned


db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

tz_SG = pytz.timezone('Singapore')
datetime_SG = datetime.now(tz_SG)

hoursbeforeextract = 36
secb4extract = hoursbeforeextract * 60 * 60

target_datetime = datetime_SG - timedelta(seconds=secb4extract)

# if what is on record is updated less than 23(hoursbeforeextract) hours ago, we need to get the record for update
tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
print (len(tickerlist), "number of entries to update")


for i in tickerlist:
    time.sleep(1)
    ticker = i._data['ticker']
    updated_time = i._data['updated_datetime']
    companyinfo = yf.Ticker(ticker)

    try:
        # print (i._data['kpi']["currency"])
        currency_to_cln = i._data['kpi']["currency"]
        currency = currencyclean(currency_to_cln)
        docs = db.collection('fx').where("currency", "==", currency).get()[0]
        rate = docs.to_dict()['rate']
        #convert string to float
        rate = float(rate)
    except KeyError:
        rate = ""

    except Exception as e:
        print (e)

    key = i.id

    try:
        marketCapUSD = i._data['kpi']["marketCap"] * rate
    except Exception as e:
        marketCapUSD = ""

    try:
        enterpriseValueUSD = i._data['kpi']["enterpriseValue"] * rate
    except Exception as e:
        enterpriseValueUSD = ""

    try:
        freeCashflowUSD = i._data['kpi']["freeCashflow"] * rate
    except Exception as e:
        freeCashflowUSD = ""

    try:
        operatingCashflowUSD = i._data['kpi']["operatingCashflow"] * rate
    except Exception as e:
        operatingCashflowUSD = ""
        
    try:
        totalDebtUSD = i._data['kpi']["totalDebt"] * rate
    except Exception as e:
        totalDebtUSD = ""
        
    try:
        totalRevenueUSD = i._data['kpi']["totalRevenue"] * rate
    except Exception as e:
        totalRevenueUSD = ""
        
    try:
        grossProfitsUSD = i._data['kpi']["grossProfits"] * rate
    except Exception as e:
        grossProfitsUSD = ""
        
    try:
        ebitdaUSD = i._data['kpi']["ebitda"] * rate
    except Exception as e:
        ebitdaUSD = ""

    try:
        currentPriceUSD = i._data['kpi']["currentPrice"] * rate
    except Exception as e:
        currentPriceUSD = "" 

    data = {

        'kpi': companyinfo.info,
        'updated_datetime': datetime_SG,
        'activated': True,

        "marketCapUSD": marketCapUSD,
        "enterpriseValueUSD": enterpriseValueUSD,
        "freeCashflowUSD": freeCashflowUSD,
        "operatingCashflowUSD": operatingCashflowUSD,
        "totalDebtUSD": totalDebtUSD,
        "totalRevenueUSD": totalRevenueUSD,
        "grossProfitsUSD": grossProfitsUSD,
        "ebitdaUSD": ebitdaUSD,
        "currentPriceUSD": currentPriceUSD
    }

    #updating data into firebase
    db.collection('tickerlist').document(i.id).set(data, merge=True)
    print ("Updated " + str(i._data['ticker']))






# # #################################################################################################
# # ####### Convert certain KPI fields to USD value for export in google sheets #####################
# # #################################################################################################

# # there is an unknown error running the below script
# # AttributeError: '_UnaryStreamMultiCallable' object has no attribute '_retry'


# def currencyclean(currency):
#     if currency == "GBp":
#         currency_cleaned = "GBP"
#     elif currency == "ZAc":
#         currency_cleaned = "ZAR"
#     else:
#         currency_cleaned = currency
#     return currency_cleaned


# # # move fx and the USD values from a dictionary within to a field in the main document
# st = time.time()
# collection_to_update = "tickerlist"
# db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")
# docs = db.collection(collection_to_update).stream()
# for i in docs:

#     print (i._data["ticker"])
    
#     try:
#         print (i._data['kpi']["currency"])
#         currency_to_cln = i._data['kpi']["currency"]
#         currency = currencyclean(currency_to_cln)
#         docs = db.collection('fx').where("currency", "==", currency).get()[0]
#         rate = docs.to_dict()['rate']
#         #convert string to float
#         rate = float(rate)
#     except KeyError:
#         rate = ""

#     except Exception as e:
#         print (e)

#     key = i.id



#     try:
#          marketCapUSD = i._data['kpi']["marketCap"] * rate
#     except Exception as e:
#         marketCapUSD = ""

#     try:
#          enterpriseValueUSD = i._data['kpi']["enterpriseValue"] * rate
#     except Exception as e:
#         enterpriseValueUSD = ""

#     try:
#          freeCashflowUSD = i._data['kpi']["freeCashflow"] * rate
#     except Exception as e:
#         freeCashflowUSD = ""

#     try:
#          operatingCashflowUSD = i._data['kpi']["operatingCashflow"] * rate
#     except Exception as e:
#         operatingCashflowUSD = ""
        
#     try:
#          totalDebtUSD = i._data['kpi']["totalDebt"] * rate
#     except Exception as e:
#         totalDebtUSD = ""
        
#     try:
#          totalRevenueUSD = i._data['kpi']["totalRevenue"] * rate
#     except Exception as e:
#         totalRevenueUSD = ""
        
#     try:
#          grossProfitsUSD = i._data['kpi']["grossProfits"] * rate
#     except Exception as e:
#         grossProfitsUSD = ""
        
#     try:
#          ebitdaUSD = i._data['kpi']["ebitda"] * rate
#     except Exception as e:
#         ebitdaUSD = ""

#     try:
#          currentPriceUSD = i._data['kpi']["currentPrice"] * rate
#     except Exception as e:
#         currentPriceUSD = "" 

#     data = {
#         "marketCapUSD": marketCapUSD,
#         "enterpriseValueUSD": enterpriseValueUSD,
#         "freeCashflowUSD": freeCashflowUSD,
#         "operatingCashflowUSD": operatingCashflowUSD,
#         "totalDebtUSD": totalDebtUSD,
#         "totalRevenueUSD": totalRevenueUSD,
#         "grossProfitsUSD": grossProfitsUSD,
#         "ebitdaUSD": ebitdaUSD
#     }

#     db.collection(collection_to_update).document(key).update(data)

# et = time.time()

# print("elapsedtime" ,(et - st))







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

