import pytz
import requests
from datetime import datetime, timedelta
import time
from firebase_admin import firestore
import yfinance as yf
import pandas as pd
import numpy as np
from google.oauth2 import service_account
import json
from google.cloud.firestore import Client
from secret import access_secret


### Run the below first if running on local to connect to secret manager on google cloud
# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"

firebase_database = "blockmacro-7b611"
# firebase_database = "python-firestore-52cfc"
firestore_api_key = access_secret("blockmacro_firebase_db")
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)


# #################################################################################################
# ####### Extracting data from yfinance into firebase and include usd values (UNDER TESTING) ######
# ####### To replace mgt_fin_exp_fb.py when this is completed #####################################
# #################################################################################################

############### Running the function from the command line ###############
# python -c 'from mgt_fin_exp_fb import extract_to_fb; extract_to_fb()'

def extract_to_fb():

    #converting currency from yfinance into correct currency that can be found in alpha vantage
    def currencyclean(currency):
        if currency == "GBp":
            currency_cleaned = "GBP"
        elif currency == "ZAc":
            currency_cleaned = "ZAR"
        elif currency == "ILA":
            currency_cleaned = "ILS"
        else:
            currency_cleaned = currency
        return currency_cleaned

    tz_SG = pytz.timezone('Singapore')
    datetime_SG = datetime.now(tz_SG)

    hoursbeforeextract = 24
    secb4extract = hoursbeforeextract * 60 * 60

    target_datetime = datetime_SG - timedelta(seconds=secb4extract)

    # if what is on record is updated less than 24(hoursbeforeextract) hours ago, we need to get the record for update
    tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
    print (len(tickerlist), "number of entries to update")


    for i in tickerlist:
        print ("Updating " + str(i._data['ticker']))
        # print ("Last time updated is " + str(i._data['updated_datetime']))
        recordtime = datetime.now(tz_SG)
        time.sleep(1)
        ticker = i._data['ticker']
        updated_time = i._data['updated_datetime']
        companyinfo = yf.Ticker(ticker)

        try:
            # print (i._data['kpi']["currency"])
            currency_to_cln = i._data['kpi']["currency"]
            #converting currency from yfinance into correct currency that can be found in alpha vantage
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
            'updated_datetime': recordtime,
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





# # ###############################################################################################################
# # ######## Daily Updating firebase finance info with yfinance - OLD VERSION ( REPLACED ) ########################
# # ###############################################################################################################

# tz_SG = pytz.timezone('Singapore')
# datetime_SG = datetime.now(tz_SG)

# hoursbeforeextract = 72
# secb4extract = hoursbeforeextract * 60 * 60

# target_datetime = datetime_SG - timedelta(seconds=secb4extract)

# # if what is on record is updated less than 23(hoursbeforeextract) hours ago, we need to get the record for update
# tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
# print (len(tickerlist), "number of entries to update")

# ###############################################################################################
# ################### include below except when analyzing the data ##############################
# ###############################################################################################
# # datalist = []
# # for i in tickerlist:
# #     try:
# #         datalist.append([i._data['updated_datetime'],i._data['tickername'],i._data['kpi']])
# #     except KeyError:
# #         datalist.append([i._data['updated_datetime'],i._data['tickername'],"does_not_exists"])
# # df = pd.DataFrame(datalist)
# # df.replace(np.nan, '', inplace=True)
# # df.to_csv("testtime.csv")

# for i in tickerlist:
#     time.sleep(1)
#     ticker = i._data['ticker']
#     updated_time = i._data['updated_datetime']
#     print (updated_time)
#     companyinfo = yf.Ticker(ticker)
#     data={
#         'kpi': companyinfo.info,
#         'updated_datetime': datetime_SG,
#         'activated': True
#         }
#     #updating data into firebase
#     db.collection('tickerlist').document(i.id).set(data, merge=True)
#     print ("Updated " + str(i._data['ticker']))
