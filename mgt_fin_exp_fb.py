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
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
from email_function import error_email
import inspect


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)



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


# #################################################################################################
# ####### Extracting data from yfinance into firebase and include usd values (UNDER TESTING) ######
# ####### To replace mgt_fin_exp_fb.py when this is completed #####################################
# #################################################################################################

############### Running the function from the command line ###############
# python -c 'from mgt_fin_exp_fb import ext_daily_equity_financials_yf_fb; ext_daily_equity_financials_yf_fb()'

def ext_daily_equity_financials_yf_fb():

    #check if the document in the collections latest date extracted is > 30 seconds ago, if so to exit(), if not proceed to run
    tz_UTC = pytz.timezone('UTC')
    time_seconds = 30
    latest_entry = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(1).get()
    timedelta = datetime.now(tz_UTC) - latest_entry[0]._data['updated_datetime']

    if timedelta.seconds < time_seconds:
        print ('exit')
        exit()

    try:

        #converting currency from yfinance into correct currency that can be found in alpha vantage
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
            #fx is updated one day late - and you have to make sure that the FX is extracted at 12 everyday
            fx_extract_time = recordtime - timedelta(days=1)

            # print (fx_extract_time)
            time.sleep(1)
            ticker = i._data['ticker']
            # print (ticker, "ticker to be extracted")
            # updated_time = i._data['updated_datetime']
            companyinfo = yf.Ticker(ticker)

            data = {
                'kpi': companyinfo.info,
                'updated_datetime': recordtime,
                'activated': True
            }

            try:
                i._data['kpi']["currency"]
            except KeyError:
                db.collection('tickerlist').document(i.id).set(data, merge=True)
                rate = 0
                print ("rates are zero so just record company info without recording usd amounts")
            else:
                currency_required = i._data['kpi']["currency"]
                # print ("the currency extracted is ", currency_required)
                #converting currency from yfinance into correct currency that can be found in FX table
                currency_required = currencyclean(currency_required)

                # print ("the currency converted is ", currency_required)
                fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                print ("the date extracted from fxhistorial is ", fx_date_str)
                # Lookingup the fx rates to get rates
                docs = db.collection('fxhistorical').document(fx_date_str).get()
                # print ("currency type retrieved from fx db is",  docs.to_dict())

                # we need to make sure that FX is extracted on the specific before this - if not this will not work 
                rate = docs._data["currencyrates"][currency_required]
                #convert string to float
                # print (rate, "is the rate and it is type is ", type(rate))
                rate = float(rate)
                # print (rate)


            kpilist = ['marketCap', 'enterpriseValue', 'freeCashflow', 'operatingCashflow', 
                    'totalDebt', 'totalRevenue', 'grossProfits', 'ebitda', 'currentPrice']

            kpi_dict = {}
            for j in kpilist:
                try:
                    kpi_in_USD = i._data['kpi'][j] * rate
                except Exception as e:
                    #if there is no kpi than replace with ""
                    kpi_in_USD = ""

                kpi_name_in_USD = j + "USD"
                kpi_dict[kpi_name_in_USD] = kpi_in_USD

            data.update(kpi_dict)

            #updating data into firebase
            db.collection('tickerlist').document(i.id).set(data, merge=True)
            print ("Updated " + str(i._data['ticker']))


    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)















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
