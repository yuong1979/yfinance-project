# import pytz
# import requests
# from datetime import datetime, timedelta
# import time
# from firebase_admin import firestore
# import yfinance as yf
# import pandas as pd
# import numpy as np
# from google.oauth2 import service_account
# import json
# from google.cloud.firestore import Client
# from secret import access_secret
# from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
# from email_function import error_email
# import inspect
# from export_gs import export_gs_func


# firestore_api_key = access_secret(firestore_api_key, project_id)
# firestore_api_key_dict = json.loads(firestore_api_key)
# fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
# db = Client(firebase_database, fbcredentials)



# def currencyclean(currency):
#     if currency == "GBp":
#         currency_cleaned = "GBP"
#     elif currency == "ZAc":
#         currency_cleaned = "ZAR"
#     elif currency == "ILA":
#         currency_cleaned = "ILS"
#     else:
#         currency_cleaned = currency
#     return currency_cleaned


# # #################################################################################################
# # ####### Extracting data from yfinance into firebase and include usd values ######################
# # #################################################################################################
# # python -c 'from mgt_fin_exp_fb import ext_daily_equity_financials_yf_fb; ext_daily_equity_financials_yf_fb()'

# def ext_daily_equity_financials_yf_fb():

#     #check if the document in the collections latest date extracted is > 30 seconds ago, if so to exit(), if not proceed to run
#     tz_UTC = pytz.timezone('UTC')
#     time_seconds = 30
#     latest_entry = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(1).get()
#     time_diff = datetime.now(tz_UTC) - latest_entry[0]._data['updated_datetime']

#     if time_diff.seconds < time_seconds:
#         print ('exiting because the latest entry has been extracted less than 30 seconds ago')
#         exit()

#     try:

#         tz_SG = pytz.timezone('Singapore')
#         datetime_SG = datetime.now(tz_SG)

#         hoursbeforeextract = 24
#         secb4extract = hoursbeforeextract * 60 * 60

#         target_datetime = datetime_SG - timedelta(seconds=secb4extract)

#         # if what is on record is updated less than 24(hoursbeforeextract) hours ago, we need to get the record for update
#         tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
#         print (len(tickerlist), "number of entries to update")

#         for i in tickerlist:
#             print ("Updating " + str(i._data['ticker']))
#             # print ("Last time updated is " + str(i._data['updated_datetime']))
#             recordtime = datetime.now(tz_SG)
#             ticker = i._data['ticker']
#             # print (ticker, "ticker to be extracted")
#             companyinfo = yf.Ticker(ticker)
#             data = {
#                 'kpi': companyinfo.info,
#                 'updated_datetime': recordtime,
#                 'record_time_kpi': recordtime,
#                 'activated': True
#             }
#             try:
#                 i._data['kpi']["currency"]

#             except KeyError:
#                 db.collection('tickerlist').document(i.id).set(data, merge=True)

#                 rate = 0
#                 print ("rates are zero so just record company info without recording usd amounts")
#             else:
#                 currency_required = i._data['kpi']["currency"]
#                 #converting currency from yfinance into correct currency that can be found in FX table
#                 currency_required = currencyclean(currency_required)
#                 # print ("the currency converted is ", currency_required)
                
#                 # Lookingup the fx rates to get rates - sometimes rates are not available and need to check previous day's rates
#                 # Also we need to make sure that FX is extracted on the specific before this, if not this will not work 
#                 try:
#                     fx_extract_time = recordtime - timedelta(days=1)
#                     fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
#                     docs = db.collection('fxhistorical').document(fx_date_str).get()
#                     rate = docs._data["currencyrates"][currency_required]
#                 except:
#                     try:
#                         fx_extract_time = recordtime - timedelta(days=2)
#                         fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
#                         docs = db.collection('fxhistorical').document(fx_date_str).get()
#                         rate = docs._data["currencyrates"][currency_required]
#                     except:
#                         try:
#                             fx_extract_time = recordtime - timedelta(days=3)
#                             fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
#                             docs = db.collection('fxhistorical').document(fx_date_str).get()
#                             rate = docs._data["currencyrates"][currency_required]
#                         except:
#                             pass

#                 # print (rate, "is the rate and it is type is ", type(rate))
#                 rate = float(rate)

#             kpilist = ['marketCap', 'enterpriseValue', 'freeCashflow', 'operatingCashflow', 
#                     'totalDebt', 'totalRevenue', 'grossProfits', 'ebitda', 'currentPrice']

#             kpi_dict = {}
#             for j in kpilist:
#                 try:
#                     kpi_in_USD = i._data['kpi'][j] * rate
#                 except Exception as e:
#                     #if there is no kpi than replace with ""
#                     kpi_in_USD = ""

#                 kpi_name_in_USD = j + "USD"
#                 kpi_dict[kpi_name_in_USD] = kpi_in_USD

#             data.update(kpi_dict)

#             #updating data into firebase
#             db.collection('tickerlist').document(i.id).set(data, merge=True)
#             print ("Updated " + str(i._data['ticker']))
#             time.sleep(5)


#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on macrokpi project"
#         content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
#         error_email(subject, content)













# ########################################################################################
# ###########  Export detailed annual and quarterly financials to fb  ####################
# ########################################################################################
# # python -c 'from mgt_fin_exp_fb import detail_fin_ext; detail_fin_ext()'


# def detail_fin_ext():
#     collection_to_update = 'tickerlisttest'
#     tz_SG = pytz.timezone('Singapore')
#     datetime_SG = datetime.now(tz_SG)
#     hoursbeforeextract = 48
#     secb4extract = hoursbeforeextract * 60 * 60
#     target_datetime = datetime_SG - timedelta(seconds=secb4extract)

#     # if what is on record is updated less than 24(hoursbeforeextract) hours ago, we need to get the record for update
#     docs = db.collection(collection_to_update).where('record_time_financials', '<=', target_datetime).order_by("record_time_financials", direction=firestore.Query.ASCENDING).get()
#     print (len(docs), "number of entries to update")

#     data_all = {}

#     #looping through the tickers
#     for tick in docs:
        
#         print ("Updating " + str(tick._data['ticker']))

#         recordtime = datetime.now(tz_SG)
#         ticker = tick._data['ticker']
#         companyinfo = yf.Ticker(ticker)

#         time_series_list = ['quarterly_financials','quarterly_balancesheet',
#                             'quarterly_cashflow','financials','balancesheet','cashflow']
#         dataset = {}

#         for kpi in time_series_list:
#             df = getattr(companyinfo, kpi)
#             df = df.to_dict()
#             date_list = list(df.keys())
#             val_list = list(df.values())
#             data_ind = {}
#             date_ind = {}

#             #relabelling the dates without time and adding them to a dictionary
#             j = 0
#             for date in date_list:
#                 fin_date = date.strftime("%Y-%m-%d")
#                 fin_values = val_list[j]
#                 data_ind[fin_date] = fin_values
#                 j = j + 1
            
#             #looping through and financials and inserting them
#             date_ind[kpi] = data_ind
            
#             #adding the quarterly/annually financials
#             dataset.update(date_ind)

#         data_all["time_series_financials"] = dataset

#         data = {
#             'activated': True,
#             'record_time_financials': recordtime
#         }

#         data.update(data_all)

#         # print (data)

#         #updating data into firebase
#         db.collection(collection_to_update).document(tick.id).set(data, merge=True)
#         print ("Updated " + str(tick._data['ticker']))










