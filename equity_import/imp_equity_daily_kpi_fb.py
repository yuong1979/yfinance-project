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
import json
from google.oauth2 import service_account
from google.cloud.firestore import Client
from secret import access_secret
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
from googleapiclient.discovery import build
from tools import error_email
import inspect



firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)



# #################################################################################################
# ####### Extracting data from yfinance into firebase and include usd values ######################
# #################################################################################################
# python -c 'from equity_import.imp_equity_daily_kpi_fb import imp_equity_daily_kpi_fb; imp_equity_daily_kpi_fb()'

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


def last_valid_fx_date():
    fxdocs = db.collection('fx').get()
    fx_list = []
    for j in fxdocs:
        fx_list.append(j._data['currency'])

    docs = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(30).get()
    for i in docs:
        fx_list_historical = list(i._data['currencyrates'].keys())
        result =  all(elem in fx_list_historical  for elem in fx_list)
        if result:
            # print("fx_list_historical contains all elements in fx_list")
            date = i._data['datetime_format'].strftime("%Y-%m-%d")
            break
        else :
            # print("fx_list_historical does not contains all elements in fx_list")
            pass
    return date


def imp_equity_daily_kpi_fb():

    collection = 'equity_daily_kpi'
    sleeptime = 3
    #check if the document in the collections latest date extracted is > 30 seconds ago, if so to exit(), if not proceed to run
    tz_UTC = pytz.timezone('UTC')
    time_seconds = 30
    latest_entry = db.collection(collection).order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(1).get()
    time_diff = datetime.now(tz_UTC) - latest_entry[0]._data['updated_datetime']

    if time_diff.seconds < time_seconds:
        print ('exiting because the latest entry has been extracted less than 30 seconds ago')
        exit()

    try:

        tz_SG = pytz.timezone('Singapore')
        datetime_SG = datetime.now(tz_SG)
        # hoursbeforeextract = 24
        # secb4extract = hoursbeforeextract * 60 * 60
        hoursbeforeextract = 1
        secb4extract = hoursbeforeextract * 60 * 1
        target_datetime = datetime_SG - timedelta(seconds=secb4extract)

        # if what is on record is updated less than 24(hoursbeforeextract) hours ago, we need to get the record for update
        docs = db.collection(collection).where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()
        print (len(docs), "number of entries to update")

        for i in docs:
            print ("Updating " + str(i._data['ticker']))
            # print ("Last time updated is " + str(i._data['updated_datetime']))
            recordtime = datetime.now(tz_SG)
            ticker = i._data['ticker']
            # print (ticker, "ticker to be extracted")
            companyinfo = yf.Ticker(ticker)

            #essential to import KPIs into collection first because kpis need to be present before its used to convert to USD
            data = {
                'kpi': companyinfo.info,
                'updated_datetime': recordtime,
                'activated': True
            }

            #inserting categories outside the kpi dictionary to enable easy filtering
            cat_list = ['country','industry','sector']

            for x in cat_list:
                try:
                    data[x] = companyinfo.info[x]
                except:
                    data[x] = ""

            try:
                currency = companyinfo.info['currency']
            except KeyError:
                db.collection(collection).document(i.id).set(data, merge=True)

                rate = 0
                print ("rates are zero so just record company info without recording usd amounts")
            else:
                currency_required = currency
                #converting currency from yfinance into correct currency that can be found in FX table
                currency_required = currencyclean(currency_required)
                # print ("the currency converted is ", currency_required)
                
                # Lookingup the fx rates to get rates - sometimes rates are not available and need to check previous day's rates
                # Also we need to make sure that FX is extracted on the specific before this, if not this will not work

                #### to replace the code below to extract the latest date that has fx rates existing
                fx_date_str= last_valid_fx_date()
                docs = db.collection('fxhistorical').document(fx_date_str).get()
                rate = docs._data["currencyrates"][currency_required]

                try:
                    fx_extract_time = recordtime - timedelta(days=1)
                    fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                    docs = db.collection('fxhistorical').document(fx_date_str).get()
                    rate = docs._data["currencyrates"][currency_required]
                except:
                    try:
                        fx_extract_time = recordtime - timedelta(days=2)
                        fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                        docs = db.collection('fxhistorical').document(fx_date_str).get()
                        rate = docs._data["currencyrates"][currency_required]
                    except:
                        try:
                            fx_extract_time = recordtime - timedelta(days=3)
                            fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                            docs = db.collection('fxhistorical').document(fx_date_str).get()
                            rate = docs._data["currencyrates"][currency_required]
                        except:
                            try:
                                fx_extract_time = recordtime - timedelta(days=4)
                                fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                                docs = db.collection('fxhistorical').document(fx_date_str).get()
                                rate = docs._data["currencyrates"][currency_required]
                            except:
                                try:
                                    fx_extract_time = recordtime - timedelta(days=5)
                                    fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                                    docs = db.collection('fxhistorical').document(fx_date_str).get()
                                    rate = docs._data["currencyrates"][currency_required]
                                except:
                                    try:
                                        fx_extract_time = recordtime - timedelta(days=5)
                                        fx_date_str = fx_extract_time.strftime("%Y-%m-%d")
                                        docs = db.collection('fxhistorical').document(fx_date_str).get()
                                        rate = docs._data["currencyrates"][currency_required]
                                    except:
                                        #### to replace the code below to extract the latest date that has fx rates existing
                                        fx_date_str= last_valid_fx_date()
                                        docs = db.collection('fxhistorical').document(fx_date_str).get()
                                        rate = docs._data["currencyrates"][currency_required]




                # print (rate, "is the rate and it is type is ", type(rate))
                rate = float(rate)

            kpilist = ['marketCap', 'enterpriseValue', 'freeCashflow', 'operatingCashflow', 
                    'totalDebt', 'totalRevenue', 'grossProfits', 'ebitda', 'currentPrice']

            kpi_dict = {}
            for j in kpilist:
                try:
                    kpi_in_USD = i._data['kpi'][j] / rate
                except Exception as e:
                    #if there is no kpi than replace with ""
                    kpi_in_USD = ""

                kpi_name_in_USD = j + "USD"
                kpi_dict[kpi_name_in_USD] = kpi_in_USD

            data.update(kpi_dict)

            #updating data into firebase
            db.collection(collection).document(i.id).set(data, merge=True)
            print ("Updated " + str(i._data['ticker']))
            time.sleep(sleeptime)

    except AttributeError as e:
        print (e)

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on equity_daily_kpi extract"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)




