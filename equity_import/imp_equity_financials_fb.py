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



########################################################################################
###########  Export detailed annual and quarterly financials to fb  ####################
########################################################################################
# python -c 'from equity_import.imp_equity_financials_fb import imp_equity_financials_fb; imp_equity_financials_fb()'

def imp_equity_financials_fb():

    collection = 'equity_financials'

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
        docs = db.collection(collection).where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).stream()
        # print (len(docs), "number of entries to update") ## does not work with stream

        for tick in docs:
            
            print ("Updating " + str(tick._data['ticker']))

            recordtime = datetime.now(tz_SG)
            ticker = tick._data['ticker']
            companyinfo = yf.Ticker(ticker)

            time_series_list = ['quarterly_financials','quarterly_balancesheet',
                                'quarterly_cashflow','financials','balancesheet','cashflow']
            dataset = {}

            for kpi in time_series_list:
                df = getattr(companyinfo, kpi)
                df = df.to_dict()
                date_list = list(df.keys())
                val_list = list(df.values())
                data_ind = {}
                date_ind = {}

                try:
                    #relabelling the dates without time and adding them to a dictionary
                    j = 0
                    for date in date_list:
                        fin_date = date.strftime("%Y-%m-%d")
                        fin_values = val_list[j]
                        data_ind[fin_date] = fin_values
                        j = j + 1
                    
                    #looping through and financials and inserting them
                    date_ind[kpi] = data_ind

                except:
                    #looping through and financials and inserting them
                    date_ind[kpi] = {}

                #adding the quarterly/annually financials
                dataset.update(date_ind)

            data = {
                'activated': True,
                "time_series_financials": dataset,
                'updated_datetime': recordtime
            }

            #updating data into firebase
            db.collection(collection).document(tick.id).set(data, merge=True)
            print ("Updated " + str(tick._data['ticker']))

    except AttributeError as e:
        print (e)

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on equity_financials extract"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)


