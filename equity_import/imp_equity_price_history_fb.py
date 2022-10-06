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





##########################################################################################################
###########  export price history of equity into fb ######################################################
##########################################################################################################
# python -c 'from equity_import.imp_equity_price_history_fb import imp_equity_price_history_fb; imp_equity_price_history_fb()'


def imp_equity_price_history_fb():
    
    collection = 'equity_price_history'

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
        record_time = datetime.now(tz_SG)

        hoursbeforeextract = 1
        secb4extract = hoursbeforeextract * 60 * 1
        ### Use this as a regular extraction daily
        target_datetime = record_time - timedelta(seconds=secb4extract)

        #### after this has done running to comment this out and use days = 2/3 - instead of dates so far back
        #### if you need to extract data a long time back (during a situation when new tickers are added) -> change days to (365 * 5)
        extraction_start_datetime = record_time - timedelta(days=(3))
        startdate = extraction_start_datetime
        enddate = record_time

        # startdate = '2022-08-01'
        # enddate = '2022-08-19'

        docs = db.collection(collection).where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).stream()

        for doc in docs:

            recordtime = datetime.now(tz_SG)
            ticker = doc._data['ticker']
            ticker = yf.Ticker(ticker)
            print ("updating ", ticker )

            # df = data.history(period="max")
            df = ticker.history(start=startdate,  end=enddate)
            # the datetime needs to be changed if not firestore will not accept it
            df.index = pd.to_datetime(df.index, format = '%m/%d/%Y').strftime('%Y-%m-%d')

            try:

                #choose only the data you need
                df = df[['Close','Dividends']]
                df = df.to_dict()
                col_list = list(df.keys())
                val_list = list(df.values())

                data = {}
                data_ind = {}

                #relabelling the dates without time and adding them to a dictionary
                j = 0
                for k in col_list:
                    fin_values = val_list[j]
                    data_ind[k] = fin_values
                    j = j + 1

                    data = {
                        'activated': True,
                        'price_history':data_ind,
                        'updated_datetime': recordtime
                    }

                    db.collection(collection).document(doc.id).set(data, merge=True)

            except:

                data = {
                    'activated': True,
                    'price_history': {},
                    'updated_datetime': recordtime
                }

                db.collection(collection).document(doc.id).set(data, merge=True)

                print ("no data - skipped", ticker)


            print ("updated ", ticker )


    except AttributeError as e:
        print (e)

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on equity_price_history extract"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)