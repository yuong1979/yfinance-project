from firebase_admin import firestore
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
from google.cloud.firestore import Client
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key, cloud_storage_key
from firebase_admin import firestore
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from secret import access_secret
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage
import math
import pytz
from google.cloud import storage
import os
import inspect


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)


####################################################################################################################################
######## Description: Extracting detailed time series price from firebase and dump into pickle and uploaded into gcp
######## This itself has no prerequisite
####################################################################################################################################

# #################################################################################################
# ####### Consolidating all historicals price #####################################################
# #################################################################################################
# python -c 'from equity_transform.eq_hist_price import export_eq_hist_price; export_eq_hist_price()'

def export_eq_hist_price():

    try:

        ticker_list = []
        price_list = []
        date_list = []

        docs = db.collection('equity_price_history').stream()
        # docs = db.collection('equity_price_history').limit(10).stream()
        count = 0
        for i in docs:
            # print (i._data['ticker'])
            ticker = i._data['ticker']

            try:
                data = i._data['price_history']['Close']

                for date, vals in data.items():
                    # print (ticker)
                    # print (date) #date
                    # print (vals) #vals
                    ticker_list.append(ticker)
                    date_list.append(date)
                    price_list.append(vals)

            except KeyError as e:

                    ticker_list.append(ticker)
                    date_list.append(None)
                    price_list.append(None)


            count = count + 1
            print (count)

        data = {
            "ticker": ticker_list,
            "date": date_list,
            "price": price_list,
        }

        df = pd.DataFrame(data)

        #change dates to datatime format
        df['date'] = pd.to_datetime(df['date'])
        #only select dates that are month end to reduce space and speed
        df = df[df['date'].dt.is_month_end]

        # print (df)

        df.to_pickle('data/eq_hist_price.pickle')

        upload_cloud_storage('eq_hist_price.pickle')
        print('upload sucessful')

        df = pd.read_pickle('data/eq_hist_price.pickle')
        print (df)


    except AttributeError as e:
        print (e)

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on export_eq_hist_price"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)


