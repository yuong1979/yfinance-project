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
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage, convert_digits
import math
import pytz
from google.cloud import storage
import os
import inspect
import numpy as np


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)


####################################################################################################################################
######## Description: Extracting ratios from firebase that are updated realtime daily, cleaning it and  dump into pickle and uploaded into gcp
######## This itself has no prerequisite
####################################################################################################################################

# #################################################################################################
# ####### Extracting daily equity raw data from firebase storing it in pickle #####################
# #################################################################################################
# python -c 'from equity_transform.eq_daily_kpi import export_eq_daily_kpi; export_eq_daily_kpi()'

values_1 = ['shortName', 
        'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
        'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
        'currentRatio', 'quickRatio', 'debtToEquity',
        'heldPercentInsiders', 'heldPercentInstitutions',
        'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
        'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
        'pegRatio', 'trailingPegRatio',
        'trailingEps', 'forwardEps',
        'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate',
        'isEsgPopulated'
        ]

values_2 = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD']

def export_eq_daily_kpi():

    try:

        values_1.insert(0, 'industry')
        values_2.insert(0, 'updated_datetime')

        collection = 'equity_daily_kpi'
        docs = db.collection(collection).stream()
        # docs = db.collection(collection).limit(187).stream()

        data = {}
        for i in docs:
            data_kpi = {}

            # print (i._data['ticker'])
            for j in values_2:
                try:
                    data_kpi[j] = i._data[j]
                except KeyError:
                    data_kpi[j] = 0

            for k in values_1:
                try:
                    data_kpi[k] = i._data['kpi'][k]
                except KeyError:
                    data_kpi[k] = 0

            data[i._data['ticker']] = data_kpi

        df = pd.DataFrame.from_dict(data)
        df = df.transpose()

        # There is a row with all messed up data populated with datetime and "NAT", I have to use this to remove the row
        # this removes all rows if all columns in subset is of an invalid type
        df = df.dropna( how='all', subset=['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD'])

        ######## data cleaning #########
        # #originally industry has zeros in them, replacing it with '' which is a string that matches the rest of the data in the column
        df.replace("", np.nan, inplace=True)
        # #originally industry has zeros in them, replacing it with '' which is a string that matches the rest of the data in the column
        df.replace(0, np.nan, inplace=True)
        # because there is data into trailingPD and pricetosalestrailing12months that are named 'infinity
        df.replace("Infinity", np.nan, inplace=True)
        #industry has data labeled 0 and float-nan, changing all of them to string "" in the end
        df['industry'].replace(0, "", inplace=True)
        df['industry'].replace(np.nan, "", inplace=True)

        df.index.names = ['Ticker']

        #converting numerals to alphabets for large numbers
        sum_list = ['marketCapUSD','totalRevenueUSD','ebitdaUSD']

        for col in sum_list:
            col_alpha = "alpha_" + str(col)
            df[col_alpha] = df[col].apply(convert_digits)


        df.to_pickle('data/eq_daily_kpi.pickle')
        upload_cloud_storage('eq_daily_kpi.pickle')
        print('upload successful')

        # df = pd.read_pickle('data/eq_daily_kpi.pickle')
        # # print (df)
        # print (df.info(verbose=True)) 


    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on export_eq_daily_kpi"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)
