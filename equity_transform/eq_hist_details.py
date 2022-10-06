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
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage, date_mapper
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
######## Description: Extracting detailed time series financials from firebase annual quarterly/ bs / cf / pl, adding dates mapping 
######## to it and to dump into pickle and uploaded into gcp - You have to run this before running eq_hist_sum.py
######## This itself has no prerequisite
####################################################################################################################################


# #################################################################################################
# ####### Extracting all historicals financials ################################################
# #################################################################################################
# python -c 'from equity_transform.eq_hist_details import export_eq_hist_details; export_eq_hist_details()'


def export_eq_hist_details():

    try:

        ticker_list = []
        date_list = []
        cattype_list = []
        kpi_list = []
        value_list = []

        docs = db.collection('equity_financials').stream()
        # docs = db.collection('equity_financials').limit(100).stream()

        cat_list = ['balancesheet','cashflow','financials','quarterly_balancesheet','quarterly_cashflow','quarterly_financials']

        count = 0
        for i in docs:
            for cat in cat_list:
                data = i._data['time_series_financials'][cat]
                # print (i._data['ticker']) #ticker
                # print (cat) #datatype
                for date, vals in data.items():
                    # print (date) #date
                    # # print (vals)
                    for kpi, val in vals.items():
                        # print (kpi) #kpi
                        # print (val) #values
                        ticker_list.append(i._data['ticker'])
                        date_list.append(date)

                        if cat == 'balancesheet':
                            cattype_list.append('annual_balancesheet')
                        elif cat == 'cashflow':
                            cattype_list.append('annual_cashflow')
                        elif cat == 'financials':
                            cattype_list.append('annual_profit&loss')
                        elif cat == 'quarterly_financials':
                            cattype_list.append('quarterly_profit&loss')
                        else:
                            cattype_list.append(cat)

                        kpi_list.append(kpi)
                        value_list.append(val)
            
            count = count + 1
            print (count)

        data = {
            "ticker": ticker_list,
            "date": date_list,
            "cattype": cattype_list,
            "kpi": kpi_list,
            "values": value_list,
        }

        df = pd.DataFrame(data)

        #convert datatypes for insertion into pickle to increase runtime speed
        df['date'] = pd.to_datetime(df['date'])

        ##insert script to insert adj time with date mapping

        date_mapping = date_mapper()

        #merging quarterly dates with df
        df_total = pd.merge(df, date_mapping, how='left', left_on='date', right_on = 'date')

        df = df_total.groupby(['ticker', 'qtr_last_date', 'ann_last_date', 'date', 'cattype','kpi'])['values'].sum()
        df = df.reset_index()

        # structure the data such that qtrly cattype will have last date as the quarterly last and the annual cattype will have dec 31 of each year as last date
        # if quarterly data than the last date should be empty, if annual data than then the last date for quarter should be empty
        # this is to fix the display of qtr results showing up in annual report in streamlit charts
        df['adj_last_date'] = df.apply(lambda x: x['ann_last_date'] if x['cattype'][0:6] == "annual" else x['qtr_last_date'], axis=1)

        df = df[['ticker', 'adj_last_date', 'date', 'cattype','kpi', 'values']]


        df.to_pickle('data/eq_hist_details.pickle')

        upload_cloud_storage('eq_hist_details.pickle')
        print('upload successful')

        # df = pd.read_pickle('data/eq_hist_details.pickle')
        # print (df)
        # print (df.info(verbose=True)) 
        # print (df.dtypes)


    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on export_eq_hist_details"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)
