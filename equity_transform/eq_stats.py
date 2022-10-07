import smtplib
from email.message import EmailMessage
from settings import (project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, 
                    schedule_function_key, firebase_auth_api_key, email_password, cloud_storage_key)
from secret import access_secret
import pytz
from datetime import datetime
from google.oauth2 import service_account
from google.cloud.firestore import Client
import json
from firebase_admin import firestore
from googleapiclient.discovery import build
from time import process_time
from google.cloud import storage
import os
import pandas as pd
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage, date_mapper



firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

# #################################################################################################
# ####### Computing equity stats ####################################################
# #################################################################################################
# python -c 'from equity_transform.eq_stats import export_eq_dl_stats; export_eq_dl_stats()'

def export_eq_dl_stats():

    doc_list = ['equity_daily_kpi', 'equity_financials', 'equity_price_history']
    limit_num = 1000
    df_total = pd.DataFrame(columns = ['type', 'rounded_updated_datetime', 'count_fx_ticker',])

    for i in doc_list:
        collection = i
        docs = db.collection(collection).stream()
        # docs = db.collection(collection).limit(limit_num).stream()

        data = {}
        for i in docs:
            data_kpi = {}
            for j in ['updated_datetime', 'ticker']:
                try:
                    data_kpi[j] = i._data[j]
                except KeyError:
                    data_kpi[j] = 0

            data[i._data['ticker']] = data_kpi

        df1 = pd.DataFrame.from_dict(data)
        df1 = df1.transpose()
        df1['rounded_updated_datetime'] = df1['updated_datetime'].dt.round('60min')  
        df1 = df1.drop('updated_datetime', axis=1)
        df1 = df1.groupby(['rounded_updated_datetime']).count()#.sort_values('updated_datetime', ascending=False)
        df1 = df1.reset_index()

        df1.rename(columns = {'ticker':'count_fx_ticker'}, inplace = True)

        ##converting into google sheets acceptable format
        df1['rounded_updated_datetime'] = df1['rounded_updated_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df1['type'] = collection
        df_total = pd.concat([df_total, df1])


    docs = db.collection(u'fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(14).stream()

    data = {}
    for i in docs:
        test = i._data['datetime_format'].strftime('%Y-%m-%d %H:%M:%S')
        data[test] = len(i._data['currencyrates'])

    df2 = pd.DataFrame(list(data.items()), columns = ['rounded_updated_datetime','count_fx_ticker'])
    df2['type'] = 'fxhistorical'

    df_eq_dl_stats = pd.concat([df_total, df2])


    df_eq_dl_stats.to_pickle('data/eq_dl_stats.pickle')
    upload_cloud_storage('eq_dl_stats.pickle')
    print ('upload successful')
 