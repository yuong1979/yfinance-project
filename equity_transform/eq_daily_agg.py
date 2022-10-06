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
######## Description: Extracting from firebase the daily ratio kpis and ranking / aggregating(median) it and then loading pickle into gcp
######## This itself has no prerequisite - BUT FILE IS EXTREMELY HUGE
####################################################################################################################################



# #################################################################################################
# ####### Computing ranking and median all equities ###############################################
# #################################################################################################
# python -c 'from equity_transform.eq_daily_agg import export_eq_daily_agg; export_eq_daily_agg()'



def export_eq_daily_agg():


    try:

        df = pd.read_pickle('data/eq_daily_kpi.pickle')


        ######## data cleaning #########
        # #originally industry has zeros in them, replacing it with '' which is a string that matches the rest of the data in the column
        df.replace("", np.nan, inplace=True)
        # because there is data into trailingPD and pricetosalestrailing12months that are named 'infinity
        df.replace("Infinity", np.nan, inplace=True)
        #industry has data labeled 0 and float-nan, changing all of them to string "" in the end
        df['industry'].replace(0, "", inplace=True)
        df['industry'].replace(np.nan, "", inplace=True)


        ind_unq = df['industry'].unique().tolist()

        cln_ind_unq = [x for x in ind_unq if x != 'nan']

        # # #temporary industry list for testing
        # cln_ind_unq = [
        #     'Steel', 'Airlines', 'Gold', 'Leisure', 'Asset Management', 'Biotechnology', 'Credit Services',
        #     'Capital Markets', 'Chemicals', 'Coking Coal', 'Communication Equipment', 'Computer Hardware', 
        #     'Confectioners', 'Conglomerates', 'Consulting Services', 'Consumer Electronics', 'Copper'
        #     ]

        #removing company_count form the dictionary because it is not part of the kpi to be ranked
        del kpi_mapping['company_count']
        del kpi_mapping['fullTimeEmployees']

        ind_count = len (cln_ind_unq)

        eq_daily_agg = pd.DataFrame(columns = ['industry', 'kpi', 'value', 'value_alpha','rank_fraction', 'rank_%', 'median', 'sum', 'max', 'min'])

        count = 0
        for i in cln_ind_unq:
            newdf = df[df['industry'] == i]

            for key, value in kpi_mapping.items():
                # print(key, value)
                value_df = newdf[[key]]
                 # this method of count seems to exclude the number of nan in the dataset
                count_eq = value_df[key].count()
                #copy is added here to avoid slice writing over slice warnings
                finaldf = value_df.dropna(subset=[key]).copy()

                finaldf['count_eq'] = count_eq
                finaldf['kpi'] = key
                finaldf['industry'] = i
                finaldf['value'] = value_df

                #converting numerals to alphabets for large numbers
                sum_list = ['marketCapUSD','totalRevenueUSD','ebitdaUSD'] 
                if key in sum_list:
                    finaldf['value_alpha'] = finaldf['value'].apply(convert_digits) 

                try:
                    if value == True:
                        finaldf['rank'] = finaldf[key].rank(ascending=False)
                        finaldf['max'] = finaldf['value'].max()
                        finaldf['min'] = finaldf['value'].min()               
                    else:
                        finaldf['rank'] = finaldf[key].rank(ascending=True)
                        finaldf['max'] = finaldf['value'].min()
                        finaldf['min'] = finaldf['value'].max()

                    finaldf['rank_fraction'] = finaldf["rank"].astype(int).astype(str) + "/" + str(count_eq)
                    finaldf['rank_%'] = 1 - (finaldf['rank'] / finaldf["count_eq"])
                    finaldf['rank_%'] = finaldf['rank_%'].astype(float).round(decimals = 2)
                    finaldf['median'] = finaldf['value'].median()
                    finaldf['sum'] = finaldf['value'].sum()
                    
                except:
                    finaldf['rank_fraction'] = ""
                    finaldf['rank'] = np.nan
                    finaldf['rank_%'] = np.nan
                    finaldf['median'] = np.nan
                    finaldf['sum'] = np.nan
                    finaldf['max'] = np.nan
                    finaldf['min'] = np.nan


                #add a function to change all those with 1/1 or 1 in rank_% to nan

                finaldf.drop(columns=[key, 'rank', 'count_eq'], inplace=True)
                eq_daily_agg = pd.concat([eq_daily_agg, finaldf])

            count = count + 1
            print (str(count) + "/" + str(ind_count))

        eq_daily_agg.to_pickle('data/eq_daily_agg.pickle')

        upload_cloud_storage('eq_daily_agg.pickle')
        print('upload sucessful')

        # df = pd.read_pickle('data/eq_daily_agg.pickle')
        # print (df)
        # print (df.info(verbose=True)) 
        # print (df.dtypes)




    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on export_eq_daily_agg"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)

