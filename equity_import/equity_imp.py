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




##################################################################################################
######### Updating equity_list from companiesmarketcap.com into firebase ##########################
##################################################################################################
# python -c 'from equity_imp import update_equity_list; update_equity_list()'

## After running this script to run the script below to update more equity into the lists

def update_equity_list():
    tz_SG = pytz.timezone('Singapore')
    datetime_SG = datetime.now(tz_SG)
    maxpages = 70
    for i in range(1,maxpages):
        time.sleep(0.5)
        r = requests.get('https://companiesmarketcap.com/page/' + str(i))
        soup = BeautifulSoup(r.text, 'html.parser')
        results1 = soup.find_all(attrs={"class":'company-code'})
        results2 = soup.find_all(attrs={"class":'company-name'})
        #market cap
        results3 = soup.find_all(attrs={"class":'td-right'})
        print ("page number " + str(i) )

        if len(results1) > 0:
            #k count is to get the company name in results2 which is in a different dataset than results1
            k = 0
            for j in results1:
                #get ticker
                ticker = j.contents[1]
                #check if ticker is inside firebase
                if not db.collection('equity_list').where("ticker", "==", ticker).get():
                    #get name
                    tickername = str(results2[k].contents[0]).strip()

                    #loading data into firebase
                    data={
                        'ticker': ticker, 
                        'tickername': tickername,
                        'created_datetime': datetime_SG,
                        'updated_datetime': datetime_SG,
                        'activated': True
                        }
                    db.collection('equity_list').add(data)
                    print (ticker + " uploaded")
                else:
                    print (ticker + " passed")
                k = k + 1


######################################################################################################################################
####### Migrating ticker data in equity_list to equity_price_history, equity_financials and equity_daily_kpi #########################
######################################################################################################################################
# python -c 'from equity_imp import migrate_equity_lists_to_datasets; migrate_equity_lists_to_datasets()'

def migrate_equity_lists_to_datasets():
    migrate_from = 'equity_list'
    migrate_to = ['equity_daily_kpi','equity_financials', 'equity_price_history']
    for stat in migrate_to:

        equity_list = db.collection(migrate_from).get()
        for tick in equity_list:
            tz_SG = pytz.timezone('Singapore')
            datetime_SG = datetime.now(tz_SG)
            data_dict = {
                'ticker': tick._data['ticker'],
                'created_datetime': datetime_SG,
                'updated_datetime': datetime_SG,
            }
            print ("ticker extracted is :", tick._data['ticker'], "dataset is :", stat)
            db.collection(stat).document(tick.id).set(data_dict, merge=True)



# ######################################################################################################################################
# ####### to migrate list of equities and framework to new lists - REMOVE MIGRATE_TO when dones to prevent accidental overwrites #######
# ######################################################################################################################################
# # python -c 'from equity_imp import migrate_equity_lists_to_equity_calc; migrate_equity_lists_to_equity_calc()'

# def migrate_equity_lists_to_equity_calc():
#     migrate_from = 'equity_list'
#     migrate_to = 'equity_calc'

#     equity_list = db.collection(migrate_from).get()
#     for tick in equity_list:
#         tz_SG = pytz.timezone('Singapore')
#         datetime_SG = datetime.now(tz_SG)
#         data_dict = {
#             'ticker': tick._data['ticker'],
#             'created_datetime': datetime_SG,
#             'updated_datetime': datetime_SG,
#         }
#         print ("ticker extracted is :", tick._data['ticker'])
#         db.collection(migrate_to).document(tick.id).set(data_dict, merge=True)




