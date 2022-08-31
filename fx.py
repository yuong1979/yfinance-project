from termios import INLCR
import requests
import time
from datetime import datetime, timedelta, date
import pytz
import pandas as pd
import numpy as np
from secret import access_secret
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud.firestore import Client
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
import requests
import yfinance as yf
from firebase_admin import firestore
from tools import error_email
import inspect
from bs4 import BeautifulSoup
import re

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()


fx_api_key = access_secret(fx_api_key, project_id)
firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

# upload fx to firebase
fx_list = [
    "CNY","INR","CAD","USD","ARS","EUR","KRW","HKD","AUD",
    "MXN","CZK","GBP","TWD","CHF","CLP","BRL","JPY","SGD",
    "NOK","ILS","ZAR","SEK","RUB","NZD","IDR","PLN","SAR",
    "MYR","TRY","THB","PEN","DKK","NGN","HUF","COP","PHP",
    "GBP","ILA","ISK"
]



###############################################################
################## not required anymore delete soon? ##########
###############################################################
# # python -c 'from fx import get_rates; get_rates()'
# def get_rates():
#     tz_SG = pytz.timezone('Singapore')
#     date = datetime.now(tz_SG)
#     currency_required = 'INR'
#     # fx_date_str = date.strftime("%Y-%m-%d")
#     fx_date_str = '2022-08-17'
#     docs = db.collection('fxhistorical').document(fx_date_str).get()
#     rate = docs._data["currencyrates"][currency_required]
#     print (rate)


# #######################################################################################################
# ############### Insert dates for FX historials ########################################################
# #######################################################################################################
# python -c 'from fx import insert_dates_fx; insert_dates_fx()'

####### insert dates into firebase for FX ########
# start_date = date(2000, 1, 1)
# end_date = date(2040, 12, 31)

def insert_dates_fx():
    start_date = date(2000, 1, 1)
    end_date = date(2022, 8, 17)
    delta = end_date - start_date
    for i in range(delta.days + 1):  
        day = start_date + timedelta(days=i)
        hist_datetime = datetime.combine(day, datetime.min.time())
        hist_input_date = day.strftime("%Y-%m-%d")
        print (hist_input_date)

        data = {
            'datetime_format' : hist_datetime,
            'updated_datetime': firestore.SERVER_TIMESTAMP,
            'created_datetime': firestore.SERVER_TIMESTAMP,
            }
        db.collection(u'fxhistorical').document(hist_input_date).set(data)


# #######################################################################################################
# ############### Delete all FX historials ##############################################################
# #######################################################################################################
# python -c 'from fx import delete_fx_history; delete_fx_history()'

# delete all documents in a collection
def delete_fx_history():
    collection = "fxhistorical"
    docs = db.collection(collection).get()
    for doc in docs:
        key = doc.id
        db.collection(collection).document(key).delete()



# #######################################################################################################
# ############### Printing historical data to google sheets #############################################
# #######################################################################################################

# python -c 'from fx import extract_fxfb_gs; extract_fxfb_gs()'

def extract_fxfb_gs():
    # doc = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(10).stream()    
    doc = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).stream()
    datalist = []
    for i in doc:
        datetime_format = i._data['datetime_format']
        datetime = i.id
        print (datetime)
        # if there are no currencyrates in the document
        try:
            currencyrate = str(i._data['currencyrates'])
        except:
            currencyrate = ""
        datalist.append([ datetime_format, datetime, currencyrate ])

    df = pd.DataFrame(datalist)
    df.set_index(0, inplace = True)
    df.rename(columns={1: 'dates', 2: 'currencies'}, inplace=True)

    print (df)
    dflist = df.values.tolist()
    sheetinfo = "Sheet1"
    #Inject the values into cols of the spreadsheet
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!E2", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()



# #######################################################################################################
# ############### Update history of the historicals FX ##################################################
# #######################################################################################################
# python -c 'from fx import extract_hist_fx_fb; extract_hist_fx_fb()'

# INR, CLP, SGD, IDR, MYR, NZD, CZK, MXN, BRL, PLN, RUB, PHP, NOK, GBP, HKD, TRY, 
# EUR, JPY, SEK, COP, NGN, THB, CHF, DKK, PEN, SAR, CAD, 
# ZAR, TWD, HUF, KRW, AUD, ISK, CNY, USD, ILS, ARS

def extract_hist_fx_fb():
    #go to firebase and choose those fx you want to update as activated = true
    doc = db.collection('fx').where('activated', '==', True).stream()

    for i in doc:
        
        currency = i._data['currency']
        forex_data = yf.download('USD'+ currency +'=X', start='2000-01-01', end='2022-08-20')
        # test = forex_data.index = pd.to_datetime(forex_data.index)
        extracted_fx = forex_data['Adj Close']

        for idate, rate in extracted_fx.items():
            #convert into str formet that excludes date so the document can be searched
            time_str_format = idate.strftime("%Y-%m-%d")
            print (time_str_format)
            print (i._data['currency'])
            currencyrates = {
                currency : rate
            }
            data = {
                "currencyrates" : currencyrates,
                "updated_datetime" : firestore.SERVER_TIMESTAMP
                    }
            #inserting the data into fx historical
            db.collection(u'fxhistorical').document(time_str_format).set(data, merge=True)





# #######################################################################################################
# ############### Daily updates of the historical FX ####################################################
# #######################################################################################################
# python -c 'from fx import ext_daily_fx_yf_fb; ext_daily_fx_yf_fb()'


def ext_daily_fx_yf_fb():

    try:

        tz_SG = pytz.timezone('Singapore')
        end_date = datetime.now(tz_SG)
        # extracting the last three days in case the last 3 days fx was updated
        start_date = end_date - timedelta(days=15)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        print (start_date_str)
        print (end_date_str)

        doc = db.collection('fx').stream()

        for i in doc:
            
            currency = i._data['currency']
            # IMPORTANT - yfinance extracts one day before instead of exact day - this is import for mgt_fin_exp_fb.py
            forex_data = yf.download('USD'+ currency +'=X', start = start_date_str, end = end_date_str)
            # test = forex_data.index = pd.to_datetime(forex_data.index)
            extracted_fx = forex_data['Adj Close']
            print (extracted_fx)

            for idate, rate in extracted_fx.items():
                #convert into str formet that excludes date so the document can be searched
                idate = idate.replace(hour=8, minute=0)
                hist_datetime = datetime.combine(idate, datetime.min.time())
                hist_input_date = idate.strftime("%Y-%m-%d")
                print (i._data['currency'])
                currencyrates = {
                    currency : rate,
                    #usd does not exist in fx so it has to be added separately
                    'USD': 1
                }
                data = {
                    "currencyrates" : currencyrates,
                    "datetime_format" : hist_datetime,
                    "created_datetime": firestore.SERVER_TIMESTAMP,
                    "updated_datetime" : firestore.SERVER_TIMESTAMP
                        }
                #inserting the data into fx historical
                db.collection(u'fxhistorical').document(hist_input_date).set(data, merge=True)


    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)


# #######################################################################################################
# ############### View data in historical FX ####################################################
# #######################################################################################################
# python -c 'from fx import view_daily_fx_fb; view_daily_fx_fb()'

def view_daily_fx_fb():
    end_date_str = '2022-08-17'
    test = db.collection(u'fxhistorical').document(end_date_str).get()
    print (test.id)
    print (test._data)




# #######################################################################################################
# ############### Inserting FX into fx daily manually from xe.com instead of yfinance ###################
# #######################################################################################################
# python -c 'from fx import manual_inserting_fx; manual_inserting_fx()'

def manual_inserting_fx():
    
    ### inserting the data from xe.com instead of yfinance

    #input formet -> year, month, date
    idate = datetime(2022, 8, 28)

    idate = idate.replace(hour=8, minute=0)
    hist_datetime = datetime.combine(idate, datetime.min.time())
    hist_input_date = idate.strftime("%Y-%m-%d")

    # print (hist_datetime)
    # print (hist_input_date)

    docs = db.collection(u'fx').stream()

    currencyrates = {}
    for i in docs:
        # print (i._data['currency'])
        currency = i._data['currency']

        target_site = "https://www.xe.com/currencyconverter/convert/?Amount=1&From=USD&To="+currency
        source = requests.get(target_site).text
        soup = BeautifulSoup(source, 'lxml')
        regex = re.compile('.*result__BigRate*')
        target = soup.find(class_=regex).get_text().split()[0].replace(',',"")
        currencyrates[currency] = target
        print(currency, target)

        time.sleep(1)

    data = {
        "currencyrates" : currencyrates,
        "datetime_format" : hist_datetime,
        "created_datetime": firestore.SERVER_TIMESTAMP,
        "updated_datetime" : firestore.SERVER_TIMESTAMP
            }
    print (data)

    db.collection(u'fxhistorical').document(hist_input_date).set(data, merge=True)





# def hardcode_manual_inserting_fx():


#     #input formet -> year, month, date
#     idate = datetime(2022, 8, 27)

#     idate = idate.replace(hour=8, minute=0)
#     hist_datetime = datetime.combine(idate, datetime.min.time())
#     hist_input_date = idate.strftime("%Y-%m-%d")

#     print (hist_datetime)
#     print (hist_input_date)

#     currencyrates = {
#         # currency : rate,
#         'INR': 79.754312,
#         'CLP': 882.53974,
#         'SGD': 1.39612,
#         'IDR': 14853.634,
#         'MYR': 4.4840377,
#         'NZD': 1.6258903,
#         'CZK': 24.59227,
#         'MXN': 19.98203,
#         'BRL': 5.0290423,
#         'PLN': 4.7349729,
#         'RUB': 61.586436,
#         'PHP': 56.225248,
#         'NOK': 9.7452844,
#         'GBP': 0.85443127,
#         'HKD': 7.8481615,
#         'TRY': 18.187447,
#         'EUR': 0.9999511,
#         'JPY': 138.51742,
#         'SEK': 10.669302,
#         'COP': 4366.5648,
#         'NGN': 422.20178,
#         'THB': 36.403456,
#         'CHF': 0.96719384,
#         'DKK': 7.4372877,
#         'PEN': 3.8323979,
#         'SAR': 3.75,
#         'CAD': 1.300628,
#         'ZAR': 16.837043,
#         'TWD': 30.449339,
#         'HUF': 408.42517,
#         'KRW': 1347.5911,
#         'AUD': 1.4485133,
#         'ISK': 141.87397,
#         'CNY': 6.9168799,
#         'ILS': 3.3161405,
#         'ARS': 138.30844,
#         #usd does not exist in fx so it has to be added separately
#         'USD': 1
#     }
#     data = {
#         "currencyrates" : currencyrates,
#         "datetime_format" : hist_datetime,
#         "created_datetime": firestore.SERVER_TIMESTAMP,
#         "updated_datetime" : firestore.SERVER_TIMESTAMP
#             }
#     print (data)

#     # #inserting the data into fx historical
#     # db.collection(u'fxhistorical').document(hist_input_date).set(data, merge=True)




########################################################################################################
########################### Adding to the currency list ################################################
########################################################################################################


# #adding any additional new currency in the above list(just add to the list any new ones) and run the below
# for i in fx_list:
#     if not db.collection('fx').where("currency", "==", i).get():
#         data={'currency': i, 'rate': 0, 'activated': True}
#         db.collection('fx').add(data)



