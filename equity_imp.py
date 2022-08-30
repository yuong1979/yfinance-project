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
from email_function import error_email
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



######################################################################################################################################
####### to migrate list of equities and framework to new lists - REMOVE MIGRATE_TO when dones to prevent accidental overwrites #######
######################################################################################################################################
# python -c 'from equity_imp import migrate_equity_lists_to_equity_calc; migrate_equity_lists_to_equity_calc()'

def migrate_equity_lists_to_equity_calc():
    migrate_from = 'equity_list'
    migrate_to = 'equity_calc'

    equity_list = db.collection(migrate_from).get()
    for tick in equity_list:
        tz_SG = pytz.timezone('Singapore')
        datetime_SG = datetime.now(tz_SG)
        data_dict = {
            'ticker': tick._data['ticker'],
            'created_datetime': datetime_SG,
            'updated_datetime': datetime_SG,
        }
        print ("ticker extracted is :", tick._data['ticker'])
        db.collection(migrate_to).document(tick.id).set(data_dict, merge=True)





# #################################################################################################
# ####### Extracting data from yfinance into firebase and include usd values ######################
# #################################################################################################
# python -c 'from equity_imp import imp_equity_daily_kpi_fb; imp_equity_daily_kpi_fb()'

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
    sleeptime = 5
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
                    kpi_in_USD = i._data['kpi'][j] * rate
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






########################################################################################
###########  Export detailed annual and quarterly financials to fb  ####################
########################################################################################
# python -c 'from equity_imp import imp_equity_financials_fb; imp_equity_financials_fb()'


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



##########################################################################################################
###########  export price history of equity into fb ######################################################
##########################################################################################################
# python -c 'from equity_imp import imp_equity_price_history_fb; imp_equity_price_history_fb()'


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