from cmath import nan
from opcode import stack_effect
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
from export_gs_function import export_gs_func




firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()


# ##################################################################################################
# ######### Updating tickerlist from companiesmarketcap.com into firebase ##########################
# ##################################################################################################

# #updating the tickerlist by checking if all the tickers in companiesmarketcap is updated.
# #if not it will copy the ticker to firebase collection - tickerlist
# tz_SG = pytz.timezone('Singapore')
# datetime_SG = datetime.now(tz_SG)
# maxpages = 70
# for i in range(1,maxpages):
#     time.sleep(0.5)
#     r = requests.get('https://companiesmarketcap.com/page/' + str(i))
#     soup = BeautifulSoup(r.text, 'html.parser')
#     results1 = soup.find_all(attrs={"class":'company-code'})
#     results2 = soup.find_all(attrs={"class":'company-name'})
#     #market cap
#     results3 = soup.find_all(attrs={"class":'td-right'})
#     print ("page number " + str(i) )

#     if len(results1) > 0:
#         #k count is to get the company name in results2 which is in a different dataset than results1
#         k = 0
#         for j in results1:
#             #get ticker
#             ticker = j.contents[1]
#             #check if ticker is inside firebase
#             if not db.collection('tickerlisttest').where("ticker", "==", ticker).get():
#                 #get name
#                 tickername = str(results2[k].contents[0]).strip()

#                 #loading data into firebase
#                 data={
#                     'ticker': ticker, 
#                     'tickername': tickername,
#                     'created_datetime': datetime_SG,
#                     'updated_datetime': datetime_SG,
#                     'marketcap': 0,
#                     'activated': True
#                     }
#                 db.collection('tickerlisttest').add(data)
#                 print (ticker + " uploaded")
#             else:
#                 print (ticker + " passed")
#             k = k + 1







# #########################################################################
# ####### reading data ####################################################
# #########################################################################

# # select based on a specific criteria like sector and marketcap
# market_cap = 1000_000_000
# docs = db.collection('tickerlisttest').where('marketCap', '>=', market_cap).stream()
# for i in docs:
#     print (i._data['ticker'])

# sector = "Technology"
# docs = db.collection('tickerlisttest').where('sector', '>=', sector).stream()
# for i in docs:
#     print (i._data['ticker'])

# # Reading data into another destination
# tickerlisttest = db.collection('tickerlisttest').get()
# for i in tickerlisttest:
#     obj = db.collection('tickerlisttest').document(i.id).get()

#     print(obj._data['ticker'])
#     print(obj._data['tickername'])
#     print(obj._data['kpi']['sector'])
#     print(obj._data['updated_datetime'])

# # Reading one single ticker
# docs = db.collection("tickerlist").where("ticker", "==", "NENT-B.ST").get()
# print(docs[0]._data['kpi']['currency'])


# # if required to do a sort
# tickerlist = db.collection('tickerlist').where('updated_datetime', '<=', target_datetime).order_by("updated_datetime", direction=firestore.Query.ASCENDING).get()



# ################################################################
# ####### deleting documents and fields  #########################
# ################################################################


# # delete all documents in a collection
# collection = "tickerlisttest"
# docs = db.collection(collection).get()
# for doc in docs:
#     key = doc.id
#     db.collection(collection).document(key).delete()

# # delete kpi fields from a collection
# collection = "tickerlisttest"
# fieldtodelete = "longBusinessSummary"
# docs = db.collection(collection).get()
# for doc in docs:
#     key = doc.id
#     db.collection(collection).document(key).update({
#     fieldtodelete: firestore.DELETE_FIELD
# })

# # delete certain documents in a collection
# collection = "kpilist"
# docs = db.collection(collection).where("rate", "==", 0).get()
# for doc in docs:
#     key = doc.id
#     print (doc.id)
#     db.collection(collection).document(key).delete()






######################################################################################
####### Migrating the real datasets to testing datasets ##########################
######################################################################################
# python -c 'from mgt_fb_crud import migrate_to_test; migrate_to_test()'

# equity_daily_kpi_test

#ASCENDING or #DESCENDING
def migrate_to_test():
    number_entries = 5
    migrate_from = 'equity_list'
    migrate_to = 'equity_calc'

    tickerlist = db.collection(migrate_from).order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(number_entries).stream()

    x=1
    for tick in tickerlist:
        data_dict = {}
        for j in tick._data:
            data_dict[j] = tick._data[j]
        db.collection(migrate_to + str("_test")).document(tick.id).set(data_dict, merge=True)
        print (str(x) + "/" + str(number_entries) + " done")
        x = x + 1

#############################################################################################################################
####### Deleting all the data to tickerdatatest (EXTREMELY DANGEROUS CODE) DOUBLE CHECK B4 RUNNING ##########################
#############################################################################################################################
# python -c 'from mgt_fb_crud import delete_all_fields; delete_all_fields()'

def delete_all_fields():
    collection = "tickerlistpricetest"
    docs = db.collection(collection).get()
    for doc in docs:
        key = doc.id
        for i in doc.to_dict():
            todelete = i
            db.collection(collection).document(key).update({
            todelete : firestore.DELETE_FIELD
        })







######################################################################################
####### Investigation ################################################################
######################################################################################

############## Running the function from the command line ###############
# python -c 'from mgt_fb_crud import ticker_investigation; ticker_investigation()'

required_ticker = 'AAPL'
def ticker_investigation():
    docs = db.collection('equity_calc').where("ticker", "==", required_ticker).get()
    # print(docs[0]._data['kpi'])
    print(docs[0]._data)



######################################################################################
####### Count number of entries ######################################################
######################################################################################

############## Running the function from the command line ###############
# python -c 'from mgt_fb_crud import counter_rows; counter_rows()'
def counter_rows():
    docs = db.collection('equity_list').get()
    print(len(docs))



########################################################################################
###########  Sample export dataframe to google sheets  #################################
########################################################################################
# python -c 'from mgt_fb_crud import sample_df_gs; sample_df_gs()'

def sample_df_gs():
    name = "pnl quarterly"
    sheetinfo = "Sheet2"
    companyticker = yf.Ticker("meta")
    df = companyticker.financials
    export_gs_func(name, df, sheetinfo)


########################################################################################
###########  Delete unwanted record datetime from the dataset  #########################
########################################################################################
# python -c 'from mgt_fb_crud import test_delete; test_delete()'

# change the fieldtodelete ""
def test_delete():
    # delete kpi fields from a collection
    collection = "tickerlisttest"
    fieldtodelete = ""
    docs = db.collection(collection).get()
    for doc in docs:
        key = doc.id
        db.collection(collection).document(key).update({
        fieldtodelete: firestore.DELETE_FIELD
    })



########################################################################################
###########  Extracting the time series financials to gs  ###############################
########################################################################################
# python -c 'from mgt_fin_exp_fb import financials_to_gs; financials_to_gs()'

ticker = 'WY'
def financials_to_gs():
    docs = db.collection('tickerlisttest').where("ticker", "==", ticker).get()[0]
    time_series_financials = docs._data['time_series_financials']
    cashflow = time_series_financials['cashflow']
    df = pd.DataFrame(cashflow)
    name = "cashflow"
    sheetinfo = "Sheet2"
    export_gs_func(name, df, sheetinfo)










# export_gs_func(name, df ,sheetinfo)

    # ## Selected KPIs including usd values 
    # kpilistselect2 = [
    # 'updated_datetime', 'ticker','tickername',
    # 'shortName', 'longBusinessSummary','symbol', 'sector', 'industry', 'country', 'marketCap',  
    # 'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare',
    # 'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
    # 'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth', 'priceToSalesTrailing12Months', 
    # 'trailingEps', 'forwardEps', 
    # 'pegRatio', 'trailingPegRatio',
    # 'currentRatio', 'quickRatio', 'debtToEquity', 
    # 'bookValue', 'enterpriseValue', 'priceToBook', 
    # 'freeCashflow', 'operatingCashflow', 'dividendYield', 'dividendRate', 
    # 'totalRevenue', 'grossProfits', 'ebitda', 'totalDebt', 'beta',
    # 'currency', 'financialCurrency',
    # 'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated',
    # 'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'lastDividendValue', 'lastDividendDate',
    # 'targetMedianPrice',  'targetMeanPrice', 'currentPrice',
    # 'marketCapUSD', 'enterpriseValueUSD', 'freeCashflowUSD', 'operatingCashflowUSD', 'totalDebtUSD',
    # 'currentPriceUSD', 'totalRevenueUSD', 'grossProfitsUSD', 'ebitdaUSD'
    # ]




 


#things to do
# add the country median sum aggregator by changing the industry one
# add email notifications to the industry median sum aggregator
# add email notifications to the country median sum aggregator
# change email notifications to include the ticker when facing an error - to the daily kpi equity extraction
# set up the aggregators so it runs once daily
# dump the industry median and sum kpi into the companies associated with the industry into equities kpi add
# add dates to all the new functions and tables



## process
# - daily runs capture all daily kpi data
# - next run to do an aggregation by country, sector and industry of the KPIs involved
# - next run to extract filter sector, industry country on the daily kpi data rank them by KPIs 


## Steps in building this process
# - adjust the daily run to insert the country, sector and industry into daily run kpi collection and push that live and observe if working fine
# - create the collection for country, sector and industry
# - create the function that updates the aggregation collection for country, sector and industry




