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




# ######################################################################################
# ####### Extracting whole ticker dataset to csv for analysis ##########################
# ######################################################################################


# # print the datetime to csv to analyse data if the code is working correctly
# obj = db.collection('tickerlist').get()
# print (len(obj))
# datalist = []
# for i in obj:

#     try:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],i._data['kpi']])
#     except KeyError:
#         datalist.append([i._data['updated_datetime'],i._data['tickername'],"does_not_exists"])

# df = pd.DataFrame(datalist)
# df.replace(np.nan, '', inplace=True)
# df.to_csv("testtime.csv")




######################################################################################
####### Migrating the testing ticker data to tickerdatatest ##########################
######################################################################################

############## Running the function from the command line ###############
# python -c 'from mgt_fb_crud import migrate_to_test; migrate_to_test()'

def migrate_to_test():

    number_entries = 10
    migrate_to = 'tickerlisttest'

    tickerlist = db.collection('tickerlist').limit(number_entries).get()

    for i in tickerlist:

        ticker = i._data['ticker']
        activated = i._data['activated']
        created_datetime = i._data['created_datetime']
        ebitdaUSD = i._data['ebitdaUSD']
        enterpriseValueUSD = i._data['enterpriseValueUSD']
        freeCashflowUSD = i._data['freeCashflowUSD']
        grossProfitsUSD = i._data['grossProfitsUSD']
        kpi = i._data['kpi']
        marketCapUSD = i._data['marketCapUSD']
        operatingCashflowUSD = i._data['operatingCashflowUSD']
        tickername = i._data['tickername']
        totalDebtUSD = i._data['totalDebtUSD']
        totalRevenueUSD = i._data['totalRevenueUSD']
        updated_datetime = i._data['updated_datetime']

        data = {

            'ticker': ticker,
            'activated': activated,
            "created_datetime": created_datetime,
            "ebitdaUSD": ebitdaUSD,
            "enterpriseValueUSD": enterpriseValueUSD,
            "freeCashflowUSD": freeCashflowUSD,
            "grossProfitsUSD": grossProfitsUSD,
            "kpi": kpi,
            "marketCapUSD": marketCapUSD,
            "operatingCashflowUSD": operatingCashflowUSD,
            "tickername": tickername,
            "totalDebtUSD": totalDebtUSD,
            "totalRevenueUSD": totalRevenueUSD,
            "updated_datetime": updated_datetime
        }

        db.collection(migrate_to).document(i.id).set(data, merge=True)



######################################################################################
####### Investigation ################################################################
######################################################################################

############## Running the function from the command line ###############
# python -c 'from mgt_fb_crud import ticker_investigation; ticker_investigation()'

ticker = 'DELHIVERY.NS'

def ticker_investigation():
    docs = db.collection('tickerlist').where("ticker", "==", ticker).get()


    print(docs[0]._data['kpi'])



########################################################################################
###########  Sample export dataframe to google sheets  #################################
########################################################################################
# python -c 'from ref_yfinance import sample_df_gs; sample_df_gs()'

def sample_df_gs():
    companyticker = yf.Ticker("meta")

    df = companyticker.financials

    dfcol = []
    for i in df.columns:
        i = i.strftime('%Y-%m-%d')
        dfcol.append(i)

    dfindex = []
    for i in df.index:
        dfindex.append([i])

    dflist = df.values.tolist()

    sheetinfo = "Sheet2"

    #Inject the values
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!B4", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()

    #Inject the index
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!A4", valueInputOption="USER_ENTERED", body={"values":dfindex}).execute()

    #Inject the fields
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!B3", valueInputOption="USER_ENTERED", body={"values":[dfcol]}).execute()







# #######################################################################################################
# ############### testing - you can delete if no longer used ############################################
# #######################################################################################################
# python -c 'from mgt_fb_crud import test; test()'


def test():

    tz_SG = pytz.timezone('Singapore')
    end_date = datetime.now(tz_SG)
    # extracting the last three days in case the last 3 days fx was updated
    start_date = end_date - timedelta(days=1)
    start_date_str = '2022-08-16'
    end_date_str = '2022-08-16'

    doc = db.collection('fx').stream()

    for i in doc:
        
        currency = i._data['currency']
        # IMPORTANT - yfinance extracts one day before instead of exact day - this is import for mgt_fin_exp_fb.py
        forex_data = yf.download('USD'+ currency +'=X', start = start_date_str, end = end_date_str)
        # test = forex_data.index = pd.to_datetime(forex_data.index)
        extracted_fx = forex_data['Adj Close']
        print (extracted_fx, currency)

    #     for idate, rate in extracted_fx.items():
    #         #convert into str formet that excludes date so the document can be searched
    #         idate = idate.replace(hour=8, minute=0)
    #         hist_datetime = datetime.combine(idate, datetime.min.time())
    #         hist_input_date = idate.strftime("%Y-%m-%d")
    #         print (i._data['currency'])
    #         currencyrates = {
    #             currency : rate,
    #             #usd does not exist in fx so it has to be added separately
    #             'USD': 1
    #         }
    #         data = {
    #             "currencyrates" : currencyrates,
    #             "datetime_format" : hist_datetime,
    #             "created_datetime": firestore.SERVER_TIMESTAMP,
    #             "updated_datetime" : firestore.SERVER_TIMESTAMP
    #                 }
    #         #inserting the data into fx historical
    #         db.collection(u'fxhistorical').document(hist_input_date).set(data, merge=True)




















######################################################################################
####### Pivoting the data into formats that are interesting ##########################
######################################################################################

############## Running the function from the command line ###############
# python -c 'from mgt_fb_crud import pivot_data; pivot_data()'


#DESCENDING

def pivot_data():
    docs = db.collection('tickerlisttest').order_by("updated_datetime", direction=firestore.Query.DESCENDING).get()
    datalist = []
    for i in docs:
        print (i._data['ticker'])

        data = {}
        data["ticker"] = i._data['ticker']

        try:
            data["industry"] = i._data['kpi']['industry']
        except:
            data["industry"] = ""

        try:
            data["sector"] = i._data['kpi']['sector']
        except:
            data["sector"] = ""

        try:
            data["country"] = i._data['kpi']['country']
        except:
            data["country"] = ""

        try:
            data["city"] = i._data['kpi']['city']
        except:
            data["city"] = ""

        try:
            data["marketCapUSD"] = i._data['marketCapUSD']
        except:
            data["marketCapUSD"] = 0

        try:
            data["enterpriseValueUSD"] = i._data['enterpriseValueUSD']
        except:
            data["enterpriseValueUSD"] = 0

        try:
            data["freeCashflowUSD"] = i._data['freeCashflowUSD']
        except:
            data["freeCashflowUSD"] = 0

        try:
            data["operatingCashflowUSD"] = i._data['operatingCashflowUSD']
        except:
            data["operatingCashflowUSD"] = 0

        try:
            data["currentPriceUSD"] = i._data['currentPriceUSD']
        except:
            data["currentPriceUSD"] = 0

        try:
            data["totalRevenueUSD"] = i._data['totalRevenueUSD']
        except:
            data["totalRevenueUSD"] = 0

        try:
            data["grossProfitsUSD"] = i._data['grossProfitsUSD']
        except:
            data["grossProfitsUSD"] = 0

        try:
            data["ebitdaUSD"] = i._data['ebitdaUSD']
        except:
            data["ebitdaUSD"] = 0



        datalist.append(data)

    df = pd.DataFrame(datalist)
    df.replace(np.nan, '', inplace=True)

    # ## Selected KPIs including usd values 
    # kpilistselect2 = [
    # 'industry','sector','ticker'
    # ]

    # df = df[kpilistselect2]
    # df = df.groupby(['industry','sector']).count() #.sort_values('updated_datetime', ascending=False)
    # df = df.reset_index()
    # print (df)


    kpilistselect2 = [
    'country','city', 'industry','sector','ticker', 'marketCapUSD'
    ]
    df = df[kpilistselect2].head(50)

 

    # print (df)
    # companyinfo = df[df['ticker'] == "MDV"]
    # companymktcap = companyinfo['marketCap'].values
    # print (companymktcap)
    # print (companymktcap[0])
    # print (type(companymktcap[0]))

    # no_companies_df = df.groupby(['industry','sector'])['ticker'].count() #.sort_values('updated_datetime', ascending=False)
    # no_companies_df = no_companies_df.reset_index()
    # print (no_companies_df)

    # sum_mkt_cap_df = df.groupby(['industry','sector'])['marketCap'].sum() #.sort_values('updated_datetime', ascending=False)
    # sum_mkt_cap_df = sum_mkt_cap_df.reset_index()
    # print (sum_mkt_cap_df)

    sum_country_mkt_cap_df = df.groupby(['country','city'])['marketCapUSD'].sum() #.sort_values('updated_datetime', ascending=False)
    sum_country_mkt_cap_df = sum_country_mkt_cap_df.reset_index()
    print (sum_country_mkt_cap_df)





