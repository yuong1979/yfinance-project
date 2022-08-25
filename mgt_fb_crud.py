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
####### Migrating the testing ticker data to tickerdatatest ##########################
######################################################################################
# python -c 'from mgt_fb_crud import migrate_to_test; migrate_to_test()'

# equity_daily_kpi_test

#ASCENDING or #DESCENDING
def migrate_to_test():
    number_entries = 500
    migrate_to = 'equity_daily_kpi_test'
    migrate_from = 'equity_daily_kpi'

    tickerlist = db.collection(migrate_from).order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(number_entries).stream()

    x=1
    for tick in tickerlist:
        data_dict = {}
        for j in tick._data:
            data_dict[j] = tick._data[j]
        db.collection(migrate_to).document(tick.id).set(data_dict, merge=True)
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
    docs = db.collection('equity_daily_kpi').where("ticker", "==", required_ticker).get()
    # print(docs[0]._data['kpi'])
    print(docs[0]._data)



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





######################################################################################
####### Updating industry aggregates #################################################
######################################################################################
# python -c 'from mgt_fb_crud import update_industry_aggregates; update_industry_aggregates()'

#DESCENDING
agg_collection = 'equity_daily_kpi'
def update_industry_aggregates():   
    docs = db.collection(agg_collection).stream()
    #Kpis that are inside the kpi field
    kpi1 = [
        'industry','sector','country', 
        'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
        'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
        'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months', 
        'pegRatio', 'trailingPegRatio',
        'currentRatio', 'quickRatio', 'debtToEquity',
        'dividendRate',
        'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated', 'fullTimeEmployees'
        ]
    #Kpis that are inside the main field
    kpi1USD = ['marketCapUSD', 'totalRevenueUSD', 'totalRevenueUSD', 'ebitdaUSD']

    main_dic = {}
    for doc in docs:
        kpi_dic = {}
        for j in kpi1:
            try:
                kpi_dic[j] = doc._data['kpi'][j]
            except:
                kpi_dic[j] = ""

        for j in kpi1USD:
            try:
                kpi_dic[j] = doc._data[j]
            except:
                kpi_dic[j] = ""

        main_dic[doc._data['ticker']] = kpi_dic

    df = pd.DataFrame(main_dic)
    df = df.transpose()

    # select the category
    category='industry'

    df_unique = getattr(df , category).unique()
    df_unique = pd.DataFrame(df_unique)
    df_unique = df_unique.rename(columns={0:category})

    # Calculating Sum for selected categories
    values = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD', 'fullTimeEmployees']

    for i in values:

        df_data = pd.DataFrame(df, columns = [category, i])
        df_data = df_data[(df_data[category] != "") & (df_data[i] != "") & (df_data[i] != 0)]
        df_data = df_data.groupby([category])[i].sum()
        df_data = df_data.reset_index(name = i)
        try:
            df_merged_sum = pd.merge(df_merged_sum, df_data, how='left', left_on = category, right_on = category)
        except:
            df_merged_sum = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

    # Calculating Median for selected categories
    values = ['grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
            'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
            'currentRatio', 'quickRatio', 'debtToEquity',
            'heldPercentInsiders', 'heldPercentInstitutions']

    for j in values:

        df_data = pd.DataFrame(df, columns = [category, j])


        df_data = df_data[(df_data[category] != "") & (df_data[j] != "") & (df_data[j] != 0)]

        df_data = df_data.groupby([category])[j].median()
        df_data = df_data.reset_index(name = j)
        try:
            df_merged_median = pd.merge(df_merged_median, df_data, how='left', left_on = category, right_on = category)
        except:
            df_merged_median = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

    # print (df_merged_median[['industry','currentRatio']].sort_values('industry', ascending=True).head(50))

    # Merging all the data into one dataframe
    df_merged_all = pd.merge(df_merged_sum, df_merged_median, how='left', left_on = category, right_on = category)

    df = df_merged_all.set_index(category)
    # consolidating all into a list for iterating and injecting into firestore    
    cols = df.columns.to_list()


    # inserting the data into the equity industry collection
    collection = 'equity_industry'
    for index, row in df.iterrows():
        data = {}
        for i in cols:
            categories = index
            data_dict = {
                i : row[i]
            }
            data.update(data_dict)
        # print (categories, data)
        try:
            # when there are no categories, stop and exit without inserting data
            db.collection(collection).document(categories).set(data, merge=True)
        except:
            pass

 
######################################################################################
####### Calculate the individual ratio ranking vs industry ###########################
######################################################################################

# change the name of the industry to add median and sum prefix to it, if not it will be hard to distinguish in the equity collection kpi
# set up the aggregator so that it runs once daily
# set up the function that throws the aggregator numbers into the equity kpi list
# set up the function that goes through each ticker within each industry to rank its postion against the entire industry and dump the rank back into the kpi list
 





######################################################################################
####### Calculate the individual ratio ranking vs industry ###########################
######################################################################################
# python -c 'from mgt_fb_crud import testing; testing()'




## design the aggregation process


## process
# - daily runs capture all daily kpi data
# - next run to do an aggregation by country, sector and industry of the KPIs involved
# - next run to extract filter sector, industry country on the daily kpi data rank them by KPIs 
#   1) insert rank into daily kpi data
#   2) insert average and median kpi of sector/industry into daily kpi data


## Steps in building this process
# - adjust the daily run to insert the country, sector and industry into daily run kpi collection and push that live and observe if working fine
# - create the collection for country, sector and industry
# - create the function that updates the aggregation collection for country, sector and industry

###### - think about the creation of this function because it could be tricky
# = create the function that extracts from daily kpi and cycle through and filter a specific sector/industry and all the ticker by a certain kpi and dump the rank
#   into the daily run collection