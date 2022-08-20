
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from secret import access_secret
from google.oauth2 import service_account
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
import json
from firebase_admin import firestore
import time
from google.cloud.firestore import Client

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()

firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)


########################################################################################
###########  Sample export dataframe to google sheets  #################################
########################################################################################
# python -c 'from ref_yfinance import sample_df_gs; sample_df_gs()'

def sample_df_gs():
    companyticker = yf.Ticker("meta")

    df = companyticker.quarterly_financials
    print (df)
    df = companyticker.quarterly_balancesheet
    print (df)
    df = companyticker.quarterly_cashflow
    print (df)

    df = companyticker.financials
    print (df)
    df = companyticker.balancesheet
    print (df)
    df = companyticker.cashflow
    print (df)


    dfindex = []
    for i in df.index:
        dfindex.append([i])

    dfcol = []
    for i in df.columns:
        i = i.strftime('%Y-%m-%d')
        dfcol.append(i)

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




########################################################################################
###########  Test export yfinance dataframe to fb  #####################################
########################################################################################
# python -c 'from ref_yfinance import test_extraction; test_extraction()'

def test_extraction():

    docs = db.collection('tickerlisttest').get()

    for i in docs:
        print ("Updating " + str(i._data['ticker']))

        ticker = i._data['ticker']
        companyinfo = yf.Ticker(ticker)

        data = { 
            'quarterly_financials' : str(companyinfo.quarterly_financials),
            'quarterly_balancesheet' : str(companyinfo.quarterly_balancesheet),
            'quarterly_cashflow' : str(companyinfo.quarterly_cashflow),
            'annual_financials' : str(companyinfo.financials),
            'annual_balancesheet' : str(companyinfo.balancesheet),
            'annual_cashflow' : str(companyinfo.cashflow),

            'updated_datetime': firestore.SERVER_TIMESTAMP,
            'activated': True
        }

        #updating data into firebase
        db.collection('tickerlisttest').document(i.id).set(data, merge=True)
        print ("Updated " + str(i._data['ticker']))


### try converting data into dataframe #####


########################################################################################
########### try converting data into dataframe #########################################
########################################################################################
# python -c 'from ref_yfinance import ticker_investigation; ticker_investigation()'

ticker = 'LOVE'

def ticker_investigation():
    docs = db.collection('tickerlisttest').where("ticker", "==", ticker).get()


    test = docs[0]._data['quarterly_financials']

    # print (type(test))

    # df = pd.DataFrame(test)

    # print (df)


    import sys
    if sys.version_info[0] < 3: 
        from StringIO import StringIO
    else:
        from io import StringIO

    import pandas as pd

    TESTDATA = StringIO(test)

    df = pd.read_csv(TESTDATA, sep=";")

    print (df)

# recommendations = companyticker.recommendations['To Grade'].value_counts()
# print (recommendations, 'recommendations')

# price_history = companyticker.history(period="max")
# print (price_history, 'price history')

# #extracting financials
# df = companyticker.financials
# df.columns = pd.to_datetime(df.columns).strftime('%Y')
# kpilist = ['Total Revenue', 'Net Income', 'Operating Income', 'Ebit']
# df = df.loc[kpilist,]
# print (df, "what is this?")

# balancesheet = companyticker.balancesheet
# print (companyticker.balancesheet, "balancesheet")

# financials = companyticker.financials
# print (financials, "financials")

# cash_flow = companyticker.cashflow
# print (cash_flow, "cash_flow")

# # revenue and earnings - last 4 years
# rev_earn = companyticker.earnings
# print (rev_earn, "rev_earn last 4 years")

# # eps since listing
# earnings_history = companyticker.earnings_history
# print (earnings_history, "earnings_history")










# # sample information

# companyticker = yf.Ticker("meta")
# print (companyticker.recommendations['To Grade'].value_counts())
# print (companyticker.financials)


# #extracting financials
# df = companyticker.financials
# df.columns = pd.to_datetime(df.columns).strftime('%Y')
# kpilist = ['Total Revenue', 'Net Income', 'Operating Income', 'Ebit']
# df = df.loc[kpilist,]
# print (df)


# #extracting historical price
# date_today = datetime.today().replace(microsecond=0)
# startdate = date_today - relativedelta(years=5)
# enddate = date_today - timedelta(days=1)
# pricehistory = companyticker.history(start=startdate,  end=enddate)
# # choose only closing date
# pricehistory = pricehistory.iloc[:, [3]]
# data = pricehistory.loc[pricehistory.index.day==1]
# print (data)

# # current information
# msft = yf.Ticker("msft")

# print (msft.info['industry'])

# # price history
# hist = msft.history(period="max")
# print (hist)

# # recommendations
# print (msft.recommendations['To Grade'].value_counts())

# # ytd details
# print (msft.info)

# # stock splits and dividends
# print (msft.actions)

# # balance sheet
# print (msft.balancesheet)

# # pnl
# print (msft.financials)

# # cashflow
# print (msft.cashflow)

# # revenue and earnings - last 4 years
# print (msft.earnings)

# # eps since listing
# print (msft.earnings_history)








