
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
import pytz

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
###########  Exploring y finance  ######################################################
########################################################################################
# python -c 'from ref_yfinance import sample_df_gs; sample_df_gs()'

def sample_df_gs():
    companyticker = yf.Ticker("aapl")

    print (companyticker.info)



    # df = companyticker.quarterly_financials
    # print (df)
    # df = companyticker.quarterly_balancesheet
    # print (df)
    # df = companyticker.quarterly_cashflow
    # print (df)

    # df = companyticker.financials
    # print (df)
    # df = companyticker.balancesheet
    # print (df)
    # df = companyticker.cashflow
    # print (df)

 


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








