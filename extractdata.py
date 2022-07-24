from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account


import json
import yfinance as yf

SERVICE_ACCOUNT_FILE = 'googlesheetsapi-keys.json'

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# The ID and range of a sample spreadsheet.
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

#collecting the Tickers
try:
    range_names = ["Tracker!A2:A20"]
    result = sheet.values().batchGet(
        spreadsheetId=REQUIRED_SPREADSHEET_ID, ranges=range_names).execute()
    Tickerranges = result.get('valueRanges', [])
    Tickdata = Tickerranges[0]['values']
    Ticklist = []
    for [i] in Tickdata:
        Ticklist.append(i)
    # print (Ticklist)

except HttpError as err:
    print(err)


#collecting the KPIs
try:
    range_names = ["KPIs!D2:D200"]
    result = sheet.values().batchGet(
        spreadsheetId=REQUIRED_SPREADSHEET_ID, ranges=range_names).execute()
    kpiranges = result.get('valueRanges', [])
    Kpidata = kpiranges[0]['values']
    kpilist = []
    for i in Kpidata:
        if i == []:
            pass
        else:
            kpilist.append(i[0])
    kpilist_in_list = [kpilist]

except HttpError as err:
    print(err)


#Inject the KPIS into the rows of the tracker
try:
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range="Tracker!B1", valueInputOption="USER_ENTERED", body={"values":kpilist_in_list})
    response = request.execute()

except HttpError as err:
    print(err)







# update the info data into the tracker - this works but rate limit gets hit very easily need to optimize
row = 2
for i in Ticklist:
    companyticker = yf.Ticker(i)
    datalist = []
    for j in kpilist:
        companytickerwithKPI = companyticker.info[j]
        datalist.append(companytickerwithKPI)

    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range="Tracker!B"+str(row), valueInputOption="USER_ENTERED", body={"values":[datalist]})
    response = request.execute()
    row = row + 1








