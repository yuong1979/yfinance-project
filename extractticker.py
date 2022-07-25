from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import gspread
import json
import yfinance as yf
from datetime import datetime, timedelta



SERVICE_ACCOUNT_FILE = 'googlesheetsapi-keys.json'

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# The ID and range of a sample spreadsheet.
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'

service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

sheetinfo = "Info"

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
    # print(kpilist_in_list)

except HttpError as err:
    print(err)

#Inject the KPIS into the rows of the info
request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
    range=sheetinfo+"!C1", valueInputOption="USER_ENTERED", body={"values":[kpilist]}).execute()


#collect all the tickers
result = sheet.values().get(spreadsheetId=REQUIRED_SPREADSHEET_ID,
                            range=sheetinfo+"!A2:A10000").execute()
company = result.get('values', [])

row = 2
for [i] in company:
    companyticker = yf.Ticker(i)
    datalist = []
    for j in kpilist:
        companytickerwithKPI = companyticker.info[j]
        datalist.append(companytickerwithKPI)

    #update the data
    date_today = datetime.today().replace(microsecond=0)

    result = sheet.values().get(spreadsheetId=REQUIRED_SPREADSHEET_ID,
                                range=sheetinfo+"!B"+str(row)).execute()
    date_time_str = result.get('values', [])

    try:
        date_time_str = date_time_str[0][0].strip('"')
        recordeddatetime = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        dateexist = True
    except:
        dateexist = False

    recordplusone = recordeddatetime + timedelta(minutes=5)

    # if it was 5 minutes since the record was last extracted or if the date does not exist then extract again
    if (date_today > recordplusone) or (dateexist == False) :
        date_today = json.dumps(date_today, indent=4, sort_keys=True, default=str)
        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!B"+str(row), valueInputOption="USER_ENTERED", body={"values":[[date_today]]}).execute()

        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!C"+str(row), valueInputOption="USER_ENTERED", body={"values":[datalist]}).execute()
        print (i, sheetinfo + " extracted")
    else: 
        print (i, sheetinfo + " passed")
        pass
   
    row = row + 1



