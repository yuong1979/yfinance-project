from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

SERVICE_ACCOUNT_FILE = 'googlesheetsapi-keys.json'

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'

try:
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range="Sheet1!A1:B3").execute()
    values = result.get('values', [])
    print(values)

    # Updating values
    aoa = [["1/1/2020",4000],["4/4/2021",9000],["1/5/2022",7000]]
    aoa = [["donkey"]]

    request = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range="Sheet2!B2", valueInputOption="USER_ENTERED", body={"values":aoa})
    response = request.execute()


except HttpError as err:
    print(err)