
from secret import access_secret
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build


google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()

########################################################################################
###########  Export function to google sheets  #########################################
########################################################################################
# python -c 'from export_gs import export_gs_func; export_gs_func()'

def export_gs_func(name, df ,sheetinfo):

    dfcol = []
    for i in df.columns:
        try:
            i = i.strftime('%Y-%m-%d')
        except:
            pass
        dfcol.append(i)

    dfindex = []
    for i in df.index:
        dfindex.append([i])

    dflist = df.values.tolist()

    #Inject the values
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!B2", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()

    #Inject the index
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!A2", valueInputOption="USER_ENTERED", body={"values":dfindex}).execute()

    #Inject the fields
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!B1", valueInputOption="USER_ENTERED", body={"values":[dfcol]}).execute()

    #Inject the name
    request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
        range=sheetinfo+"!A1", valueInputOption="USER_ENTERED", body={"values":[[name]]}).execute()