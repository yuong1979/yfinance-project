import smtplib
from email.message import EmailMessage
from settings import (project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, 
                    schedule_function_key, firebase_auth_api_key, email_password)
from secret import access_secret
import pytz
from datetime import datetime
from google.oauth2 import service_account
from google.cloud.firestore import Client
from secret import access_secret

import json
from firebase_admin import firestore
from googleapiclient.discovery import build


email_password = access_secret(email_password, project_id)

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()



# #######################################################################################################
# ############### Error email ##########################################################################
# #######################################################################################################

# python -c 'from email_function import error_email; error_email()'

def error_email(subject, content):
    EMAIL_ADDRESS = "macrokpi2022@gmail.com"
    EMAIL_PASSWORD = email_password
    from_contacts = [EMAIL_ADDRESS]
    to_contacts = [EMAIL_ADDRESS]
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = from_contacts
    msg['To'] = to_contacts
    content_to_send = content
    msg.set_content(content_to_send)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


# ####################################################################################################################################
# ############### To decide if the extraction needs to proceed (if the earliest extracted date is beyond a target) ###################
# ####################################################################################################################################


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

# if the earliest date is not 
def decide_extraction(target_hours, collection, collection_updated_datetime):
    tz_SG = pytz.timezone('Singapore')
    # time_in_hours = 1
    # time_seconds = 60 * 60 * time_in_hours 
    latest_entry = db.collection(collection).order_by(collection_updated_datetime, direction=firestore.Query.ASCENDING).limit(1).get()
    time_passed = datetime.now(tz_SG) - latest_entry[0]._data[collection_updated_datetime]

    #breaking down and converting the time passed into days and seconds
    days, seconds = time_passed.days, time_passed.seconds
    hours_passed = days * 24 + seconds // 3600

    # print (hours_passed)
    # print (datetime.now(tz_SG), "timenow")
    # print (latest_entry[0]._data[collection_updated_datetime], "collection last updated datetime")

    if hours_passed < target_hours:
        print ('exiting because the latest entry has been extracted less than 24 hours ago')
        exit()
    else:
        print("proceed with extraction, data last extracted ", hours_passed, "hours ago")



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







# #######################################################################################################
# ############### Sample email ##########################################################################
# #######################################################################################################
# python -c 'from email_function import sample_email; sample_email()'

def sample_email():
    EMAIL_ADDRESS = "macrokpi2022@gmail.com"
    EMAIL_PASSWORD = email_password
    contacts = [EMAIL_ADDRESS]
    msg = EmailMessage()
    msg['Subject'] = 'Grab dinner this weekend'
    msg['From'] = EMAIL_ADDRESS
    msg['To'] = contacts
    msg.set_content('How about dinner at 6pm this saturday ok,  dude dont be bwei swee!')
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)