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



email_password = access_secret(email_password, project_id)





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
def decide_extraction(time_in_hours, collection, collection_updated_datetime):
    tz_SG = pytz.timezone('Singapore')
    # time_in_hours = 1
    time_seconds = 60 * 60 * time_in_hours 
    latest_entry = db.collection(collection).order_by(collection_updated_datetime, direction=firestore.Query.ASCENDING).limit(1).get()
    time_diff = datetime.now(tz_SG) - latest_entry[0]._data[collection_updated_datetime]

    if time_diff.seconds < time_seconds:
        print ('exiting because the latest entry has been extracted less than 24 hours ago')
        exit()






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