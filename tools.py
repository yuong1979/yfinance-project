import smtplib
from email.message import EmailMessage
from settings import (project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, 
                    schedule_function_key, firebase_auth_api_key, email_password, cloud_storage_key)
from secret import access_secret
import pytz
from datetime import datetime
from google.oauth2 import service_account
from google.cloud.firestore import Client
import json
from firebase_admin import firestore
from googleapiclient.discovery import build
from time import process_time
from google.cloud import storage
import os
import pandas as pd


email_password = access_secret(email_password, project_id)

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()







# python -c 'from tools import testing; testing()'

def testing():


    date_mapping = date_mapper()

    df_test = pd.read_pickle('data/eq_hist_details.pickle')


    print (df_test)
    print (df_test.dtypes)
    print (df_test.columns)




# #################################################################################################
# ####### Calculating date mapping ####################################################
# #################################################################################################
# python -c 'from tools import date_mapping; date_mapping()'

def date_mapper():

    # building the date mapping to group dates from financials in different quarterly buckets
    tz_UTC = pytz.timezone('UTC')
    today_date = datetime.now(tz_UTC).date()
    test = pd.date_range(start='2009-01-01', end=today_date, inclusive="both")
    test_df = pd.DataFrame(test, columns=['date'])

    quarter = pd.PeriodIndex(test_df['date'], freq='Q', name='Quarter')
    test_df['quarter'] = quarter
    year = pd.PeriodIndex(test_df['date'], freq='Y', name='Year')
    test_df['year'] = year

    qtr_last_date = test_df.groupby(['quarter']).last()
    qtr_last_date = qtr_last_date.reset_index()
    qtr_last_date = qtr_last_date.rename({'date': 'qtr_last_date'}, axis=1)
    qtr_last_date = qtr_last_date[['quarter','qtr_last_date']]

    ann_last_date = test_df.groupby(['year']).last()
    ann_last_date = ann_last_date.reset_index()
    ann_last_date = ann_last_date.rename({'date': 'ann_last_date'}, axis=1)
    ann_last_date = ann_last_date[['year','ann_last_date']]

    df_date_merge = pd.merge(test_df, qtr_last_date, how='left', left_on='quarter', right_on = 'quarter')
    date_mapping = pd.merge(df_date_merge, ann_last_date, how='left', left_on='year', right_on = 'year')

    return (date_mapping)




# #################################################################################################
# ####### Calculating time taken for processes ####################################################
# #################################################################################################
# python -c 'from equity_transform import calculate_time; calculate_time()'

def calculate_time():
    t1_start = process_time()
    t1_stop = process_time()
    print("Elapsed time during the whole program in seconds:",t1_stop-t1_start) 


# #################################################################################################
# ####### Convert digits to alphabets #############################################################
# #################################################################################################
# python -c 'from equity_transform import convert_digits; convert_digits()'


trillion = 1000_000_000_000
billion = 1000_000_000
million = 1000_000
thousand = 1000

n_thousand = -1000
n_million = -1000_000
n_billion = -1000_000_000
n_trillion = -1000_000_000_000

def convert_digits(num):
    try:
        if num >= trillion:
            number = str(round(float(num / trillion),2)) + 'T'        
        elif num >= billion:
            number = str(round(float(num / billion),2)) + 'B'
        elif num >= million:
            number = str(round(float(num / million),2)) + 'M'
        elif num >= thousand:
            number = str(round(float(num / thousand),2)) + 'K'

        elif num <= n_trillion:
            number = str(round(float(num / trillion),2)) + 'T'

        elif num <= n_billion:
            number = str(round(float(num / billion),2)) + 'B'

        elif num <= n_million:
            number = str(round(float(num / million),2)) + 'M'

        elif num <= n_thousand:
            number = str(round(float(num / thousand),2)) + 'K'

        else:
            number = num
    except:
        number = num
            
    return number



# #######################################################################################################
# ############### Push data from inside data folder to google cloud storage #############################
# #######################################################################################################

# python -c 'from tools import upload_cloud_storage; upload_cloud_storage()'

def upload_cloud_storage(filename):
    #upload to gcp cloud storage
    key = access_secret(cloud_storage_key, project_id)
    storage_client = storage.Client(str(key))
    #pushing a file on gcp bucket
    UPLOADFILE = os.path.join(os.getcwd() + "/data", filename)
    bucket = storage_client.get_bucket('test_cloud_storage_bucket_blockmacro')
    blob = bucket.blob(filename)
    blob.upload_from_filename(UPLOADFILE)



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


# # ####################################################################################################################################
# # ############### To decide if the extraction needs to proceed (if the earliest extracted date is beyond a target) ###################
# # ####################################################################################################################################


# firestore_api_key = access_secret(firestore_api_key, project_id)
# firestore_api_key_dict = json.loads(firestore_api_key)
# fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
# db = Client(firebase_database, fbcredentials)

# # if the earliest date is not 
# def decide_extraction(target_hours, collection, collection_updated_datetime):
#     tz_SG = pytz.timezone('Singapore')
#     # time_in_hours = 1
#     # time_seconds = 60 * 60 * time_in_hours 
#     latest_entry = db.collection(collection).order_by(collection_updated_datetime, direction=firestore.Query.ASCENDING).limit(1).get()
#     time_passed = datetime.now(tz_SG) - latest_entry[0]._data[collection_updated_datetime]

#     #breaking down and converting the time passed into days and seconds
#     days, seconds = time_passed.days, time_passed.seconds
#     hours_passed = days * 24 + seconds // 3600

#     # print (time_passed)
#     # print (hours_passed)
#     # print (datetime.now(tz_SG), "timenow")
#     # print (latest_entry[0]._data[collection_updated_datetime], "collection last updated datetime")

#     if hours_passed < target_hours:
#         print ('exiting because the latest entry has been extracted less than 24 hours ago')
#         exit()
#     else:
#         print("proceed with extraction, data last extracted ", hours_passed, "hours ago")



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





kpi_remove = [
'company_count',
'ebitdaUSD',
'marketCapUSD',
'totalRevenueUSD',
'fullTimeEmployees',
'priceToSalesTrailing12Months',
'earningsQuarterlyGrowth',
'trailingAnnualDividendRate',
'fiveYearAvgDividendYield',
'trailingAnnualDividendYield',
'daily_agg_record_time',
]


kpi_mapping = {
'company_count': True,
'ebitdaUSD': True,
'marketCapUSD': True,
'totalRevenueUSD': True,
'fullTimeEmployees': False,

# profitability
'grossMargins': True,
'operatingMargins': True,
'ebitdaMargins': True,
'profitMargins': True,

# growth
'earningsGrowth': True,
'revenueGrowth': True,

# value
'forwardEps': True,
'trailingEps': True,

'forwardPE': False,
'trailingPE': False,

'pegRatio': False,
'trailingPegRatio': False,

'enterpriseToEbitda': False,
'enterpriseToRevenue': False,

'returnOnEquity': True,
'returnOnAssets': True,

# popularity
'heldPercentInstitutions': True,
'heldPercentInsiders': True,

# financial health
'debtToEquity': False,
'quickRatio': True,
'currentRatio': True,

# dividends
'dividendYield': True,
'dividendRate': True,
'trailingAnnualDividendRate': True,
'fiveYearAvgDividendYield': True,
'trailingAnnualDividendYield': True,

'earningsQuarterlyGrowth': True,
'priceToBook': False,
'priceToSalesTrailing12Months': False

}
