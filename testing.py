from firebase_admin import firestore
import pandas as pd
import numpy as np
from firebase_admin import firestore, credentials, initialize_app
from fx import ext_daily_fx_yf_fb
from secret import access_secret
import json
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key



cloud_run_apikey = access_secret(schedule_function_key, project_id)
firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)

# Initialize Firestore DB
cred = credentials.Certificate(firestore_api_key_dict)
default_app = initialize_app(cred)
db = firestore.client()

pw = cloud_run_apikey


equity_financials_hist_list = db.collection('equity_financials').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(3).stream()




print (equity_financials_hist_list)


