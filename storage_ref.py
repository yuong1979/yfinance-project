from google.cloud import storage
import google.cloud.storage
import json
import os
import sys


import pandas as pd 
import io
from io import BytesIO
from secret import access_secret
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key, cloud_storage_key


key = access_secret(cloud_storage_key, project_id)
key = str(key)


#create client object
storage_client = storage.Client(key)

print (storage_client)

# #reading the name of all files from bucket
bucket = storage_client.get_bucket('test_cloud_storage_bucket_blockmacro')

filename = [filename.name for filename in list(bucket.list_blobs(prefix='')) ]
print(filename)


# #downloading file from bucket
# blop = bucket.blob(blob_name = 'industry_data.csv').download_as_string()

# with open ('industry_data.csv', "wb") as f:
#         f.write(blop)


# #pushing a file on gcp bucket
# filename= 'industry_data.csv'
# UPLOADFILE = os.path.join(os.getcwd(), filename)
# bucket = storage_client.get_bucket('scientist1995-test')
# blob = bucket.blob(filename)
# blob.upload_from_filename(UPLOADFILE)

# #Reading CSV File Drieectly from GCP bucket
# df = pd.read_csv(
#     io.BytesIO(
#                  bucket.blob(blob_name = 'industry_data.csv').download_as_string() 
#               ) ,
#                  encoding='UTF-8',
#                  sep=',')

# print (df)


# #delete file form gcp bucket
# DELETE_FILE = 'industry_data.csv'
# bucket = storage_client.get_bucket('scientist1995-test')
# blob = bucket.blob(DELETE_FILE)
# blob.delete()

