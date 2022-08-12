from calendar import day_abbr, month
from sre_parse import State
import yfinance as yf
import pandas as pd
import numpy as np
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import json
import yfinance as yf
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
from firebase_admin import firestore
import pandas as pd
import time
import pytz
import re
from dateutil.relativedelta import relativedelta
from firebase_admin import firestore
import pytz
import time
from secret import access_secret
from google.cloud.firestore import Client

# db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json"


# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"

# secret manager - python tutorial
# https://codelabs.developers.google.com/codelabs/secret-manager-python#8
# if running on local - need to create secret / service manager / api key
# https://cloud.google.com/docs/authentication/getting-started

### bare minimum
# pip install firebase-admin
# pip install Flask
# google-cloud-secret-manager
# pip install pandas
# pip install yfinance
# pip install beautifulsoup4
