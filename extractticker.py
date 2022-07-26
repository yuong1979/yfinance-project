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

from bs4 import BeautifulSoup
import requests



r = requests.get('https://companiesmarketcap.com/')

# print (r.text[0:10000000])

soup = BeautifulSoup(r.text, 'html.parser')

# <div class="company-code"><span class="rank d-none"></span>2222.SR</div>

results = soup.find_all(attrs={"class":'company-code'})

print (len(results))

print (results[0].contents[1])

# https://companiesmarketcap.com/page/65/


for i in results:
    print (i.contents[1])



