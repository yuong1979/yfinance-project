from calendar import day_abbr, month
from sre_parse import State
import yfinance as yf
import pandas as pd
import numpy as np


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

from firebase_admin import firestore
import pandas as pd
import plotly.express as px
import time
from datetime import datetime as dt
import pytz
import re

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from firebase_admin import firestore
db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

import pytz
import time




