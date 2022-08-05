
from datetime import datetime, timedelta
from firebase_admin import firestore
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta

# ###############################################################################
# ######## Daily Updating google sheets with firebase ###########################
# ###############################################################################
db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")


#### export required data to google sheets ######
docs = db.collection('tickerlist').stream()

datalist = []
for i in docs:
    try:
        data = i._data['kpi']
        data["ticker"] = i._data['ticker']
        data["tickername"] = i._data['tickername']
        #got to convert datetime into a format acceptable by google sheets and wont throw error
        data["updated_datetime"] = i._data['updated_datetime'].strftime('%Y-%m-%d %H:%M:%S')
        datalist.append(data)
    except KeyError:
        pass

df = pd.DataFrame(datalist)
df.replace(np.nan, '', inplace=True)

print (df)

# kpilistcomplete = ['fiftyDayAverage', 'shortPercentOfFloat', 'askSize', 'bid', 'targetLowPrice', 'ask', 'phone', 'grossProfits', 'currentRatio', 'volume', 'trailingEps', 'exchangeDataDelayedBy', 'tradeable', 'category', 'annualHoldingsTurnover', 'regularMarketPrice', 'dayHigh', 'algorithm', 'uuid', 'SandP52WeekChange', 'gmtOffSetMilliseconds', 'revenuePerShare', 'forwardEps', 'headSymbol', 'targetHighPrice', 'priceToSalesTrailing12Months', 'postMarketChange', 'netIncomeToCommon', 'lastFiscalYearEnd', 'startDate', 'underlyingExchangeSymbol', 'marketCap', 'earningsQuarterlyGrowth', 'fundInceptionDate', 'messageBoardId', 'freeCashflow', 'ebitda', 'bookValue', 'revenueQuarterlyGrowth', 'priceHint', '52WeekChange', 'volume24Hr', 'dividendYield', 'forwardPE', 'targetMedianPrice', 'maxAge', 'dateShortInterest', 'fiftyTwoWeekHigh', 'city', 'grossMargins', 'expireDate', 'shortRatio', 'regularMarketSource', 'trailingPegRatio', 'legalType', 'morningStarRiskRating', 'operatingMargins', 'zip', 'regularMarketDayLow', 'website', 'sharesShortPreviousMonthDate', 'enterpriseValue', 'industry', 'debtToEquity', 'averageVolume', 'underlyingSymbol', 'lastCapGain', 'dividendRate', 'preMarketPrice', 'recommendationKey', 'companyOfficers', 'currentPrice', 'preMarketChange', 'earningsGrowth', 'coinMarketCapLink', 'sharesPercentSharesOut', 'maxSupply', 'regularMarketChange', 'financialCurrency', 'regularMarketVolume', 'fromCurrency', 'threeYearAverageReturn', 'twoHundredDayAverage', 'nextFiscalYearEnd', 'returnOnAssets', 'exchangeName', 'address1', 'isEsgPopulated', 'returnOnEquity', 'regularMarketPreviousClose', 'quoteSourceName', 'morningStarOverallRating', 'numberOfAnalystOpinions', 'totalDebt', 'floatShares', 'fullTimeEmployees', 'mostRecentQuarter', 'payoutRatio', 'averageDailyVolume10Day', 'lastMarket', 'totalCashPerShare', 'sharesShortPriorMonth', 'previousClose', 'lastSplitDate', 'open', 'marketState', 'fiveYearAvgDividendYield', 'heldPercentInstitutions', 'fundFamily', 'regularMarketDayHigh', 'operatingCashflow', 'volumeAllCurrencies', 'market', 'regularMarketTime', 'fiveYearAverageReturn', 'quickRatio', 'symbol', 'trailingAnnualDividendYield', 'sharesOutstanding', 'beta', 'postMarketPrice', 'totalAssets', 'yield', 'toCurrency', 'pegRatio', 'circulatingSupply', 'averageDailyVolume3Month', 'annualReportExpenseRatio', 'totalRevenue', 'longName', 'exchange', 'trailingPE', 'recommendationMean', 'lastSplitFactor', 'quoteType', 'country', 'bidSize', 'trailingAnnualDividendRate', 'impliedSharesOutstanding', 'targetMeanPrice', 'ytdReturn', 'longBusinessSummary', 'currency', 'revenueGrowth', 'logo_url', 'sector', 'exchangeTimezoneShortName', 'beta3Year', 'averageVolume10days', 'navPrice', 'exchangeTimezoneName', 'profitMargins', 'totalCash', 'priceToBook', 'lastDividendValue', 'heldPercentInsiders', 'dayLow', 'sharesShort', 'enterpriseToEbitda', 'state', 'regularMarketOpen', 'openInterest', 'exDividendDate', 'lastDividendDate', 'fiftyTwoWeekLow', 'strikePrice', 'ebitdaMargins', 'shortName', 'enterpriseToRevenue', 'fax']

kpilistselect = ['updated_datetime', 'ticker','tickername',
'shortName', 'longBusinessSummary','symbol', 'sector', 'industry', 'country', 'marketCap',  
'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare',
'debtToEquity', 'operatingMargins', 'profitMargins', 'ebitda', 'ebitdaMargins',
'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth',
'trailingEps', 'forwardEps', 
'pegRatio', 'trailingPegRatio',
'currentRatio', 'quickRatio', 
'bookValue', 'enterpriseValue', 'priceToBook', 
'freeCashflow', 'operatingCashflow', 'dividendYield', 'dividendRate', 
'grossProfits', 'grossMargins', 'totalRevenue', 'totalDebt', 'beta', 
'volume', 'priceToSalesTrailing12Months', 'targetMedianPrice', 'fiftyTwoWeekHigh', 'city', 'shortRatio', 'averageVolume', 'underlyingSymbol', 
'currentPrice',  'financialCurrency','twoHundredDayAverage', 'nextFiscalYearEnd', 'address1', 'isEsgPopulated', 'targetMeanPrice',  
'currency', 'logo_url', 'longName', 'regularMarketPreviousClose', 'numberOfAnalystOpinions',  'floatShares', 'fullTimeEmployees', 
'mostRecentQuarter', 'payoutRatio', 'averageDailyVolume10Day', 'totalCashPerShare', 'sharesShortPriorMonth', 'fiveYearAvgDividendYield', 
'heldPercentInstitutions', 'trailingAnnualDividendYield', 'sharesOutstanding', 'recommendationMean', 'lastSplitFactor', 'bidSize', 
'trailingAnnualDividendRate', 'averageVolume10days', 'totalCash','lastDividendValue', 'heldPercentInsiders', 'dayLow', 'sharesShort', 
'enterpriseToEbitda', 'regularMarketOpen', 'exDividendDate', 'lastDividendDate', 'fiftyTwoWeekLow', 'enterpriseToRevenue']


#change the column location / select the list of columns to be used
# df = df[['symbol','sector','currency','returnOnEquity','industry']]
df = df[kpilistselect]

dflist = df.values.tolist()
dfcol = df.columns.tolist()

SERVICE_ACCOUNT_FILE = 'secret/googlesheetsapi-keys.json'
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# The ID and range of a sample spreadsheet.
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()
sheetinfo = "Info"


#Inject the KPIS into the rows of the info
request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
    range=sheetinfo+"!A2", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()


#Inject the KPIS into the rows of the info
request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
    range=sheetinfo+"!A1", valueInputOption="USER_ENTERED", body={"values":[dfcol]}).execute()
