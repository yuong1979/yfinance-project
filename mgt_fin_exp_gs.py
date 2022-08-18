
from curses import use_default_colors
from datetime import datetime, timedelta
from firebase_admin import firestore
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
import json
from google.cloud.firestore import Client
from secret import access_secret
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
from email_function import error_email
import inspect


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

google_sheets_api_key = access_secret(google_sheets_api_key, project_id)
google_sheets_api_key_dict = json.loads(google_sheets_api_key)
gscredentials = service_account.Credentials.from_service_account_info(google_sheets_api_key_dict)
REQUIRED_SPREADSHEET_ID = '1_lobEzbiuP9TE2UZqmqSAwizT8f2oeuZ8mVuUTbBAsA'
service = build('sheets', 'v4', credentials=gscredentials)
sheet = service.spreadsheets()




# ###############################################################################
# ######## Daily Updating google sheets - ticker info from firebase #############
# ###############################################################################

############### Running the function from the command line ###############
# python -c 'from mgt_fin_exp_gs import ext_daily_equity_financials_fb_gs; ext_daily_equity_financials_fb_gs()'

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))



def ext_daily_equity_financials_fb_gs():

    try:

        #### export required data to google sheets ######
        docs = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.ASCENDING).stream()

        datalist = []
        for i in docs:
            try:
                data = i._data['kpi']
                data["ticker"] = i._data['ticker']
                data["tickername"] = i._data['tickername']

                #inserting all the kpis into list to be exported to google sheets
                kpilist = ['marketCapUSD', 'enterpriseValueUSD', 'freeCashflowUSD', 'operatingCashflowUSD', 
                    'totalDebtUSD', 'totalRevenueUSD', 'grossProfitsUSD', 'ebitdaUSD', 'currentPriceUSD']

                for j in kpilist:

                    try:
                        data[j] = i._data[j]
                    except:
                        data[j] = ""

                #got to convert datetime into a format acceptable by google sheets and wont throw error
                data["updated_datetime"] = i._data['updated_datetime'].strftime('%Y-%m-%d %H:%M:%S')
                datalist.append(data)
            except KeyError:
                pass

        df = pd.DataFrame(datalist)
        df.replace(np.nan, '', inplace=True)

        ## Complete list of KPIs
        # kpilistcomplete = ['fiftyDayAverage', 'shortPercentOfFloat', 'askSize', 'bid', 'targetLowPrice', 'ask', 'phone', 'grossProfits', 'currentRatio', 'volume', 'trailingEps', 'exchangeDataDelayedBy', 'tradeable', 'category', 'annualHoldingsTurnover', 'regularMarketPrice', 'dayHigh', 'algorithm', 'uuid', 'SandP52WeekChange', 'gmtOffSetMilliseconds', 'revenuePerShare', 'forwardEps', 'headSymbol', 'targetHighPrice', 'priceToSalesTrailing12Months', 'postMarketChange', 'netIncomeToCommon', 'lastFiscalYearEnd', 'startDate', 'underlyingExchangeSymbol', 'marketCap', 'earningsQuarterlyGrowth', 'fundInceptionDate', 'messageBoardId', 'freeCashflow', 'ebitda', 'bookValue', 'revenueQuarterlyGrowth', 'priceHint', '52WeekChange', 'volume24Hr', 'dividendYield', 'forwardPE', 'targetMedianPrice', 'maxAge', 'dateShortInterest', 'fiftyTwoWeekHigh', 'city', 'grossMargins', 'expireDate', 'shortRatio', 'regularMarketSource', 'trailingPegRatio', 'legalType', 'morningStarRiskRating', 'operatingMargins', 'zip', 'regularMarketDayLow', 'website', 'sharesShortPreviousMonthDate', 'enterpriseValue', 'industry', 'debtToEquity', 'averageVolume', 'underlyingSymbol', 'lastCapGain', 'dividendRate', 'preMarketPrice', 'recommendationKey', 'companyOfficers', 'currentPrice', 'preMarketChange', 'earningsGrowth', 'coinMarketCapLink', 'sharesPercentSharesOut', 'maxSupply', 'regularMarketChange', 'financialCurrency', 'regularMarketVolume', 'fromCurrency', 'threeYearAverageReturn', 'twoHundredDayAverage', 'nextFiscalYearEnd', 'returnOnAssets', 'exchangeName', 'address1', 'isEsgPopulated', 'returnOnEquity', 'regularMarketPreviousClose', 'quoteSourceName', 'morningStarOverallRating', 'numberOfAnalystOpinions', 'totalDebt', 'floatShares', 'fullTimeEmployees', 'mostRecentQuarter', 'payoutRatio', 'averageDailyVolume10Day', 'lastMarket', 'totalCashPerShare', 'sharesShortPriorMonth', 'previousClose', 'lastSplitDate', 'open', 'marketState', 'fiveYearAvgDividendYield', 'heldPercentInstitutions', 'fundFamily', 'regularMarketDayHigh', 'operatingCashflow', 'volumeAllCurrencies', 'market', 'regularMarketTime', 'fiveYearAverageReturn', 'quickRatio', 'symbol', 'trailingAnnualDividendYield', 'sharesOutstanding', 'beta', 'postMarketPrice', 'totalAssets', 'yield', 'toCurrency', 'pegRatio', 'circulatingSupply', 'averageDailyVolume3Month', 'annualReportExpenseRatio', 'totalRevenue', 'longName', 'exchange', 'trailingPE', 'recommendationMean', 'lastSplitFactor', 'quoteType', 'country', 'bidSize', 'trailingAnnualDividendRate', 'impliedSharesOutstanding', 'targetMeanPrice', 'ytdReturn', 'longBusinessSummary', 'currency', 'revenueGrowth', 'logo_url', 'sector', 'exchangeTimezoneShortName', 'beta3Year', 'averageVolume10days', 'navPrice', 'exchangeTimezoneName', 'profitMargins', 'totalCash', 'priceToBook', 'lastDividendValue', 'heldPercentInsiders', 'dayLow', 'sharesShort', 'enterpriseToEbitda', 'state', 'regularMarketOpen', 'openInterest', 'exDividendDate', 'lastDividendDate', 'fiftyTwoWeekLow', 'strikePrice', 'ebitdaMargins', 'shortName', 'enterpriseToRevenue', 'fax']

        ## Selected KPIs excluding usd 
        kpilistselect1 = ['updated_datetime', 'ticker','tickername',
        'shortName', 'longBusinessSummary','symbol', 'sector', 'industry', 'country', 'marketCap',  
        'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare',
        'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
        'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth', 'priceToSalesTrailing12Months', 
        'trailingEps', 'forwardEps', 
        'pegRatio', 'trailingPegRatio',
        'currentRatio', 'quickRatio', 'debtToEquity', 
        'bookValue', 'enterpriseValue', 'priceToBook', 
        'freeCashflow', 'operatingCashflow', 'dividendYield', 'dividendRate', 
        'totalRevenue', 'grossProfits', 'ebitda', 'totalDebt', 'beta',
        'currency', 'financialCurrency',
        'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated',
        'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'lastDividendValue', 'lastDividendDate',
        'targetMedianPrice',  'targetMeanPrice', 'currentPrice', 
        'volume', 'averageVolume', 'averageVolume10days', 'averageDailyVolume10Day',
        'longName', 'city', 'address1',
        'fiftyTwoWeekHigh',  'shortRatio',  'underlyingSymbol', 'twoHundredDayAverage', 'nextFiscalYearEnd', 
        'logo_url',  'regularMarketPreviousClose', 'numberOfAnalystOpinions',  'floatShares', 'fullTimeEmployees', 
        'mostRecentQuarter', 'payoutRatio',  'totalCashPerShare', 'sharesShortPriorMonth',  
        'sharesOutstanding', 'recommendationMean', 'lastSplitFactor', 'bidSize', 'totalCash', 'dayLow', 'sharesShort', 
        'enterpriseToEbitda', 'regularMarketOpen', 'exDividendDate',  'fiftyTwoWeekLow', 'enterpriseToRevenue']


        ## Selected KPIs including usd values 
        kpilistselect2 = [
        'updated_datetime', 'ticker','tickername',
        'shortName', 'longBusinessSummary','symbol', 'sector', 'industry', 'country', 'marketCap',  
        'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare',
        'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
        'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth', 'priceToSalesTrailing12Months', 
        'trailingEps', 'forwardEps', 
        'pegRatio', 'trailingPegRatio',
        'currentRatio', 'quickRatio', 'debtToEquity', 
        'bookValue', 'enterpriseValue', 'priceToBook', 
        'freeCashflow', 'operatingCashflow', 'dividendYield', 'dividendRate', 
        'totalRevenue', 'grossProfits', 'ebitda', 'totalDebt', 'beta',
        'currency', 'financialCurrency',
        'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated',
        'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'lastDividendValue', 'lastDividendDate',
        'targetMedianPrice',  'targetMeanPrice', 'currentPrice',
        'marketCapUSD', 'enterpriseValueUSD', 'freeCashflowUSD', 'operatingCashflowUSD', 'totalDebtUSD',
        'currentPriceUSD', 'totalRevenueUSD', 'grossProfitsUSD', 'ebitdaUSD'
        ]

        #change the column location / select the list of columns to be used
        # df = df[['symbol','sector','currency','returnOnEquity','industry']]
        df = df[kpilistselect2]

        dflist = df.values.tolist()
        dfcol = df.columns.tolist()

        sheetinfo = "Info"

        #Inject the values into cols of the spreadsheet
        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!A2", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()


        #Inject the KPIS into the rows
        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!A1", valueInputOption="USER_ENTERED", body={"values":[dfcol]}).execute()


    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)


# ###############################################################################
# ######## Daily Updating google sheets - datetime info with firebase ###########
# ###############################################################################

############### Running the function from the command line ###############
# python -c 'from mgt_fin_exp_gs import ext_daily_equity_datetime_fb_gs; ext_daily_equity_datetime_fb_gs()'



def ext_daily_equity_datetime_fb_gs():
    
    try:
        #### export required data to google sheets ######
        docs = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.ASCENDING).stream()

        datalist = []
        for i in docs:
            try:
                data = i._data['kpi']
                data["ticker"] = i._data['ticker']
                data["tickername"] = i._data['tickername']
                #got to convert datetime into a format acceptable by google sheets and wont throw error
                data["updated_datetime"] = hour_rounder(i._data['updated_datetime']).strftime('%Y-%m-%d %H:%M:%S')
                datalist.append(data)
            except KeyError:
                pass

        df = pd.DataFrame(datalist)
        df.replace(np.nan, '', inplace=True)

        ## Selected KPIs including usd values 
        kpilistselect2 = [
        'updated_datetime', 'ticker'
        ]

        #change the column location / select the list of columns to be used
        # df = df[['symbol','sector','currency','returnOnEquity','industry']]
        df = df[kpilistselect2]

        df = df.groupby(['updated_datetime']).count()#.sort_values('updated_datetime', ascending=False)

        df = df.reset_index()

        #this needs to be fixed, because there are no columns in gs
        dflist = df.values.tolist()
        dfcol = df.columns.tolist()

        sheetinfo = "Datetime"

        #Inject the KPIS into the rows of the info
        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!A2", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()


        #Inject the KPIS into the rows of the info
        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!A1", valueInputOption="USER_ENTERED", body={"values":[dfcol]}).execute()

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)
