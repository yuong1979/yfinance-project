
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
from tools import error_email
import inspect
import pytz

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




# ## naming conventions
# equity_list
# equity_price_history
# equity_financials
# equity_daily_kpi



# ###############################################################################
# ######## Export daily equity kpi into google sheets  ##########################
# ###############################################################################

############### Running the function from the command line ###############
# python -c 'from equity_exp import exp_equity_daily_kpi_gs; exp_equity_daily_kpi_gs()'

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))



def exp_equity_daily_kpi_gs():

    try:

        collection = 'equity_daily_kpi'
        #### export required data to google sheets ######
        docs = db.collection(collection).order_by("updated_datetime", direction=firestore.Query.ASCENDING).stream()

        datalist = []

        for i in docs:

            try:
                data = i._data['kpi']
                data["ticker"] = i._data['ticker']
                # data["tickername"] = i._data['tickername']

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
        # print (df)

        df.replace(np.nan, '', inplace=True)

        ## Complete list of KPIs
        # kpilistcomplete = ['fiftyDayAverage', 'shortPercentOfFloat', 'askSize', 'bid', 'targetLowPrice', 'ask', 'phone', 'grossProfits', 'currentRatio', 'volume', 'trailingEps', 'exchangeDataDelayedBy', 'tradeable', 'category', 'annualHoldingsTurnover', 'regularMarketPrice', 'dayHigh', 'algorithm', 'uuid', 'SandP52WeekChange', 'gmtOffSetMilliseconds', 'revenuePerShare', 'forwardEps', 'headSymbol', 'targetHighPrice', 'priceToSalesTrailing12Months', 'postMarketChange', 'netIncomeToCommon', 'lastFiscalYearEnd', 'startDate', 'underlyingExchangeSymbol', 'marketCap', 'earningsQuarterlyGrowth', 'fundInceptionDate', 'messageBoardId', 'freeCashflow', 'ebitda', 'bookValue', 'revenueQuarterlyGrowth', 'priceHint', '52WeekChange', 'volume24Hr', 'dividendYield', 'forwardPE', 'targetMedianPrice', 'maxAge', 'dateShortInterest', 'fiftyTwoWeekHigh', 'city', 'grossMargins', 'expireDate', 'shortRatio', 'regularMarketSource', 'trailingPegRatio', 'legalType', 'morningStarRiskRating', 'operatingMargins', 'zip', 'regularMarketDayLow', 'website', 'sharesShortPreviousMonthDate', 'enterpriseValue', 'industry', 'debtToEquity', 'averageVolume', 'underlyingSymbol', 'lastCapGain', 'dividendRate', 'preMarketPrice', 'recommendationKey', 'companyOfficers', 'currentPrice', 'preMarketChange', 'earningsGrowth', 'coinMarketCapLink', 'sharesPercentSharesOut', 'maxSupply', 'regularMarketChange', 'financialCurrency', 'regularMarketVolume', 'fromCurrency', 'threeYearAverageReturn', 'twoHundredDayAverage', 'nextFiscalYearEnd', 'returnOnAssets', 'exchangeName', 'address1', 'isEsgPopulated', 'returnOnEquity', 'regularMarketPreviousClose', 'quoteSourceName', 'morningStarOverallRating', 'numberOfAnalystOpinions', 'totalDebt', 'floatShares', 'fullTimeEmployees', 'mostRecentQuarter', 'payoutRatio', 'averageDailyVolume10Day', 'lastMarket', 'totalCashPerShare', 'sharesShortPriorMonth', 'previousClose', 'lastSplitDate', 'open', 'marketState', 'fiveYearAvgDividendYield', 'heldPercentInstitutions', 'fundFamily', 'regularMarketDayHigh', 'operatingCashflow', 'volumeAllCurrencies', 'market', 'regularMarketTime', 'fiveYearAverageReturn', 'quickRatio', 'symbol', 'trailingAnnualDividendYield', 'sharesOutstanding', 'beta', 'postMarketPrice', 'totalAssets', 'yield', 'toCurrency', 'pegRatio', 'circulatingSupply', 'averageDailyVolume3Month', 'annualReportExpenseRatio', 'totalRevenue', 'longName', 'exchange', 'trailingPE', 'recommendationMean', 'lastSplitFactor', 'quoteType', 'country', 'bidSize', 'trailingAnnualDividendRate', 'impliedSharesOutstanding', 'targetMeanPrice', 'ytdReturn', 'longBusinessSummary', 'currency', 'revenueGrowth', 'logo_url', 'sector', 'exchangeTimezoneShortName', 'beta3Year', 'averageVolume10days', 'navPrice', 'exchangeTimezoneName', 'profitMargins', 'totalCash', 'priceToBook', 'lastDividendValue', 'heldPercentInsiders', 'dayLow', 'sharesShort', 'enterpriseToEbitda', 'state', 'regularMarketOpen', 'openInterest', 'exDividendDate', 'lastDividendDate', 'fiftyTwoWeekLow', 'strikePrice', 'ebitdaMargins', 'shortName', 'enterpriseToRevenue', 'fax']

        ## Selected KPIs excluding usd 
        kpilistselect1 = ['updated_datetime', 'ticker',
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
        'updated_datetime', 'ticker',
        'shortName', 'longBusinessSummary','symbol', 'sector', 'industry', 'country', 'marketCap',  
        'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'revenuePerShare',
        'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
        'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'earningsGrowth', 'priceToSalesTrailing12Months', 
        'trailingEps', 'forwardEps', 
        'pegRatio', 'trailingPegRatio',
        'currentRatio', 'quickRatio', 'debtToEquity', 
        'bookValue', 'enterpriseValue', 'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
        'freeCashflow', 'operatingCashflow', 
        'totalRevenue', 'grossProfits', 'ebitda', 'totalDebt', 'beta',
        'currency', 'financialCurrency',
        'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated',
        'dividendYield', 'dividendRate',
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
# ######## Export datetime for all datasets into google sheets  #################
# ###############################################################################
# python -c 'from equity_exp import exp_dataset_datetime_gs; exp_dataset_datetime_gs()'

def exp_dataset_datetime_gs():

    collection_list = ['equity_daily_kpi','equity_financials','equity_price_history']
    tz_SG = pytz.timezone('Singapore')
    datetime_SG = datetime.now(tz_SG).strftime('%Y-%m-%d %H:%M:%S')
    datetime_SG_gs = [datetime_SG]
    sheetinfo = "Datetime"
    #clear spreadsheets of old data before inserting new ones
    service.spreadsheets().values().clear(spreadsheetId=REQUIRED_SPREADSHEET_ID, range=sheetinfo+'!A2:H50').execute()

    try:
        for col in collection_list:

            # collection = 'equity_daily_kpi'
            docs = db.collection(col).order_by("updated_datetime", direction=firestore.Query.DESCENDING).stream()
            #input selected KPIs to analyse    
            kpi1USD = ['ticker','updated_datetime']

            main_dic = {}
            for doc in docs:
                kpi_dic = {}

                for j in kpi1USD:
                    try:
                        kpi_dic[j] = doc._data[j]
                    except:
                        kpi_dic[j] = ""

                main_dic[doc._data['ticker']] = kpi_dic

            df = pd.DataFrame(main_dic)
            df = df.transpose()
            df['rounded_updated_datetime'] = df['updated_datetime'].dt.round('60min')  
            df = df.drop('updated_datetime', axis=1)
            df = df.groupby(['rounded_updated_datetime']).count()#.sort_values('updated_datetime', ascending=False)
            df = df.reset_index()
            ##converting into google sheets acceptable format
            df['rounded_updated_datetime'] = df['rounded_updated_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

            dflist = df.values.tolist()
            dfcolname = df.columns.tolist()

            sheetinfo = "Datetime"

            if col == 'equity_daily_kpi':
                range1 = "!A3"
                range2 = "!A2"
                range3 = "!B1"

            if col == 'equity_financials':
                range1 = "!D3"
                range2 = "!D2"
                range3 = "!E1"

            if col == 'equity_price_history':
                range1 = "!G3"
                range2 = "!G2"
                range3 = "!H1"

            request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
                range=sheetinfo+range1, valueInputOption="USER_ENTERED", body={"values":dflist}).execute()

            request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
                range=sheetinfo+range2, valueInputOption="USER_ENTERED", body={"values":[dfcolname]}).execute()

            request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
                range=sheetinfo+range3, valueInputOption="USER_ENTERED", body={"values":[datetime_SG_gs]}).execute()

            print (col, " done!")

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)


# ###############################################################################
# ######## Export datetime for fx update into google sheets  ####################
# ###############################################################################
# python -c 'from equity_exp import exp_fx_datetime_gs; exp_fx_datetime_gs()'
def exp_fx_datetime_gs():

    try:

        docs = db.collection(u'fx').get()
        curr_list = []
        for i in docs:
            curr_list.append(i._data['currency'])

        docs = db.collection(u'fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(3).stream()

        curr_dict = {}

        for i in docs:
            currencies = list(i._data['currencyrates'].keys())
            # currencies = i._data['currencyrates'].keys()
            year = i._data['datetime_format'].year
            month = i._data['datetime_format'].month
            day = i._data['datetime_format'].day
            hour = i._data['datetime_format'].hour
            minute = i._data['datetime_format'].minute
            tzinfo = i._data['datetime_format'].tzinfo
            date = datetime(year, month, day, hour, minute, tzinfo=tzinfo).strftime('%Y-%m-%d')
            curr_dict[date] = currencies


        dataset_dict = {}
        for i in curr_dict:
            data_dict = {}
            for curr in curr_list:
                if curr in curr_dict[i]:
                    data_dict[curr] = "yes"
                else:
                    data_dict[curr] = "no"

            dataset_dict[i] = data_dict

        df = pd.DataFrame.from_dict(dataset_dict)

        # print (df)

        dfindex = []
        for i in df.index:
            dfindex.append([i])
        dflist = df.values.tolist()
        dfcolname = df.columns.tolist()

        sheetinfo = "Datetime"

        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!K3", valueInputOption="USER_ENTERED", body={"values":dflist}).execute()

        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!K2", valueInputOption="USER_ENTERED", body={"values":[dfcolname]}).execute()

        request = sheet.values().update(spreadsheetId=REQUIRED_SPREADSHEET_ID, 
            range=sheetinfo+"!J3", valueInputOption="USER_ENTERED", body={"values":dfindex}).execute()

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on macrokpi project"
        content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
        error_email(subject, content)












# ################################################################################################################
# ######## Checking the last extracted ticker - TO CONFIRM EXTRACTION SCRIPTS ARE RUNNING ########################
# ################################################################################################################
# python -c 'from equity_exp import imported_data_details_check; imported_data_details_check()'

def imported_data_details_check():

    dataset_list = ['equity_daily_kpi', 'equity_financials', 'equity_price_history']

    for j in dataset_list:

        docs = db.collection(j).order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(1).stream()

        for i in docs:
            datetime = i.to_dict()['updated_datetime'].strftime('%Y-%m-%d %H:%M:%S')
            ticker = i.to_dict()['ticker']
            print ("Latest Dataset: " + j + " Ticker: " + ticker + " Downloaded Time: " + str(datetime))

    dataset_list = ['equity_daily_kpi', 'equity_financials', 'equity_price_history']

    for j in dataset_list:

        docs = db.collection(j).order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(1).stream()

        for i in docs:
            datetime = i.to_dict()['updated_datetime'].strftime('%Y-%m-%d %H:%M:%S')
            ticker = i.to_dict()['ticker']
            print ("Earliest Dataset: " + j + " Ticker: " + ticker + " Downloaded Time: " + str(datetime))

    # sort the latest data in fx
    docs = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(1).get()
    print ("Latest FX Dataset: " + docs[0]._data['datetime_format'].strftime('%Y-%m-%d'))

    # sort the earliest data in country
    docs = db.collection('equity_daily_country').order_by("daily_agg_record_time", direction=firestore.Query.ASCENDING).limit(1).get()
    print ("Earliest Country Dataset: " + docs[0]._data['daily_agg_record_time'].strftime('%Y-%m-%d'))

    # sort the earliest data in industry
    docs = db.collection('equity_daily_industry').order_by("daily_agg_record_time", direction=firestore.Query.ASCENDING).limit(1).get()
    print ("Earliest Industry Dataset: " + docs[0]._data['daily_agg_record_time'].strftime('%Y-%m-%d'))

    # sort the earliest data in ranking
    docs = db.collection('equity_daily_agg').order_by("daily_industry_rank_updated_datetime", direction=firestore.Query.ASCENDING).limit(1).get()
    print ("Earliest Daily Ranking Dataset: " + docs[0]._data['daily_industry_rank_updated_datetime'].strftime('%Y-%m-%d'))

    # sort the earliest data in aggregated
    docs = db.collection('equity_daily_agg').order_by("daily_industry_agg_updated_datetime", direction=firestore.Query.ASCENDING).limit(1).get()
    print ("Earliest Daily Aggregation Dataset: " + docs[0]._data['daily_industry_agg_updated_datetime'].strftime('%Y-%m-%d'))
