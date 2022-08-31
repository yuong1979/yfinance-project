
from secret import access_secret
from google.oauth2 import service_account
from google.cloud.firestore import Client
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
import json


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)

# create a new database equity_historicals_agg
# go through the equity_financials and identify revenue/profit/debt/equity/assets
# go through the equity_financials database to gather all the relevant dates
# with those dates go through and extract price and fx and dump those relevant data into equity_historicals_agg
# dump all those revenue/profit/debt/equity/assets data into equity_historicals_agg as well
# create a new database equity_historicals_industry_agg
# create the functions that aggregate the median sum of the historicals by industry and dump it into equity_historicals_industry_agg
# create the functions that ranks of the equities by their ratio quarterly -> TOUGH => and dump the data into equity_historicals_agg 
# (make sure its another set of data - not in the same set as the raw data)





####################################################################################################################
################### dumping the median/sum (aggregates) industry data into equity_daily_agg ########################
####################################################################################################################
# python -c 'from equity_compute_hist import test; test()'



# Gross Profit
# Net Income
# Operating Income
# Total Revenue
# Ebit

# Total Cashflows From Investing Activities
# Change To Netincome
# Total Cash From Operating Activities
# Net Income
# Change In Cash
# Total Cash From Financing Activities

# Total Current Assets
# Total Stockholder Equity
# Total Current Liabilities
# Total Assets


# ASCENDING
# DESCENDING

# extremely tough to compare like on like because annual closing differ between companies

def test():
    print ("test")

    docs = db.collection('equity_financials').limit(10).stream()

    annual_list = ['balancesheet','cashflow','financials']
    quarter_list = ['quarterly_balancesheet','quarterly_cashflow','quarterly_financials']

    annual_date_list = []
    quarter_date_list = []

    for i in docs:
        # print (i._data['ticker'])

        for j in annual_list:
            dates = i._data['time_series_financials'][j].keys()
            for date in dates:
                if date not in annual_date_list:
                    annual_date_list.append(date)

        for k in quarter_list:
            dates = i._data['time_series_financials'][k].keys()
            for date in dates:
                if date not in quarter_date_list:
                    quarter_date_list.append(date)


    print (annual_date_list)
    print (quarter_date_list)

    # print (i._data['time_series_financials']['quarterly_balancesheet'])
    # print (i._data['time_series_financials']['quarterly_cashflow'])
    # print (i._data['time_series_financials']['quarterly_financials'])
    # print (i._data['time_series_financials']['balancesheet'])
    # print (i._data['time_series_financials']['cashflow'])
    # print (i._data['time_series_financials']['financials'])

    # print (i._data['time_series_financials']['financials'].keys())


    # list of fields required
    # ticker
    # date
    # quarterly/annually
    # q1/q2/q3/q4/y1/y2/y3/y4
    # kpi keys
    # kpi values
    # price of equity - extract from equity_historical_price
    # industry - extract from equity_daily_kpi
    # financial currency - extract from equity_daily_kpi
    # rates on the date - extract from fx_historicals








