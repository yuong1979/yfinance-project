import yfinance as yf
import pandas as pd
import numpy as np
# import seaborn as sb
# import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

#sample information

companyticker = yf.Ticker("meta")
# print (companyticker.recommendations['To Grade'].value_counts())
# print (companyticker.financials)


# #extracting financials
# df = companyticker.financials
# df.columns = pd.to_datetime(df.columns).strftime('%Y')
# kpilist = ['Total Revenue', 'Net Income', 'Operating Income', 'Ebit']
# df = df.loc[kpilist,]
# print (df)


# #extracting historical price
# date_today = datetime.today().replace(microsecond=0)
# startdate = date_today - relativedelta(years=5)
# enddate = date_today - timedelta(days=1)
# pricehistory = companyticker.history(start=startdate,  end=enddate)
# # choose only closing date
# pricehistory = pricehistory.iloc[:, [3]]
# data = pricehistory.loc[pricehistory.index.day==1]
# print (data)




# # current information
# msft = yf.Ticker("msft")

# print (msft.info['industry'])

# # price history
# hist = msft.history(period="max")
# print (hist)

# # recommendations
# print (msft.recommendations['To Grade'].value_counts())

# # ytd details
# print (msft.info)

# # ytd details
# print (msft.info)

# # stock splits and dividends
# print (msft.actions)

# # balance sheet
# print (msft.balancesheet)

# # pnl - detailed
# print (msft._financials)

# # pnl - summarized
# print (msft.financials)

# # pnl - cashflow
# print (msft.cashflow)

# # revenue and earnings - last 4 years
# print (msft.earnings)

# # eps since listing
# print (msft.earnings_history)








