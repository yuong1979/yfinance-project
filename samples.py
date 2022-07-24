import yfinance as yf
import pandas as pd
import numpy as np
# import seaborn as sb
# import matplotlib.pyplot as plt


#sample information

# current information
msft = yf.Ticker("msft")

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

# pnl - summarized
print (msft.financials)

# pnl - cashflow
print (msft.cashflow)

# revenue and earnings - last 4 years
print (msft.earnings)

# # eps since listing
# print (msft.earnings_history)







