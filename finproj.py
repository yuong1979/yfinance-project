import yfinance as yf
import pandas as pd
import numpy as np
# import seaborn as sb
# import matplotlib.pyplot as plt


#sample information

# current information
msft = yf.Ticker("MSFT")
print (msft.info)

# price history
hist = msft.history(period="max")
print (hist)

# dividends
div = msft.dividends
print (div)

print (msft.earnings)
print (msft.cashflow)
print (msft.balance_sheet)
print (msft.recommendations)
print (msft.recommendations['To Grade'].value_counts())


