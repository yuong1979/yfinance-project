from firebase_admin import firestore
import pandas as pd
import numpy as np

db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")

#### export required data to google sheets ######

docs = db.collection('tickerlisttest').stream()

datalist = []
for i in docs:
    datalist.append(i._data['kpi'])


df = pd.DataFrame(datalist)


df.replace(np.nan, '', inplace=True)

# listing all the columns
print (df.columns)

## presenting the df only with the columns desired and rows 0 to 3
# print (df[['symbol', 'sector']][0:3])

# print (df.iloc[0:2])

# #get a rows you want into a list
# for index, row in df.iterrows():
#     print(index, row['symbol'])

# column_names = df.columns.values.tolist()
# print (column_names)

# #get data according to a certain condition
# print (df.loc[df['sector'] == "Technology"])

# # #sorting values by first symbol return on equity
# print (df.sort_values(['symbol', 'returnOnEquity'], ascending=False))


# ## creating a new column with a new formula - issues
# df['diff'] = df['dayHigh'] + df['dayLow'] 

# ## removing a specific column
# df = df.drop(columns=['diff'])

# ## selecting a few columns using numbers
# df = df.iloc[:, 2:10]
# print (df)



