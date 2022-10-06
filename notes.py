# secret manager - python tutorial
# https://codelabs.developers.google.com/codelabs/secret-manager-python#8
# if running on local - need to create secret / service manager / api key
# https://cloud.google.com/docs/authentication/getting-started

### bare minimum
# pip install firebase-admin
# pip install Flask
# pip install google-cloud-secret-manager
# pip install pandas
# pip install yfinance
# pip install beautifulsoup4




# yfinance
# Add ranking and median to the incomestatement cashflow and balancesheet by industry
# check that the numbers all tally and add up in the ranking and median of financials
# also include the export of into google storage and import into streamlit


# firebase
# Check why is price fucked up for major stocks

# On stream lit
# move the completed chart to the option page on home




############## Change object to numeric or data types ###########################################
# pd.to_numeric(df['test'])  / pd.to_datetime(df['test']) - refer to these for the various dtypes https://www.youtube.com/watch?v=6qmMrZeX9F4

############## View the correlations in a heatmap ###############################################
# sns.pairplot(df, var=['col1', 'col2', 'col3' , 'col4'], hue='col_w_cat_type')
# see correlations among items
# df_corr = df[['col1','col2','col3']]
# sns.heatmap(df_corr, annot=True)

############## Better way to do queries ##########################################################
# filter df[df['col1']>50] df.query('col1 != "item"') / df.query('Year < 1980 and Time > 10') or df.query('Year < @year and Time > @time')

############## Count the number of values for each unique value ##################################
# value counts - df['col1'].value_counts()

############## Group by ##########################################################################
# df.query('col1 != "other"').groupby('col1')['col2'].agg(['mean','count']).query('count>=10').sort_values('mean')

############## check the numbers of na and details in each column ################################
# df.isna().sum() or df[df['col'].isna()]

############## drop entire rows that have missing and na values in them  #########################
# df.dropna(axis="index" how='any') - if you want to drop rows with all na on the rows you can use "NA" instead of any

############## replace items with bad information  ###############################################
# df.replace('NA', np.nan, inplace=True) or df.replace('Missing', np.nan, inplace=True)

############## fill all na cells with 'missing'  #################################################
# df.fillna('missing')

############## convert dtypes ####################################################################
# df['age'] = df['age'].astype(float) / df['col'] = df['col'].astype('category') - if you are having issues than do this df['col'].unique() and than df['cols'].replace("error", 0, inplace=True)

############## Rename columns quickly ############################################################
# effective column rename method is df.rename(columns{'Year', 'Race_Year'})

############## output to csv remove the unnamed column ###########################################
# df.to_csv('output.csv', index=False) or pd.read_csv('output.csv', index_col=[0])

############## output to label date before output to pickle ######################################
# df['date'] = pd.to_datetime(df['date'])



# ############## groupby the datatypes of a column #################################################
# type_breakdown = df['industry'].map(type).value_counts()
# ############## summary of datatypes in a dataframe ###############################################
# print (df.info(verbose=True)) / print (df.dtypes)
# ############## Search for null in a column #######################################################
# dftest = df[df['isEsgPopulated'].isnull()]
# ############## iterating through each column for type ############################################
# for i in df['industry']:
#     if isinstance(i, int) == True:
#         print ('integer', i)
#     elif isinstance(i, float) == True:
#         print ('float', i)
#     elif isinstance(i, str) == True:
#         print ('string', i)

# # ############## looping through df ############################################
# for index, row in df.iterrows():
#     print (index, 'index')
#     print (row['marketCapUSD'])
#     print (row['totalRevenueUSD'])
#     print (row['ebitdaUSD'])






# wrap your expressions so you can chain your code - https://youtu.be/_gaAoJBMJ_Q?t=627
# when receiving a value is trying to be set on a copy of a slice of a dataframe - use this to fix - https://youtu.be/_gaAoJBMJ_Q?t=217


# df_industry_sum = pd.read_pickle('data/eq_hist_sum.pickle')
# df = df_industry_sum.groupby(["year", "cattype", "quarter"])['values'].sum()
# df = df.reset_index()
# print (df.tail(60))

# #structure the data such that qtrly cattype will have last date as the quarterly last and the annual cattype will have dec 31 of each year as last date
# #if quarterly data than the last date should be empty, if annual data than then the last date for quarter should be empty


        
