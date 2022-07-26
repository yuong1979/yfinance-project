# from datetime import datetime, timedelta
# from google.oauth2 import service_account
# from google.cloud.firestore import Client
# from secret import access_secret
# from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
# import pandas as pd
# import json
# import pandas as pd
# import numpy as np
# import pytz
# from cmath import nan
# from firebase_admin import firestore
# import time
# from tools import decide_extraction
# import inspect
# from tools import error_email
# import os
# from google.cloud import storage
# from secret import access_secret
# from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key, cloud_storage_key


# firestore_api_key = access_secret(firestore_api_key, project_id)
# firestore_api_key_dict = json.loads(firestore_api_key)
# fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
# db = Client(firebase_database, fbcredentials)


# #only extract when 24 hours has passed, if not dont need
# extraction_time_target = 24

# ###################################################################################################################################
# ####### Computing the industry median and sum across equities and dumping into equity daily industry collection ###################
# ###################################################################################################################################
# # python -c 'from equity_aggregate import update_industry_aggregates; update_industry_aggregates()'

# #DESCENDING
# agg_collection = 'equity_daily_kpi'
# def update_industry_aggregates():


#     try:
            
#         # select the category
#         category='industry'

#         decide_extraction(extraction_time_target, 'equity_daily_' + category, 'daily_agg_record_time')

#         docs = db.collection(agg_collection).stream()
#         #Kpis that are inside the kpi field
#         kpi1 = [
#             'industry','sector','country', 
#             'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
#             'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
#             'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
#             'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
#             'pegRatio', 'trailingPegRatio',
#             'trailingEps', 'forwardEps', 
#             'currentRatio', 'quickRatio', 'debtToEquity',
#             'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate',
#             'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated', 'fullTimeEmployees'
#             ]
#         #Kpis that are inside the main field
#         kpi1USD = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD']

#         main_dic = {}
#         for doc in docs:
#             kpi_dic = {}
#             for j in kpi1:
#                 try:
#                     kpi_dic[j] = doc._data['kpi'][j]
#                 except:
#                     kpi_dic[j] = ""

#             for j in kpi1USD:
#                 try:
#                     kpi_dic[j] = doc._data[j]
#                 except:
#                     kpi_dic[j] = ""

#             main_dic[doc._data['ticker']] = kpi_dic

#         df = pd.DataFrame(main_dic)
#         df = df.transpose()

#         # df.to_csv('testdata.csv')
#         # df = pd.read_csv('testdata.csv')
    
#         df_unique = getattr(df , category).unique()
#         df_unique = pd.DataFrame(df_unique)
#         df_unique = df_unique.rename(columns={0:category})


#         # Calculating Sum for selected categories
#         values = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD', 'fullTimeEmployees']

#         for i in values:

#             df_data = pd.DataFrame(df, columns = [category, i])
#             df_data = df_data[(df_data[category] != "") & (df_data[i] != "") & (df_data[i] != 0)]
#             df_data = df_data.groupby([category])[i].sum()
#             df_data = df_data.reset_index(name = i)
#             # df_data = df_data.rename(columns={ i: "Sum_" + str(i) })

#             #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
#             try:
#                 df_merged_sum = pd.merge(df_merged_sum, df_data, how='left', left_on = category, right_on = category)
#             except:
#                 df_merged_sum = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)


#         # Calculating Median for selected categories
#         values = [
#                 'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
#                 'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
#                 'currentRatio', 'quickRatio', 'debtToEquity',
#                 'heldPercentInsiders', 'heldPercentInstitutions',
#                 # new ones
#                 'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
#                 'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
#                 'pegRatio', 'trailingPegRatio',
#                 'trailingEps', 'forwardEps', 
#                 'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate'
#                 ]


#         for j in values:

#             df_data = pd.DataFrame(df, columns = [category, j])
#             df_data = df_data[(df_data[category] != "") & (df_data[j] != "") & (df_data[j] != 0)]
#             df_data = df_data.groupby([category])[j].median()
#             df_data = df_data.reset_index(name = j)
#             # df_data = df_data.rename(columns={ j: "Median_" + str(j) })
#             #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
#             try:
#                 df_merged_median = pd.merge(df_merged_median, df_data, how='left', left_on = category, right_on = category)
#             except:
#                 df_merged_median = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

#         # print (df_merged_median[['industry','currentRatio']].sort_values('industry', ascending=True).head(50))

#         # Merging all the data into one dataframe
#         df_merged_all = pd.merge(df_merged_sum, df_merged_median, how='left', left_on = category, right_on = category)


#         # including the number of companies in the unique dataframe
#         df_company_count = df.groupby('industry').count()['sector']
#         df_merged_all = pd.merge(df_merged_all, df_company_count, how='left', left_on = category, right_on = category)
#         df_merged_all = df_merged_all.rename(columns={'sector':'company_count'})

#         df = df_merged_all.set_index(category)

#         # consolidating all into a list for iterating and injecting into firestore    
#         cols = df.columns.to_list()
#         tz_SG = pytz.timezone('Singapore')

#         recordtime = datetime.now(tz_SG)
#         df['daily_agg_record_time'] = recordtime

#         # fill all the blanks with na
#         df.fillna('', inplace=True)

#         df.reset_index(inplace=True)

#         # print (df)
#         #upload data data folder as pickle
#         df.to_pickle('data/eq_daily_industry.pickle')

#         #upload to gcp cloud storage
#         key = access_secret(cloud_storage_key, project_id)
#         key = str(key)
#         storage_client = storage.Client(key)
#         #pushing a file on gcp bucket
#         filename= 'eq_daily_industry.pickle'
#         UPLOADFILE = os.path.join(os.getcwd() + "/data", filename)
#         bucket = storage_client.get_bucket('test_cloud_storage_bucket_blockmacro')
#         blob = bucket.blob(filename)
#         blob.upload_from_filename(UPLOADFILE)

#         #######################################################################################
#         ################### consider that this below might no longer be neccessary#############
#         #######################################################################################

#         # # inserting the data into the equity industry collection
#         # collection = 'equity_daily_' + category
#         # for index, row in df.iterrows():
#         #     recordtime = datetime.now(tz_SG)
#         #     data = {
#         #         'daily_agg_record_time' : recordtime
#         #     }
#         #     dataset = {}
#         #     for i in cols:
#         #         categories = index
#         #         data_dict = {
#         #             i : row[i]
#         #         }
#         #         dataset.update(data_dict)
#         #     data['daily_agg'] = dataset

#         #     # print (categories, data)
#         #     try:
#         #         # when there are no categories, stop and exit without inserting data
#         #         db.collection(collection).document(categories).set(data, merge=True)
#         #     except:
#         #         pass

#     except AttributeError as e:
#         print (e)

#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on updating industry aggregates"
#         try:
#             ticker = ticker
#         except:
#             ticker = "None"
#         content = "Error in \n File name: "+ str(file_name) \
#              + "\n Function: " + str(function_name) \
#               + "\n Detailed error: " + str(e) \
#                 + "\n Error data: " + str(ticker)

#         error_email(subject, content)

# ####################################################################################################################
# ################### dumping the median/sum industry data into equity_daily_agg #####################################
# ####################################################################################################################
# # python -c 'from equity_aggregate import insert_industry_agg_data; insert_industry_agg_data()'



# # ASCENDING - earliest date first
# # DESCENDING - latest date first
# def insert_industry_agg_data():


#     try:

#         # decide_extraction(extraction_time_target, 'equity_daily_agg', 'daily_industry_agg_updated_datetime')

#         tz_SG = pytz.timezone('Singapore')

#         e_docs = db.collection('equity_daily_agg').order_by("daily_industry_agg_updated_datetime", direction=firestore.Query.ASCENDING).stream()

#         x = 0
#         for i in e_docs:
#             ticker = i._data['ticker']
#             industry = i._data['industry']

#             try:
#                 ind_data = db.collection('equity_daily_industry').document(industry).get()
#                 ind_data = ind_data._data['daily_agg']
#             except:
#                 ind_data = ""

#             recordtime = datetime.now(tz_SG)
#             data = {
#                 'industry': industry,
#                 'industry_data': ind_data,
#                 'daily_industry_agg_updated_datetime': recordtime
#             }

#             # print (data)
#             db.collection('equity_daily_agg').document(i.id).set(data, merge=True)
#             print (x, ticker)
#             x = x + 1

#     except AttributeError as e:
#         print (e)

#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on dumping industry aggregates into equity_daily_agg"
#         try:
#             ticker = ticker
#         except:
#             ticker = "None"
#         content = "Error in \n File name: "+ str(file_name) \
#              + "\n Function: " + str(function_name) \
#               + "\n Detailed error: " + str(e) \
#                 + "\n Error data: " + str(ticker)

#         error_email(subject, content)
















# ###################################################################################################################################
# ####### Computing the country median and sum across equities and dumping into equity daily country collection #####################
# ###################################################################################################################################
# # python -c 'from equity_aggregate import update_country_aggregates; update_country_aggregates()'

# #DESCENDING
# agg_collection = 'equity_daily_kpi'
# def update_country_aggregates():

#     try:

#         # select the category
#         category='country'

#         # decide_extraction(extraction_time_target, 'equity_daily_' + category, 'daily_agg_record_time')

#         docs = db.collection(agg_collection).stream()
#         #Kpis that are inside the kpi field
#         kpi1 = [
#             'industry','sector','country', 
#             'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
#             'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
#             'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
#             'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
#             'pegRatio', 'trailingPegRatio',
#             'trailingEps', 'forwardEps', 
#             'currentRatio', 'quickRatio', 'debtToEquity',
#             'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate',
#             'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated', 'fullTimeEmployees'
#             ]

#         #Kpis that are inside the main field
#         kpi1USD = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD']

#         main_dic = {}
#         for doc in docs:
#             kpi_dic = {}
#             for j in kpi1:
#                 try:
#                     kpi_dic[j] = doc._data['kpi'][j]
#                 except:
#                     kpi_dic[j] = ""

#             for j in kpi1USD:
#                 try:
#                     kpi_dic[j] = doc._data[j]
#                 except:
#                     kpi_dic[j] = ""

#             main_dic[doc._data['ticker']] = kpi_dic

#         df = pd.DataFrame(main_dic)
#         df = df.transpose()

#         df_unique = getattr(df , category).unique()
#         df_unique = pd.DataFrame(df_unique)
#         df_unique = df_unique.rename(columns={0:category})

#         # Calculating Sum for selected categories
#         values = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD', 'fullTimeEmployees']

#         for i in values:

#             df_data = pd.DataFrame(df, columns = [category, i])
#             df_data = df_data[(df_data[category] != "") & (df_data[i] != "") & (df_data[i] != 0)]
#             df_data = df_data.groupby([category])[i].sum()
#             df_data = df_data.reset_index(name = i)
#             df_data = df_data.rename(columns={ i: "Sum_" + str(i) })
#             #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
#             try:
#                 df_merged_sum = pd.merge(df_merged_sum, df_data, how='left', left_on = category, right_on = category)
#             except:
#                 df_merged_sum = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

#         # Calculating Median for selected categories
#         values = ['grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
#                 'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
#                 'currentRatio', 'quickRatio', 'debtToEquity',
#                 'heldPercentInsiders', 'heldPercentInstitutions',

#                 # new ones
#                 'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
#                 'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
#                 'pegRatio', 'trailingPegRatio',
#                 'trailingEps', 'forwardEps', 
#                 'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate'
#         ]


#         for j in values:

#             df_data = pd.DataFrame(df, columns = [category, j])
#             df_data = df_data[(df_data[category] != "") & (df_data[j] != "") & (df_data[j] != 0)]
#             df_data = df_data.groupby([category])[j].median()
#             df_data = df_data.reset_index(name = j)
#             df_data = df_data.rename(columns={ j: "Median_" + str(j) })
#             #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
#             try:
#                 df_merged_median = pd.merge(df_merged_median, df_data, how='left', left_on = category, right_on = category)
#             except:
#                 df_merged_median = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

#         # print (df_merged_median[['industry','currentRatio']].sort_values('industry', ascending=True).head(50))

#         # Merging all the data into one dataframe
#         df_merged_all = pd.merge(df_merged_sum, df_merged_median, how='left', left_on = category, right_on = category)

#         df = df_merged_all.set_index(category)

#         # consolidating all into a list for iterating and injecting into firestore    
#         cols = df.columns.to_list()

#         tz_SG = pytz.timezone('Singapore')


#         print (df)
#         df.to_pickle('country_data_test.pickle')




#         # # inserting the data into the equity industry collection
#         # collection = 'equity_daily_' + category
#         # for index, row in df.iterrows():
#         #     recordtime = datetime.now(tz_SG)
#         #     data = {
#         #         'daily_agg_record_time' : recordtime
#         #     }
#         #     dataset = {}
#         #     for i in cols:
#         #         categories = index
#         #         data_dict = {
#         #             i : row[i]
#         #         }
#         #         dataset.update(data_dict)
#         #     data['daily_agg'] = dataset
#         #     # print (categories, data)
#         #     try:
#         #         # when there are no categories, stop and exit without inserting data
#         #         db.collection(collection).document(categories).set(data, merge=True)
#         #     except:
#         #         pass

#     except AttributeError as e:
#         print (e)

#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on updating country aggregates"
#         try:
#             ticker = ticker
#         except:
#             ticker = "None"
#         content = "Error in \n File name: "+ str(file_name) \
#              + "\n Function: " + str(function_name) \
#               + "\n Detailed error: " + str(e) \
#                 + "\n Error data: " + str(ticker)

#         error_email(subject, content)








# # ##################################################################################################
# # ################### To create a clean equity_daily_agg database ##################################
# # ##################################################################################################
# # # python -c 'from equity_aggregate import delele_unwanted; delele_unwanted()'

# # def delele_unwanted():
# #     tz_SG = pytz.timezone('Singapore')
# #     collection = "equity_daily_agg"


# #     docs = db.collection(collection).order_by("daily_industry_agg_updated_datetime", direction=firestore.Query.ASCENDING).stream()

# #     # fieldtodelete1 = 'industry_data.daily_agg.Median_enterpriseToRevenuepegRatio'
# #     # fieldtodelete2 = 'industry_data.daily_agg_record_time'

# #     # db.collection('equity_daily_agg').document(i.id).set(data, merge=True)

# #     fieldtodelete = 'industry_data'


# #     for doc in docs:
# #         key = doc.id
# #         print (doc._data['ticker'])
# #         recordtime = datetime.now(tz_SG)

# #         data = {
# #             'daily_industry_agg_updated_datetime': recordtime
# #         }

# #         db.collection(collection).document(key).update({fieldtodelete: firestore.DELETE_FIELD})
# #         db.collection('equity_daily_agg').document(key).set(data, merge=True)



# # ##################################################################################################
# # ################### To create a clean equity_daily_agg database ##################################
# # ##################################################################################################
# # # python -c 'from equity_aggregate import creating_equity_daily_agg; creating_equity_daily_agg()'

# # # DESCENDING
# # # ASCENDING

# # #ASCENDING or #DESCENDING
# # def creating_equity_daily_agg():
# #     tz_SG = pytz.timezone('Singapore')
# #     migrate_from = 'equity_list'
# #     migrate_to = 'equity_daily_agg'

# #     tickerlist = db.collection(migrate_from).order_by("updated_datetime", direction=firestore.Query.DESCENDING).stream()

# #     x=1
# #     for tick in tickerlist:
# #         data_dict = {}
# #         recordtime = datetime.now(tz_SG)

# #         ticker = tick._data['ticker']
# #         tickername = tick._data['tickername']
# #         # print (tick._data('tickername'))

# #         data = {
# #             'activated': True,
# #             'ticker': ticker,
# #             'tickername': tickername,
# #             'industry': '',
# #             'industry_data': '',
# #             'ranking_data': '',
# #             'daily_industry_rank_updated_datetime': recordtime,
# #             'daily_industry_agg_updated_datetime': recordtime
# #         }
        
# #         db.collection(migrate_to).document(tick.id).set(data, merge=True)
# #         print (str(x) + " done")
# #         x = x + 1







# # # ##########################################################################################################################
# # # ######### To test csv - this works - meaning that csv files can be used in cloud run to aid in intensive compute #########
# # # ##########################################################################################################################
# # # # python -c 'from equity_aggregate import test_csv; test_csv()'
# # from random import random

# # def test_csv():

# #     df_dict = {}
# #     x = 0
# #     # generate random numbers between 0-1
# #     for _ in range(5):
# #         value = random()
# #         x = x + 1
# #         df_dict[x] = value

# #     df = pd.DataFrame.from_dict(df_dict, orient='index', columns=["test"])
# #     # print (df)

# #     df.to_csv('dataframes/test.csv')

# ###### insert the below into flask main.py to test the function #########
#     # test_csv()

#     # sample_csv_data_list = []
#     # with open(filepath) as file:
#     #     csv_file = csv.reader(file)
#     #     for row in csv_file:
#     #         sample_csv_data_list.append(row)
