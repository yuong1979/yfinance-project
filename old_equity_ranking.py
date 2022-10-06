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

# firestore_api_key = access_secret(firestore_api_key, project_id)
# firestore_api_key_dict = json.loads(firestore_api_key)
# fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
# db = Client(firebase_database, fbcredentials)


# #only extract when 24 hours has passed, if not dont need
# extraction_time_target = 24






# ######################################################################################################################
# ####### Extracts data from equity daily kpi, converts them into dataframe, dump them into csv for ranking ############
# ######################################################################################################################
# # python -c 'from equity_ranking import equity_kpi_ranker; equity_kpi_ranker()'


# def equity_kpi_ranker():

#     try:

#         #check if the csv data is up to date, if not need to run equity_kpi_ranker again
#         tz_SG = pytz.timezone('Singapore')
#         recordtime = datetime.now(tz_SG).strftime("%Y-%m-%d")
#         try:
#             df = pd.read_csv('dataframes/'+ recordtime + '.csv')
#             exit()
#         except:
#             #if there is an error with detecting the csv than proceed with the creating the dataframe
#             pass


#         values_1 = ['grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
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


#         ranking_criteria_1 = ['pos','pos','pos','pos',
#                             'pos','pos','pos','pos',
#                             'pos','pos','neg',
#                             'pos','pos',
#                             # new ones
#                             'neg','neg','pos','neg',
#                             'neg','neg','neg',
#                             'neg','neg',
#                             'pos','pos',
#                             'pos','pos','pos','pos','pos'
#                             ]

#         values_2 = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD']
#         ranking_criteria_2 = ['pos','pos','pos']


#         df = pd.DataFrame(columns = ['industry', 'ticker', 'kpi', 'ranking_criteria', 'value'])
#         industry_docs = db.collection('equity_daily_industry').get()
#         total_count = len(industry_docs)
#         count = 0
#         for i in industry_docs:
            
#             industry_name = i.id
#             count = count + 1
#             equity_docs = db.collection('equity_daily_kpi').where('industry' , '==' , industry_name).stream()
#             for j in equity_docs:
#                 # For loading value set 1
#                 x = 0
#                 for k in values_1:
                    
#                     try:
#                         value = j._data['kpi'][k]
#                     except:
#                         value = None

#                     print (str(count) + "/" + str(total_count) + " " + industry_name, j._data['ticker'],k, ranking_criteria_1[x],  value)
#                     # append rows to an empty DataFrame
#                     df = pd.concat([df, pd.DataFrame.from_records([{ 
#                                     'industry' : industry_name, 
#                                     'ticker' : j._data['ticker'], 
#                                     'kpi' : k, 
#                                     'ranking_criteria' : ranking_criteria_1[x], 
#                                     'value' : value
#                         }])])

#                     x = x + 1

#                 # For loading value set 2
#                 x = 0
#                 for k in values_2:
#                     print (str(count) + "/" + str(total_count) + " " + industry_name, j._data['ticker'],k, ranking_criteria_2[x],  j._data[k])
#                     # append rows to an empty DataFrame
#                     df = pd.concat([df, pd.DataFrame.from_records([{ 
#                                     'industry' : industry_name, 
#                                     'ticker' : j._data['ticker'], 
#                                     'kpi' : k, 
#                                     'ranking_criteria' : ranking_criteria_2[x], 
#                                     'value' : j._data[k]
#                         }])])
#                     x = x + 1


#         print ("data to be ranked migrated successfully to dataframe")


#         # # print (df)
#         # df.to_csv('testdata.csv')

#         tz_SG = pytz.timezone('Singapore')
#         # recordtime = '2022-08-31'
#         recordtime = datetime.now(tz_SG).strftime("%Y-%m-%d")
#         filelist = [ f for f in os.listdir('dataframes/') if f.endswith(".csv") ]
#         # delete files inside the dataframe folder before creating a another new csv
#         for f in filelist:
#             os.remove(os.path.join('dataframes/', f))
#         df.to_csv('dataframes/'+ recordtime + '.csv')




#     except AttributeError as e:
#         print (e)

#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on ranking updates"
#         try:
#             ticker = ticker
#         except:
#             ticker = "None"
#         content = "Error in \n File name: "+ str(file_name) \
#              + "\n Function: " + str(function_name) \
#               + "\n Detailed error: " + str(e) \
#                 + "\n Error data: " + str(ticker)

#         error_email(subject, content)




# ######################################################################################################################
# ####### Data manipulation to rank the equities and dumps ranked data into equity_daily_agg ###########################
# ######################################################################################################################
# # python -c 'from equity_ranking import insert_industry_rank_data; insert_industry_rank_data()'



# def insert_industry_rank_data():

#     try:
#         #check if the csv data is up to date, if not need to run equity_kpi_ranker again
#         tz_SG = pytz.timezone('Singapore')
#         recordtime = datetime.now(tz_SG).strftime("%Y-%m-%d")
#         try:
#             df = pd.read_csv('dataframes/'+ recordtime + '.csv')
#         except:
#             # rerun function then exit to wait for the function to be run successfully through cloud schedule
#             equity_kpi_ranker()
#             exit()

#         decide_extraction(extraction_time_target, 'equity_daily_agg', 'daily_industry_rank_updated_datetime')
#         # replace with na if the value of the ratio is zero
#         df['value'] = df['value'].apply(lambda x: nan if x == 0 else x)

#         # df.drop(['Unnamed: 0'], axis=1, inplace=True)

#         unique_industry_list = df['industry'].unique()
#         unique_kpi_list = df['kpi'].unique()

#         final_df = pd.DataFrame(columns = ['ticker','industry', 'kpi', 'ranking_criteria', 'value', 'ranked', 'count_co_ind', 'rank', 'rank_%'])

#         for i in unique_industry_list:
#             for j in unique_kpi_list:
#                 ticker_df = df.loc[ (df['industry'] == i) & (df['kpi'] == j) , ["ticker", "industry", "kpi", "ranking_criteria", "value"]]
#                 # unique_ind_ticker_list = ticker_df['ticker'].unique()
#                 ranking_criteria = ticker_df.ranking_criteria.values[0]

#                 if ranking_criteria == "pos":
#                     ticker_df['ranked'] = ticker_df['value'].rank(ascending=False)
#                 else:
#                     ticker_df['ranked'] = ticker_df['value'].rank(ascending=True)

#                 # count of all tickers with valid values in a specific industry
#                 count = ticker_df[ticker_df.columns[4]].values
#                 counter = count.size - np.count_nonzero(np.isnan(count))
#                 ticker_df['count_co_ind'] = counter

#                 # concatenating the columns to get the fraction format
#                 ticker_df["rank"] = ticker_df["ranked"].apply(lambda x: "{:.0f}".format(x)).astype(str) + " / " + ticker_df["count_co_ind"].astype(str)
                
#                 #replacing all invalid columns in rank with "nan"
#                 ticker_df["rank"] = ticker_df["rank"].apply(lambda x: nan if x.__contains__('nan') else x)

#                 #rank% to create another perspective to rank equities
#                 ticker_df["rank_%"] = 1 - (ticker_df["ranked"] / ticker_df["count_co_ind"])

#                 # ticker_df.drop(['count_co_ind', 'ranked'], axis=1, inplace=True)

#                 final_df = pd.concat([final_df, ticker_df], axis=0)

#         final_df.reset_index(inplace=True, drop=True)
#         final_df.sort_values(by=['ticker'], inplace=True)

#         #counting the number of nulls contained all in the total dataset
#         count_null = final_df['rank_%'].isna().sum()
#         count_all = final_df.shape[0]
#         # print (count_null/count_all)

#         # print (final_df)
#         # final_df.to_csv('final_data.csv')


#         print ("data successfully ranked")



#         ########################################################
#         # dumping the ranked kpi data into equity_daily_agg ####
#         ########################################################

#         unique_kpi_list = final_df['kpi'].unique()

#         tz_SG = pytz.timezone('Singapore')
        
#         docs = db.collection('equity_daily_agg').order_by("daily_industry_rank_updated_datetime", direction=firestore.Query.ASCENDING).stream()

#         count = 0
#         for ticker in docs:

#             recordtime = datetime.now(tz_SG)
#             tickername = ticker._data['ticker']

#             data = {}

#             rankdataset = {}
#             for kpi in unique_kpi_list:
                
#                 kpis_required = final_df.loc[ (final_df['ticker'] == tickername) & (final_df['kpi'] == kpi) , ["rank", "rank_%"]]

#                 try:
#                     rank = kpis_required.values[0][0]
#                     rank_percentage = kpis_required.values[0][1]
#                 except:
#                     rank = ""
#                     rank_percentage = ""

#                 rank_kpi = kpi + " rank_kpi"
#                 rank_percentage_kpi = kpi + " rank_percentage_kpi"

#                 rankdataset[rank_kpi] = rank
#                 rankdataset[rank_percentage_kpi] = rank_percentage

#             data['ranking_data'] = rankdataset
#             data['daily_industry_rank_updated_datetime'] = recordtime
#             data['activate'] = True

#             db.collection('equity_daily_agg').document(ticker.id).set(data, merge=True)

#             print (str(count) , tickername)
#             count = count + 1
            
#         print ("dumped ranked data into equity_calc")

#     except AttributeError as e:
#         print (e)

#     except Exception as e:
#         print (e)
#         file_name = __name__
#         function_name = inspect.currentframe().f_code.co_name
#         subject = "Error on dumping ranking into equity_daily_agg"
#         try:
#             ticker = ticker
#         except:
#             ticker = "None"
#         content = "Error in \n File name: "+ str(file_name) \
#              + "\n Function: " + str(function_name) \
#               + "\n Detailed error: " + str(e) \
#                 + "\n Error data: " + str(ticker)

#         error_email(subject, content)


