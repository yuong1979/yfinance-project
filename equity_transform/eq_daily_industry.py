from firebase_admin import firestore
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
from google.cloud.firestore import Client
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key, cloud_storage_key
from firebase_admin import firestore
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta
from secret import access_secret
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage, convert_digits
import math
import pytz
from google.cloud import storage
import os
import inspect
import numpy as np

firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)




####################################################################################################################################
######## Description: Extracting from firebase the daily ratio kpis and grouping it by industries and then loading pickle into gcp
######## This itself has no prerequisite
####################################################################################################################################


# #only extract when 24 hours has passed, if not dont need
# extraction_time_target = 24

###################################################################################################################################
####### Computing the industry median and sum and rank ############################################################################
###################################################################################################################################
# python -c 'from equity_transform.eq_daily_industry import export_eq_industry; export_eq_industry()'

#DESCENDING
agg_collection = 'equity_daily_kpi'
def export_eq_industry():

    try:
            
        # select the category
        category='industry'

        docs = db.collection(agg_collection).stream()
        #Kpis that are inside the kpi field
        kpi1 = [
            'industry','sector','country', 
            'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
            'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins',
            'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
            'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
            'pegRatio', 'trailingPegRatio',
            'trailingEps', 'forwardEps', 
            'currentRatio', 'quickRatio', 'debtToEquity',
            'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate',
            'heldPercentInsiders', 'heldPercentInstitutions', 'isEsgPopulated', 'fullTimeEmployees'
            ]
        #Kpis that are inside the main field
        kpi1USD = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD']

        main_dic = {}
        for doc in docs:
            kpi_dic = {}
            for j in kpi1:
                try:
                    kpi_dic[j] = doc._data['kpi'][j]
                except:
                    kpi_dic[j] = ""

            for j in kpi1USD:
                try:
                    kpi_dic[j] = doc._data[j]
                except:
                    kpi_dic[j] = ""

            main_dic[doc._data['ticker']] = kpi_dic

        df = pd.DataFrame(main_dic)
        df = df.transpose()

        # #originally industry has zeros in them, replacing it with '' which is a string that matches the rest of the data in the column
        df.replace("", np.nan, inplace=True)
        # because there is data into trailingPD and pricetosalestrailing12months that are named 'infinity
        df.replace("Infinity", np.nan, inplace=True)

        df['industry'].replace(0, "", inplace=True)
        df['industry'].replace(np.nan, "", inplace=True)
        df['sector'].replace(0, "", inplace=True)
        df['sector'].replace(np.nan, "", inplace=True)
        df['country'].replace(0, "", inplace=True)
        df['country'].replace(np.nan, "", inplace=True)

        # df.to_csv('testdata.csv')
        # df = pd.read_csv('testdata.csv')

        df_unique = getattr(df , category).unique()
        df_unique = pd.DataFrame(df_unique)
        df_unique = df_unique.rename(columns={0:category})


        # Calculating Sum for selected categories
        values = ['marketCapUSD', 'totalRevenueUSD', 'ebitdaUSD', 'fullTimeEmployees']

        for i in values:

            df_data = pd.DataFrame(df, columns = [category, i])
            df_data = df_data[(df_data[category] != "") & (df_data[i] != "") & (df_data[i] != 0)]
            df_data = df_data.groupby([category])[i].sum()
            df_data = df_data.reset_index(name = i)
            # df_data = df_data.rename(columns={ i: "Sum_" + str(i) })

            #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
            try:
                df_merged_sum = pd.merge(df_merged_sum, df_data, how='left', left_on = category, right_on = category)
            except:
                df_merged_sum = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)


        # Calculating Median for selected categories
        values = [
                'grossMargins', 'operatingMargins', 'profitMargins',  'ebitdaMargins', 
                'returnOnAssets', 'returnOnEquity', 'revenueGrowth', 'earningsGrowth',
                'currentRatio', 'quickRatio', 'debtToEquity',
                'heldPercentInsiders', 'heldPercentInstitutions',
                # new ones
                'forwardPE', 'trailingPE', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months',
                'priceToBook', 'enterpriseToEbitda', 'enterpriseToRevenue',
                'pegRatio', 'trailingPegRatio',
                'trailingEps', 'forwardEps', 
                'trailingAnnualDividendYield', 'trailingAnnualDividendRate', 'fiveYearAvgDividendYield', 'dividendYield', 'dividendRate'
                ]


        for j in values:

            df_data = pd.DataFrame(df, columns = [category, j])
            df_data = df_data[(df_data[category] != "") & (df_data[j] != "") & (df_data[j] != 0)]
            df_data = df_data.groupby([category])[j].median()
            df_data = df_data.reset_index(name = j)
            # df_data = df_data.rename(columns={ j: "Median_" + str(j) })
            #try except created because the first merge will be missing a merged df, while subsequent ones will be fine because merged is available
            try:
                df_merged_median = pd.merge(df_merged_median, df_data, how='left', left_on = category, right_on = category)
            except:
                df_merged_median = pd.merge(df_unique, df_data, how='left', left_on = category, right_on = category)

        # print (df_merged_median[['industry','currentRatio']].sort_values('industry', ascending=True).head(50))


        # Merging all the data into one dataframe
        df_merged_all = pd.merge(df_merged_sum, df_merged_median, how='left', left_on = category, right_on = category)


        # including the number of companies in the unique dataframe
        df_company_count = df.groupby('industry').count()['sector']
        df_merged_all = pd.merge(df_merged_all, df_company_count, how='left', left_on = category, right_on = category)
        df_merged_all = df_merged_all.rename(columns={'sector':'company_count'})

        df = df_merged_all.set_index(category)

        # consolidating all into a list for iterating and injecting into firestore    
        cols = df.columns.to_list()
        tz_SG = pytz.timezone('Singapore')

        recordtime = datetime.now(tz_SG)
        df['daily_agg_record_time'] = recordtime

        df.reset_index(inplace=True)


        #Calculating the ranking the median of each industry
        cols_rank = df.columns.tolist()
        remove_cols = ['industry', 'daily_agg_record_time', 'company_count']

        cols_rank = [x for x in cols_rank if x not in remove_cols]

        for i in cols_rank:
            abs_rank = "ranked_" + str(i)
            rank_fraction = "rank_fraction_" + str(i)
            rank_percent = "ranked_%_" + str(i)

            # count of all tickers with valid values in a specific industry
            count = df[i].values
            counter = count.size - np.count_nonzero(np.isnan(count))

            if kpi_mapping[i] == True:
                df[abs_rank] = df[i].rank(ascending=False)

            elif kpi_mapping[i] == False:
                df[abs_rank] = df[i].rank(ascending=True)

            # concatenating the columns to get the fraction format
            df[rank_fraction] = df[abs_rank].apply(lambda x: "{:.0f}".format(x)).astype(str) + " / " + str(counter)
            #replacing all invalid columns in rank with "nan"
            df[rank_fraction] = df[rank_fraction].apply(lambda x: np.nan if x.__contains__('nan') else x)
            # rank% to create another perspective to rank equities
            df[rank_percent] = 1 - (df[abs_rank] / counter)


        #converting numerals to alphabets for large numbers
        sum_list = ['marketCapUSD','totalRevenueUSD','ebitdaUSD']

        for col in sum_list:
            col_alpha = "alpha_" + str(col)
            df[col_alpha] = df[col].apply(convert_digits)


        #upload data data folder as pickle
        df.to_pickle('data/eq_daily_industry.pickle')

        upload_cloud_storage('eq_daily_industry.pickle')
        print('upload successful')

        # df = pd.read_pickle('data/eq_daily_industry.pickle')
        # print (df)
        # print (df.info(verbose=True)) 
        # print (df.dtypes)


        #######################################################################################
        ################### consider that this below might no longer be neccessary#############
        #######################################################################################
        
        # # inserting the data into the equity industry collection
        # collection = 'equity_daily_' + category
        # for index, row in df.iterrows():
        #     recordtime = datetime.now(tz_SG)
        #     data = {
        #         'daily_agg_record_time' : recordtime
        #     }
        #     dataset = {}
        #     for i in cols:
        #         categories = index
        #         data_dict = {
        #             i : row[i]
        #         }
        #         dataset.update(data_dict)
        #     data['daily_agg'] = dataset

        #     # print (categories, data)
        #     try:
        #         # when there are no categories, stop and exit without inserting data
        #         db.collection(collection).document(categories).set(data, merge=True)
        #     except:
        #         pass

    except AttributeError as e:
        print (e)

    except Exception as e:
        print (e)
        file_name = __name__
        function_name = inspect.currentframe().f_code.co_name
        subject = "Error on export_eq_industry"
        try:
            ticker = ticker
        except:
            ticker = "None"
        content = "Error in \n File name: "+ str(file_name) \
             + "\n Function: " + str(function_name) \
              + "\n Detailed error: " + str(e) \
                + "\n Error data: " + str(ticker)

        error_email(subject, content)




