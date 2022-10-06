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
from tools import error_email, export_gs_func, kpi_mapping, kpi_remove, upload_cloud_storage, date_mapper
import math
import pytz
from google.cloud import storage
import os
import inspect
import numpy as np
from time import process_time


firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)
fbcredentials = service_account.Credentials.from_service_account_info(firestore_api_key_dict)
db = Client(firebase_database, fbcredentials)


####################################################################################################################################
######## Description: Extracting detailed time series historicals bs/pnl/cf and grouping it by industry to view industry time series financials
######## Prerequisite is to have run eq_hist_details.py first to get the pickle file
####################################################################################################################################


# #################################################################################################
# ####### Consolidating all historicals financials - PREREQUISITE #################################
# #################################################################################################
# python -c 'from equity_transform.eq_hist_sum import export_eq_hist_sum; export_eq_hist_sum()'

def export_eq_hist_sum():

    df = pd.read_pickle('data/eq_hist_details.pickle')

    industry_df = pd.read_pickle('data/eq_daily_kpi.pickle')
    industry_df = industry_df['industry']
    industry_df = industry_df.reset_index()

    # vlookip industry data to hist details
    df_w_history = pd.merge(industry_df, df, how='right', left_on='Ticker', right_on = 'ticker')
    date_mapping = date_mapper()
    #merging quarterly dates with df
    df_total = pd.merge(df_w_history, date_mapping, how='left', left_on='date', right_on = 'date')

    # ###############################################################################################################
    # ##################### Summming the data set for industry ###################################################
    # ###############################################################################################################

    df_hist_sum = df_total.groupby(['year', 'quarter', 'qtr_last_date','ann_last_date','industry','cattype','kpi'])['values'].sum()
    df_hist_sum = df_hist_sum.reset_index()

    # structure the data such that qtrly cattype will have last date as the quarterly last and the annual cattype will have dec 31 of each year as last date
    # if quarterly data than the last date should be empty, if annual data than then the last date for quarter should be empty
    # this is to fix the display of qtr results showing up in annual report in streamlit charts
    df_hist_sum['last_date'] = df_hist_sum.apply(lambda x: x['ann_last_date'] if x['cattype'][0:6] == "annual" else x['qtr_last_date'], axis=1)

    df_hist_sum.to_pickle('data/eq_hist_sum.pickle')
    upload_cloud_storage('eq_hist_sum.pickle')
    print ('upload successful')





    ##################### Removed ranking script because 1)the date size is large 2)computational intensive 3) no real use case, real case replaced by proportion





    # ###############################################################################################################
    # ##################### Ranking the data set ####################################################################
    # ###############################################################################################################
    # # this could be hard to do because the computing is extremely heavyweight and kpis have ascending and descending that needs to be included

    # t1_start = process_time()

    # df_rank_ticker = df_total.groupby(['year','quarter','ticker','industry','cattype','kpi'])['values'].sum()
    # df_rank_ticker = df_rank_ticker.reset_index()
    # # print (df_rank_ticker)

    # unique_quarter = df_rank_ticker['quarter'].unique().tolist()
    # all_unique_kpi = df_rank_ticker['kpi'].unique().tolist()
    # unique_industry = df_rank_ticker['industry'].unique().tolist()
    # unique_year = df_rank_ticker['year'].unique().tolist()
    # # unique_cattype = df_rank_ticker['cattype'].unique().tolist()

    # # unique_industry = ['Entertainment', 'Telecom Services']

    # unique_kpi = [
    # 'Total Assets', 'Total Current Assets', 'Total Current Liabilities', 'Total Liab', 'Total Stockholder Equity', 
    # 'Total Cashflows From Investing Activities', 'Change To Netincome', 'Total Cash From Operating Activities', 'Net Income', 'Change In Cash', 'Total Cash From Financing Activities',
    # 'Total Revenue', 'Gross Profit', 'Operating Income', 'Net Income', 'Ebit'
    # ]



    # ###############################################################################################################
    # ##################### Ranking the annual data set #############################################################
    # ###############################################################################################################

    # ann_unique_cattype = ['annual_profit&loss', 'annual_cashflow', 'annual_balancesheet']

    # #filter for selected kpis
    # df_rank_ticker_ann = df_rank_ticker[df_rank_ticker['kpi'].isin(unique_kpi)]
    # #filter for selected cattype
    # df_rank_ticker_ann = df_rank_ticker_ann[df_rank_ticker_ann['cattype'].isin(ann_unique_cattype)]
    # #merged to columns to make script run faster (by filtering for merged column)
    # df_rank_ticker_ann['cattype_kpi'] = df_rank_ticker_ann['cattype'] + df_rank_ticker_ann['kpi']
    # #merged to columns to make script run faster (by filtering for merged column)
    # df_rank_ticker_ann['industry_year'] = df_rank_ticker_ann['industry'] + df_rank_ticker_ann['year']

    # unique_cattype_kpi = df_rank_ticker_ann['cattype_kpi'].unique().tolist()
    # unique_industry_year = df_rank_ticker_ann['industry_year'].unique().tolist()

    # df_hist_rank_qtr = pd.DataFrame()

    # total_count = len(unique_industry_year)
    # count = 0
    # for i in unique_industry_year:
    #     for j in unique_cattype_kpi:
    #         df_data = df_rank_ticker_ann[(df_rank_ticker_ann['industry_year'] == i) & (df_rank_ticker_ann['cattype_kpi'] == j)].copy()
    #         test_zero = df_data['values'].sum()
    #         if test_zero != 0:
    #             df_data['rank'] = df_data['values'].rank(ascending=False)
    #             count_val = df_data['values'].count()
    #             df_data['rank_fraction'] = df_data["rank"].astype(int).astype(str) + "/" + str(count_val)
    #             df_data['rank_%'] = 1 - (df_data['rank'] / count_val)

    #         df_hist_rank_qtr = pd.concat([df_data, df_hist_rank_qtr])

    #     count = count + 1
    #     print (str(count) + " / " + str(total_count))

    # print (df_hist_rank_qtr, "df_hist_rank_qtr")






    # ###############################################################################################################
    # ##################### Ranking the quarterly data set ##########################################################
    # ###############################################################################################################

    # qtr_unique_cattype = ['quarterly_profit&loss', 'quarterly_cashflow', 'quarterly_balancesheet']

    # #filter for selected kpis
    # df_rank_ticker_qtr = df_rank_ticker[df_rank_ticker['kpi'].isin(unique_kpi)]
    # #filter for selected cattype
    # df_rank_ticker_qtr = df_rank_ticker_qtr[df_rank_ticker_qtr['cattype'].isin(qtr_unique_cattype)]
    # #merged to columns to make script run faster (by filtering for merged column)
    # df_rank_ticker_qtr['cattype_kpi'] = df_rank_ticker_qtr['cattype'] + df_rank_ticker_qtr['kpi']
    # #merged to columns to make script run faster (by filtering for merged column)
    # df_rank_ticker_qtr['industry_qtr'] = df_rank_ticker_qtr['industry'] + df_rank_ticker_qtr['quarter'].astype(str)

    # unique_cattype_kpi = df_rank_ticker_qtr['cattype_kpi'].unique().tolist()
    # unique_industry_qtr = df_rank_ticker_qtr['industry_qtr'].unique().tolist()

    # df_hist_rank_ann = pd.DataFrame()

    # total_count = len(unique_industry_qtr)
    # count = 0
    # for i in unique_industry_qtr:
    #     for j in unique_cattype_kpi:
    #         df_data = df_rank_ticker_qtr[(df_rank_ticker_qtr['industry_qtr'] == i) & (df_rank_ticker_qtr['cattype_kpi'] == j)].copy()
    #         test_zero = df_data['values'].sum()
    #         if test_zero != 0:
    #             df_data['rank'] = df_data['values'].rank(ascending=False)
    #             count_val = df_data['values'].count()
    #             df_data['rank_fraction'] = df_data["rank"].astype(int).astype(str) + "/" + str(count_val)
    #             df_data['rank_%'] = 1 - (df_data['rank'] / count_val)

    #         df_hist_rank_ann = pd.concat([df_data, df_hist_rank_ann])

    #     count = count + 1
    #     print (str(count) + " / " + str(total_count))

    # print (df_hist_rank_ann, "df_hist_rank_ann")



    # df_hist_rank = pd.concat([df_hist_rank_ann, df_hist_rank_qtr])
    # df_hist_rank.to_pickle('data/eq_hist_rank.pickle')


    # ### if the numbers check out - uncomment the below and run upload
    # upload_cloud_storage('eq_hist_rank.pickle')

    # print('uploads successful')


    # t1_stop = process_time()
    # print("Elapsed time during the whole program in seconds:",t1_stop-t1_start) 




# #################################################################################################
# ####### Check the ranking and the sum if they all add up ########################################
# #################################################################################################
# python -c 'from equity_transform.eq_hist_details_agg import checking; checking()'

def checking():

    #compare this two to the below
    df_hist_rank = pd.read_pickle('data/df_hist_rank.pickle')
    df_hist_sum = pd.read_pickle('data/df_hist_sum.pickle')

    df = pd.read_pickle('data/eq_hist_details.pickle')

    df_groupby = df.groupby('kpi')['values'].sum()
    df_hist_sum_groupby = df_hist_sum.groupby('kpi')['values'].sum()
    df_hist_rank_groupby = df_hist_rank.groupby('kpi')['values'].sum()

    print (df_groupby.tail(30), "df_groupby")
    print (df_hist_sum_groupby.tail(30), "df_hist_sum_groupby")
    print (df_hist_rank_groupby.head(30), "df_hist_rank_groupby")

    ###############################################################################################################
    ##################### Reference ###############################################################################
    ###############################################################################################################


    # ########## checking if the consolidated sum of the values is accurate
    # df_industry_check_1 = df_industry_sum.groupby('kpi')['values'].sum()
    # print (df_industry_check_1.tail(30))
    # df_industry_check_2 = df_w_history.groupby('kpi')['values'].sum()
    # print (df_industry_check_2.tail(30))


    # ########## checking the dates distribution to see if reallocation is required, 
    # ########## currently all financials submission that fall between the quarterly start and end dates are reported in that quarterly date
    # df_check_date = df[['date','quarter','ticker']]
    # df_check_date = df_check_date.groupby(['quarter', 'date'])['ticker'].count()
    # df_check_date = df_check_date.reset_index()
    # print (df_check_date.tail(50))



