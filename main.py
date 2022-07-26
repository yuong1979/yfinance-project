from doctest import debug_script
from unicodedata import name
from flask import Flask, render_template, request, redirect, url_for
from firebase_admin import firestore, credentials, initialize_app
from fx import ext_daily_fx_yf_fb
from secret import access_secret
import json
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
from tools import error_email
import inspect
# from mgt_fb_crud import financials_to_gs
# from mgt_fin_exp_fb import ext_daily_equity_financials_yf_fb
# from mgt_fin_exp_gs import ext_daily_equity_financials_fb_gs, ext_daily_equity_datetime_fb_gs
from equity_exp import exp_dataset_datetime_gs, exp_equity_daily_kpi_gs, exp_fx_datetime_gs
from equity_import.imp_equity_daily_kpi_fb import imp_equity_daily_kpi_fb
from equity_import.imp_equity_financials_fb import imp_equity_financials_fb
from equity_import.imp_equity_price_history_fb import imp_equity_price_history_fb
# from equity_transform import export_eq_daily_kpi, export_eq_daily_agg, export_eq_industry, export_eq_hist_details, export_eq_hist_price

from equity_transform.eq_daily_agg import export_eq_daily_agg
from equity_transform.eq_daily_industry import export_eq_industry
from equity_transform.eq_daily_kpi import export_eq_daily_kpi
from equity_transform.eq_hist_details import export_eq_hist_details
from equity_transform.eq_hist_price import export_eq_hist_price
from equity_transform.eq_hist_sum import export_eq_hist_sum
from equity_transform.eq_stats import export_eq_dl_stats



app = Flask(__name__)

port = 5000

### Run the below first if running on local to connect to secret manager on google cloud
# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"
# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/test_local_access.json"

cloud_run_apikey = access_secret(schedule_function_key, project_id)
firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)

# Initialize Firestore DB
cred = credentials.Certificate(firestore_api_key_dict)
default_app = initialize_app(cred)
db = firestore.client()

pw = cloud_run_apikey







# ###############################################################################
# ######## Get request for running using cloud scheduler ########################
# ###############################################################################

try:

    # runs every one hour - frequency : * */1 * * *
    @app.route("/imp_equity_daily_kpi_fb")
    def run_imp_equity_daily_kpi_fb():
        #extracting daily kpi ratios
        imp_equity_daily_kpi_fb()
        return redirect(url_for("home"))

    # runs every 10 minutes - frequency : */10 * * * *
    @app.route("/imp_equity_financials_fb")
    def run_imp_equity_financials_fb():
        #extracting financials - only done once a quarter after initial extraction is complete
        imp_equity_financials_fb()
        return redirect(url_for("home"))

    # runs every one hour - frequency : * */1 * * *
    @app.route("/imp_equity_price_history_fb")
    def run_imp_equity_price_history_fb():
        #extracting financials - after initial extraction is complete, change the code to extract daily only
        imp_equity_price_history_fb()
        return redirect(url_for("home"))

    # runs every day at 12am - frequency : 1 0 * * *
    @app.route("/ext_daily_fx_yf_fb")
    def run_ext_daily_fx_yf_fb():
        #extracting daily fx for rates to use in kpi calculations
        ext_daily_fx_yf_fb()
        return redirect(url_for("home"))


    # runs every twelve hour - frequency : * */12 * * *

    #### hold the timed extraction for the below until data-extraction into firebase is complete, maybe only need to run once a day
    #### This is to run all the compute and dump cleaned data into streamlit
    @app.route("/equity_compute")
    def run_equity_compute():

        # to extract daily kpi ratios to gcp
        export_eq_daily_kpi()
        # to extract daily kpi ratios group them by industry to gcp
        export_eq_industry()
        # to extract financials time series to gcp
        export_eq_hist_details()
        # to extract price time series to gcp
        export_eq_hist_price()
        # to extract stats to monitor firebase extract health
        export_eq_dl_stats()

        ##### with prerequisites #####
        # to extract financials time series - grouped by industry to gcp - prerequisite - export_eq_hist_details()
        export_eq_hist_sum()
        # to extract daily kpi ratios and aggregate them by rank/media and load into gcp - prerequisite - export_eq_daily_kpi()
        export_eq_daily_agg()

        return redirect(url_for("home"))





    #################################################################################
    ############ Below is outdated - exclude from cloud schedule as well ############
    #################################################################################
    # # runs every two hours - frequency : * */2 * * *
    # @app.route("/equity_compute_intensive")
    # def run_equity_compute_intensive():
    #     insert_industry_rank_data()
    #     insert_industry_agg_data()
    #     return redirect(url_for("home"))




    # # runs every day at 12pm - change to running every 12 hours - frequency : 0 */12 * * *
    # @app.route("/exp_fb_gs")
    # def run_exp_fb_gs():
    #     #extracting all datasets datetiem into gs to ensure all neccessary extracted
    #     exp_dataset_datetime_gs()
    #     #extracting daily kpi into gs
    #     exp_equity_daily_kpi_gs()
    #     #extracting fx datetime into gs to confirm that at least last 3 days for fx is extracted
    #     exp_fx_datetime_gs()

    #     return redirect(url_for("home"))

except Exception as e:
    print (e)
    file_name = __name__
    function_name = inspect.currentframe().f_code.co_name
    subject = "Error on macrokpi project"
    content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
    error_email(subject, content)


import csv
filepath = "dataframes/test.csv"


@app.get("/")
def home():


    earliest_tickerlist = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(5).get()
    latest_tickerlist = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(5).get()

    #latest downloaded equity_price_history in firebase
    equity_price_history_list = db.collection('equity_price_history').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(3).stream()
    
    #latest downloaded equity financials price history in firebase
    equity_financials_hist_list = db.collection('equity_financials').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(3).stream()

    #latest downloaded fx entry in firebase
    fxlist = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(3).stream()

    return render_template("base.html", 
        earliest_tickerlist=earliest_tickerlist,
        latest_tickerlist=latest_tickerlist,

        equity_price_history_list=equity_price_history_list,

        equity_financials_hist_list=equity_financials_hist_list,

        fxlist=fxlist,

        # sample_csv_data_list=sample_csv_data_list
        
        )

    # return render_template("base.html")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=port)



# @app.get("/")
# def home():
#     return render_template("base_test.html")

# if __name__ == "__main__":
#     app.run(debug=True, host='0.0.0.0', port=port)



#cloud schedule updates
# every 2 hours
# name : imp_equity_daily_kpi_fb
# description : imp_equity_daily_kpi_fb
# frequency : * */2 * * *
# http : https://flaskapptest-hk6dfyb5mq-uc.a.run.app/imp_equity_daily_kpi_fb

# every 10 minutes
# name : imp_equity_financials_fb
# description : imp_equity_financials_fb
# frequency : */10 * * * *
# http : https://flaskapptest-hk6dfyb5mq-uc.a.run.app/imp_equity_financials_fb

# every 10 minutes
# name : imp_equity_price_history_fb
# description : imp_equity_price_history_fb
# frequency : */10 * * * *
# http : https://flaskapptest-hk6dfyb5mq-uc.a.run.app/imp_equity_price_history_fb

# at 0000 everyday
# name : ext_daily_fx_yf_fb
# description : ext_daily_fx_yf_fb
# frequency : 1 0 * * *
# http : https://flaskapptest-hk6dfyb5mq-uc.a.run.app/ext_daily_fx_yf_fb

# every 6 hours
# name : exp_fb_gs
# description : exp_fb_gs
# frequency : 0 */6 * * *
# http : https://flaskapptest-hk6dfyb5mq-uc.a.run.app/exp_fb_gs













# ###############################################################################
# ######## Post request does not work only get request works ####################
# ###############################################################################


# @app.route('/runmission', methods = ['GET', 'POST'])
# def run_extract():

#     print (request.form)

#     # try:

#     if request.method == 'GET':
#         error_email("test1","test2")


#     if request.method == 'POST':

#         if request.form['mission'] == "run_extract_financials_fb":
#             print ("run_extract_financials_fb")
#             ext_daily_equity_financials_yf_fb()


#         if request.form['mission'] == "run_extract_fx_fb":
#             print ("run_extract_fx_fb")
#             ext_daily_fx_yf_fb()


#         if request.form['mission'] == "run_extract_fb_gs":
#             print ("run_extract_fb_gs")
#             ext_daily_equity_financials_fb_gs()
#             ext_daily_equity_datetime_fb_gs()


#         if request.form['mission'] == "testonly":
#             print ("testonly works")

#     return redirect(url_for("home"))

#     # except Exception as e:
#     #     print (e)
#     #     file_name = __name__
#     #     function_name = inspect.currentframe().f_code.co_name
#     #     subject = "Error on macrokpi project"
#     #     content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
#     #     error_email(subject, content)

