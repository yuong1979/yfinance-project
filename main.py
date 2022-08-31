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
from equity_imp import imp_equity_daily_kpi_fb, imp_equity_financials_fb, imp_equity_price_history_fb
from equity_compute import update_country_aggregates, update_industry_aggregates, insert_industry_agg_data, equity_kpi_ranker_0, equity_kpi_ranker_1





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
        #extracting daily kpi
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


    # runs every one hour - frequency : * */1 * * *
    @app.route("/equity_compute")
    def run_equity_compute():
        #extracting financials - after initial extraction is complete, change the code to extract daily only
        update_country_aggregates() 
        update_industry_aggregates()

        equity_kpi_ranker_0()
        equity_kpi_ranker_1()

        insert_industry_agg_data()
        return redirect(url_for("home"))


    # runs every day at 12am - frequency : 1 0 * * *
    @app.route("/ext_daily_fx_yf_fb")
    def run_ext_daily_fx_yf_fb():
        #extracting daily fx for rates to use in kpi calculations
        ext_daily_fx_yf_fb()
        return redirect(url_for("home"))

    # runs every day at 12pm - change to running every 6 hours - frequency : 0 */6 * * *
    @app.route("/exp_fb_gs")
    def run_exp_fb_gs():
        #extracting all datasets datetiem into gs to ensure all neccessary extracted
        exp_dataset_datetime_gs()
        #extracting daily kpi into gs
        exp_equity_daily_kpi_gs()
        #extracting fx datetime into gs to confirm that at least last 3 days for fx is extracted
        exp_fx_datetime_gs()

        return redirect(url_for("home"))

except Exception as e:
    print (e)
    file_name = __name__
    function_name = inspect.currentframe().f_code.co_name
    subject = "Error on macrokpi project"
    content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
    error_email(subject, content)



# earliest_equity_daily_kpi_list = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(1).get()
# print (earliest_equity_daily_kpi_list[0]._data)
# earliest_price_history_list = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(1).get()
# print (earliest_price_history_list)

# equity_daily_agg
# equity_financials
# equity_price_history




@app.get("/")
def home():

    earliest_tickerlist = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(5).get()
    latest_tickerlist = db.collection('equity_daily_kpi').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(5).get()

    #latest downloaded fx entry
    fxlist = db.collection('fxhistorical').order_by("datetime_format", direction=firestore.Query.DESCENDING).limit(3).stream()

    #latest downloaded equity_daily_country
    equity_daily_country_list = db.collection('equity_daily_country').order_by("daily_agg_record_time", direction=firestore.Query.DESCENDING).limit(3).stream()

    #latest downloaded equity_daily_industry
    equity_daily_industry_list = db.collection('equity_daily_industry').order_by("daily_agg_record_time", direction=firestore.Query.DESCENDING).limit(3).stream()

    #latest downloaded equity_price_history
    equity_price_history_list = db.collection('equity_price_history').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(3).stream()

    # for i in equity_price_history_list:
    #     print (i)
    #     print (i._data)

    #latest downloaded equity_daily_agg - rank
    equity_daily_agg_rank_list = db.collection('equity_daily_agg').order_by("daily_industry_rank_updated_datetime", direction=firestore.Query.DESCENDING).limit(3).stream()
    #latest downloaded equity_daily_agg - avg
    equity_daily_agg_avg_list = db.collection('equity_daily_agg').order_by("daily_industry_agg_updated_datetime", direction=firestore.Query.DESCENDING).limit(3).stream()


    return render_template("base.html", 
        earliest_tickerlist=earliest_tickerlist,
        latest_tickerlist=latest_tickerlist,

        equity_price_history_list=equity_price_history_list,

        equity_daily_agg_rank_list=equity_daily_agg_rank_list,
        equity_daily_agg_avg_list=equity_daily_agg_avg_list,

        equity_daily_country_list=equity_daily_country_list,
        equity_daily_industry_list=equity_daily_industry_list,
        fxlist=fxlist
        
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

