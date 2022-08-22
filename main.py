from flask import Flask, render_template, request, redirect, url_for
from firebase_admin import firestore, credentials, initialize_app
from mgt_fb_crud import financials_to_gs
# from mgt_fin_exp_fb import ext_daily_equity_financials_yf_fb
from equity_imp import imp_equity_daily_kpi_fb
from equity_exp import exp_equity_daily_kpi_datetime_gs, exp_equity_daily_kpi_gs
from fx import ext_daily_fx_yf_fb
from mgt_fin_exp_gs import ext_daily_equity_financials_fb_gs, ext_daily_equity_datetime_fb_gs
from secret import access_secret
import json
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key
from email_function import error_email
import inspect

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

    @app.route("/run_extract_financials_fb")
    def run_extract_financials_fb():
        imp_equity_daily_kpi_fb()
        return redirect(url_for("home"))


    @app.route("/run_extract_fx_fb")
    def run_extract_fx_fb():
        ext_daily_fx_yf_fb()
        return redirect(url_for("home"))


    @app.route("/run_extract_fb_gs")
    def run_extract_fb_gs():
        exp_equity_daily_kpi_datetime_gs()
        exp_equity_daily_kpi_gs()
        return redirect(url_for("home"))

except Exception as e:
    print (e)
    file_name = __name__
    function_name = inspect.currentframe().f_code.co_name
    subject = "Error on macrokpi project"
    content = "Error in \n File name: " + str(file_name) + "\n Function: " + str(function_name) + "\n Detailed error: " + str(e)
    error_email(subject, content)

@app.get("/")
def home():

    earliest_tickerlist = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(5).get()
    latest_tickerlist = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.DESCENDING).limit(5).get()
    fxlist = db.collection('fx').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(10).get()
    return render_template("base.html", 
        earliest_tickerlist=earliest_tickerlist,
        latest_tickerlist=latest_tickerlist, 
        fxlist=fxlist)

    # return render_template("base.html")

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=port)



# @app.get("/")
# def home():
#     return render_template("base_test.html")

# if __name__ == "__main__":
#     app.run(debug=True, host='0.0.0.0', port=port)















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

