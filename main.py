from flask import Flask, render_template, request, redirect, url_for
from firebase_admin import firestore, credentials, initialize_app
from mgt_fin_exp_fb import extract_to_fb
from fx import extract_fx
from mgt_fin_exp_gs import extract_to_gs
from secret import access_secret
import json
from settings import project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, schedule_function_key, firebase_auth_api_key

app = Flask(__name__)

port = 5000

### Run the below first if running on local to connect to secret manager on google cloud
# export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"

cloud_run_apikey = access_secret(schedule_function_key, project_id)
firestore_api_key = access_secret(firestore_api_key, project_id)
firestore_api_key_dict = json.loads(firestore_api_key)

# Initialize Firestore DB
cred = credentials.Certificate(firestore_api_key_dict)
default_app = initialize_app(cred)
db = firestore.client()

pw = cloud_run_apikey

@app.get("/extract_info_fb_" + pw)
def run_extract_to_fb():
    extract_to_fb()
    return redirect(url_for("home"))

@app.get("/extract_fx_fb_" + pw)
def run_extract_fx():
    extract_fx()
    return redirect(url_for("home"))

@app.get("/extract_to_gs_" + pw)
def run_extract_to_gs():
    extract_to_gs()
    return redirect(url_for("home"))


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
