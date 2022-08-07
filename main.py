from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from samples import ticker_update , fx_update , extract_to_gs
from firebase_admin import firestore, credentials, initialize_app
from secret import apikeys

app = Flask(__name__)

port = 5000

# Initialize Firestore DB
cred = credentials.Certificate('secret/serviceAccountKey.json')
default_app = initialize_app(cred)
db = firestore.client()

pw = apikeys.run_cloud_key

# for testing
# http://localhost:5000/ticker_update_v5w4ZaM6*7B9
# http://localhost:5000/fx_update_v5w4ZaM6*7B9
# http://localhost:5000/extract_to_gs_v5w4ZaM6*7B9

@app.get("/ticker_update_" + pw)
def run_ticker_update():
    ticker_update()
    return redirect(url_for("home"))

@app.get("/fx_update_" + pw)
def run_fx_update():
    fx_update()
    return redirect(url_for("home"))

@app.get("/extract_to_gs_" + pw)
def run_extract_to_gs():
    extract_to_gs()
    return redirect(url_for("home"))


@app.get("/")
def home():
    tickerlist = db.collection('tickerlist').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(10).get()
    fxlist = db.collection('fx').order_by("updated_datetime", direction=firestore.Query.ASCENDING).limit(10).get()
    #add fx last time extracted
    return render_template("base.html", tickerlist=tickerlist, fxlist=fxlist)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=port)


