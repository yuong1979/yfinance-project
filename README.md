# Sample flask project with docker and GCP deployment ready 

**Clone project**

`git clone https://github.com/yuong1979/flask.git`

**Change directory into the project**

`cd flaskproj`

**Start a the virtual environment**

`python3 -m venv venv`

**Run the virtual environment**

`source venv/bin/activate`

**Run the virtual environment**

`pip install -r requirements.txt`

### Deploy on local without docker

**Run the server only local**

`python3 main.py`

Access app through url http://localhost:5000/


### Deploy with docker on local

Comment out the production code on Dockerfile

**Build the image**

`docker build -t flaskapp .`

**Deploy the image**

`docker run -p 5000:5000 flaskapp`

Access app through url http://localhost:5000/


### Deploy with docker on production

Comment out the development code on Dockerfile

**Initialize gcloud**

`gcloud init`

**Build image**

`gcloud builds submit --tag gcr.io/test-python-api-spreadsheets/flaskapp`

**Deploy image**

`gcloud run deploy --image gcr.io/test-python-api-spreadsheets/flaskapp`




<!-- export FLASK_APP=app.py
export FLASK_ENV=development
flask run -->
