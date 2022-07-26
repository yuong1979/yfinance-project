
#################################################################################
#################################################################################
############################ FLASK APP ##########################################
#################################################################################
#################################################################################

###### ###### ###### ###### ###### ###### ###### ###### ###### ###### ######
###### ###### ###### Local (NOT WORKING) ###### ###### ###### ###### ###### 
###### ###### ###### ###### ###### ###### ###### ###### ###### ###### ######

##### local #####
# docker build -t flaskapp .
# docker run -p 5000:5000 flaskapp
##### delete images and containers #####
# docker rmi -f $(docker images -aq)
# docker rm -vf $(docker ps -aq)

# FROM python:3.8
# COPY . .
# RUN pip install -r requirements.txt
# EXPOSE 5000
# ENTRYPOINT [ "python3" ]
# CMD ["main.py"]



###### ###### ###### ###### ###### ###### ###### ###### ###### ###### ######
###### ###### ###### Production ###### ###### ###### ###### ###### ##### ###
###### ###### ###### ###### ###### ###### ###### ###### ###### ###### ######

##### instructions to connect to environment variables on GCP #####
# https://youtu.be/JIE89dneaGo

######## PRODUCTION DATABASE ########
# gcloud builds submit --tag gcr.io/blockmacro-7b611/flaskappprod --project=blockmacro-7b611
# gcloud run deploy flaskappprod --image gcr.io/blockmacro-7b611/flaskappprod --platform managed --project=blockmacro-7b611 --allow-unauthenticated --region us-central1

######## TESTING DATABASE ########
# gcloud builds submit --tag gcr.io/testing-33c79/flaskapptest --project=testing-33c79
# gcloud run deploy flaskapptest --image gcr.io/testing-33c79/flaskapptest --platform managed --project=testing-33c79 --allow-unauthenticated --region us-central1

FROM python:3.8

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

# Install production dependencies.
RUN pip install -r requirements.txt
RUN pip install gunicorn

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
