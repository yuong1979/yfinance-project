

# ###############################################################################
# ######## Testing of cloud functions before deployment #########################
# ###############################################################################

url = "http://localhost:5000"
# url = "https://flaskapptest-hk6dfyb5mq-uc.a.run.app/"

#actual post request function for using on google cloud

# python -c 'from cloud_function import run_extract_financials_fb; run_extract_financials_fb()'
import requests
def run_extract_financials_fb():
    payload = {"mission": "run_extract_financials_fb"}
    r = requests.post(url+"/runmission", data=payload)
    print (r.status_code)
    return f'Post success'


# python -c 'from cloud_function import run_extract_fx_fb; run_extract_fx_fb()'
def run_extract_fx_fb():
    payload = {"mission": "run_extract_fx_fb"}
    r = requests.post(url+"/runmission", data=payload)
    print (r.status_code)
    return f'Post success'

# python -c 'from cloud_function import run_extract_fb_gs; run_extract_fb_gs()'
def run_extract_fb_gs():
    payload = {"mission": "run_extract_fb_gs"}
    r = requests.post(url+"/runmission", data=payload)
    print (r.status_code)
    return f'Post success'


# python -c 'from cloud_function import testing; testing()'
def testing():
    r = requests.get(url+"/run_extract_fx_fb")
    print (r.status_code)
    print (r.headers)
    return f'Post success'



# import requests

# def run_extract_fx_fb():
#     payload = {"mission": "run_extract_fx_fb"}
#     #replace with url endpoint of cloudrun 
#     url = "https://flaskapptest-hk6dfyb5mq-uc.a.run.app/"
#     r = requests.post(url+"/runmission", data=payload)
#     print (r.status_code)
#     return f'Post success'


# http://localhost:5000/runmission?mission=testonly





