

# #testing get request
# payload = {"firstName": "John", "lastName": "Smith"}
# r = requests.get("http://localhost:5000/gettest", params=payload)
# # print (r.status_code)
# # print (r.url)
# print (r.text)
# print (r.headers)
# print (r.url)

# #testing post request
# payload = {"firstName": "John", "lastName": "Smith"}
# r = requests.post("http://localhost:5000/posttest", data=payload)
# # print (r.status_code)
# # print (r.url)
# print (r.text)



#actual post request function for using on google cloud
import requests
def send_post(request):
    payload = {"title": "testing now1"}
    #replace with url endpoint of cloudrun 
    url = "http://localhost:5000"
    r = requests.post(url+"/add", data=payload)
    print (r.status_code)
    return f'Post success'



import pytz
import datetime as dt

localtz=pytz.timezone('Asia/Singapore')
now = dt.datetime.now()
now=localtz.localize(now)
print(now)


