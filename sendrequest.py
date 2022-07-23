import requests

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


#actual post request
payload = {"title": "testing now1"}
r = requests.post("http://localhost:5000/add", data=payload)
print (r.status_code)
# print (r.url)
# print (r.text)
