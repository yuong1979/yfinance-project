import smtplib
from email.message import EmailMessage
from settings import (project_id, firebase_database, fx_api_key, firestore_api_key, google_sheets_api_key, 
                    schedule_function_key, firebase_auth_api_key, email_password)
from secret import access_secret


email_password = access_secret(email_password, project_id)

EMAIL_ADDRESS = "macrokpi2022@gmail.com"
EMAIL_PASSWORD = email_password

contacts = [EMAIL_ADDRESS]

msg = EmailMessage()
msg['Subject'] = 'Grab dinner this weekend'
msg['From'] = EMAIL_ADDRESS
msg['To'] = contacts
msg.set_content('How about dinner at 6pm this saturday ok,  dude dont be bwei swee!')

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:

    smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

    smtp.send_message(msg)