#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime, timedelta
from datetime import date
import pandas as pd
import numpy as np
import pandas.io.sql as psql
import mysql.connector
import csv
from pandas import DataFrame

import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os

import traceback
# from datetime import date, timedelta

today=date.today()
yesterday = today- timedelta(days=1)

print(today,yesterday)


mydb = mysql.connector.connect(
  host="marketing-db.cmwukub0eama.ap-south-1.rds.amazonaws.com",
  user="admin",
  password="marketing",
  database="marketingDashboard"
)
mycursor = mydb.cursor()
conn = mycursor.execute

df = pd.read_sql_query(f'''
SELECT Created_at_IST_Date,enquiry_id,leadId,source,lead_source,disease_leads,category_final,team_final,city,url,full_url,
       call_status 
FROM marketingDashboard.enquiry_table
where Created_at_IST_Date >= '2023-12-01'
      and isResurfaced is null
      and sr_number='9513309926'
      and sevenDayUniqueSQL=1
''',mydb)

df=pd.DataFrame(df)
df.shape


# In[2]:


from pymongo import MongoClient
from pprint import pprint

client = MongoClient('mongodb://pristynRoot:pristyn321Root@10.0.25.154:27017/bitnami_parse')
db=client.bitnami_parse

df_Leads_LeadID = df['enquiry_id']
x = df_Leads_LeadID.to_string(header=False, index=False).split('\n')
                  
vals = [','.join(ele.split()) for ele in x]
vals

# vals = list(df_Enq['enquiry_id'].unique())

# Extracting Knwlarity calls data
col = db["EnquiriesView"]
cursor = col.aggregate([
    {'$match' : { '_id' : { '$in' : vals} }},
    {
            "$lookup": {
                "from": "LeadsView",
                "localField": "leadId",
                "foreignField": "_id",
                "as": "LeadDetails"
            }
        },
        {
            "$unwind": {
                "path": "$LeadDetails",
                 "preserveNullAndEmptyArrays": True
            }
        },
    {
       "$project":
         {
 "enquiry_id":"$_id" ,           
"leadStatus" : "$LeadDetails.leadStatus",
"leadSubStatus":"$LeadDetails.leadSubStatus",
"_id":0
         }}])

Enquiries = list(cursor)
df_Enquiries=DataFrame(Enquiries)
df_Enquiries.shape


# In[3]:


final=df.merge(df_Enquiries,on='enquiry_id',how='left')

clean_leadsource={'Not going with Pristyncare' : 'Not going with Pristyncare',
                  'Not responding now' : 'Not responding now',
                  'CrossCategory' : 'CrossCategory',
                  'Warm' : 'Warm',
                  'DNP' : 'DNP',
                  'Cold' : 'Cold',
                  'Cost' : 'Cost',
                  'Location far away' : 'IrrelevantLead',
                  'Duplicate' : 'Duplicate',
                  'Didn’t Enquire' : 'IrrelevantLead',
                  'Only Medication' : 'Only Medication',
                  'Hot' : 'Hot',
                  'Invalid Number' : 'IrrelevantLead',
                  'Closed' : 'Closed',
                  'Relevant' : 'Relevant',
                  'New' : 'New',
                  'Surgery not Suggested' : 'Surgery not Suggested',
                  'FollowUp' : 'FollowUp',
                  'Disease not covered' : 'IrrelevantLead',
                  'DNP exhausted' : 'DNP',
                  'Language barrier' : 'Language barrier',
                  'DNP 1' : 'DNP',
                  'Not Serviceable' : 'IrrelevantLead',
                  'xOPD' : 'xOPD',
                  'IrrelevantLead' : 'IrrelevantLead',
                  'Exisiting Sx/Px Query' : 'Exisiting Sx/Px Query',
                  'Qualified' : 'Qualified',
                  'DNP 2' : 'DNP',
                  'Not interested' : 'IrrelevantLead',
                  'Didnâ€™t Enquire' : 'IrrelevantLead'}

final['clean_leadsource']=final['leadStatus'].replace(clean_leadsource)
final.shape


# In[4]:


leadids=final['leadId'].values.tolist()
leadids = ', '.join(["'{}'".format(x) for x in leadids])
leadids[:100]


# In[5]:


df_appt = pd.read_sql_query(f'''
select date(Appointment_Start_Time) as Appointment_Start_date,LeadId as leadId,
       Appointment_Type,Appointment_Status,OPDType,Doc_Surgery_Status,Payment_Mode,
       case when Appointment_Type="OPD" and 
            (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
            then 1 else 0 end as OPD_Booked_Flag,
       case when Appointment_Type="OPD" and Appointment_Status = "Active"
            and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
            then 1 else 0 end as OPD_Flag,
       case when Appointment_Type="OPD" and Appointment_Status = "Active"
            and (OPDType in ("Consultation","Walkin","Online Consult","Scan"))
            and  (Doc_Surgery_Status in ("Surgery Suggested","Surgery Needed","Potential Surgery"))
            then 1 else 0 end as SS_Flag,                                   
       case when (Appointment_Type="IPD" and Appointment_Status = "Active")
            or (Appointment_Type="OPD" and Appointment_Status = "Active" and OPDType="Procedure" )
            then 1 else 0 end as IPD_Flag
from marketingDashboard.Appointments
where Appointment_Start_Time>='2023-12-01'
and leadId in ({leadids})
''',mydb)

df_appt=pd.DataFrame(df_appt)
df_appt.shape


# In[6]:


x=final[['Created_at_IST_Date','leadId']]
dff=df_appt.merge(x,on='leadId',how='left')

appts=dff[(dff['Created_at_IST_Date']<=dff['Appointment_Start_date'])]


appts=appts[['leadId','OPD_Booked_Flag','OPD_Flag','SS_Flag','IPD_Flag']]
appts=appts.groupby(['leadId']).agg({'OPD_Booked_Flag': 'sum', 'OPD_Flag': 'sum', 'SS_Flag': 'sum',
                                     'IPD_Flag':'sum'}).reset_index()
appts.shape


# In[7]:


final=final.merge(appts,on='leadId',how='left')
final=final.fillna(0)
final.shape


# In[21]:


try:
    csvFile = '9513309926_URO.csv'
    final.to_csv(csvFile,index=False)
    
    sender_email = ["marketing.reports@pristyncare.com"]
    receiver_email = ["gaurav.kumar2@pristyncare.com","somesh.kumar@pristyncare.com","deepinder.singh@pristyncare.com",
                      "yogesh@pristyncare.com","saurabh.garg@pristyncare.com","surya.milky@pristyncare.com"]
    password = "Target@7000"
    
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    # Authenticate with Google Sheets
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/ubuntu/GK-DM/Crons/syed-reporting-83428b4082ce.json", scope)
    client = gspread.authorize(creds)
    spreads2 = client.open_by_key("1zri_H72_BJ7WQ7jRXcgHvP2MiTjHRZGhb1C9Y8PL3jI")

    sheetName = "Leads!A:Z" 
    spreads2.values_clear(sheetName)

    spreads2.values_update(sheetName,
                     params={'valueInputOption': 'USER_ENTERED'},
                     body={'values': list(csv.reader(open(csvFile)))})
    
except Exception as e: 
    
    error=traceback.format_exc() #to store error
    traceback.print_exc() # to pring in runtime
    subject = "⚠️[ALERT!] 9513309926 SR Number URO Report Failed to run "
    body = f"Hi,\n 9513309926 SR Number URO Report Failed to run because :\n\n {error} "+"\n"+"\n link: \n https://docs.google.com/spreadsheets/d/1zri_H72_BJ7WQ7jRXcgHvP2MiTjHRZGhb1C9Y8PL3jI/edit?usp=sharing"
    email = MIMEMultipart()
    email["From"] = ', '.join(sender_email)
    email["To"] =  ', '.join(receiver_email)
    # email["CC"] = ', '.join(cc_email)
    email["Subject"] = subject


    email.attach(MIMEText(body,"plain"))
    bodytext=""

    session = smtplib.SMTP('smtp.gmail.com', 587) 
    session.starttls() 
    session.login(sender_email[0],password) 
    text = email.as_string()
    session.sendmail(sender_email, receiver_email,text)
    #                      +cc_email,text)
    print("Alert mail sent")
    
else:
    
    subject = "[Cron runed] 9513309926 SR Number URO Report Updated "+str(yesterday)
    body = "Hi,\n 9513309926 SR Number URO Report Updated till "+str(yesterday)+"\n link: \n https://docs.google.com/spreadsheets/d/1zri_H72_BJ7WQ7jRXcgHvP2MiTjHRZGhb1C9Y8PL3jI/edit?usp=sharing"
    email = MIMEMultipart()
    email["From"] = ', '.join(sender_email)
    email["To"] =  ', '.join(receiver_email)
    # email["CC"] = ', '.join(cc_email)
    email["Subject"] = subject


    email.attach(MIMEText(body,"plain"))
    bodytext=""

    session = smtplib.SMTP('smtp.gmail.com', 587) 
    session.starttls() 
    session.login(sender_email[0],password) 
    text = email.as_string()
    session.sendmail(sender_email, receiver_email,text)
    #                      +cc_email,text)
    print("Success mail sent")

