from boto3 import client
import  boto3 
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import csv

conn = client('s3')
s3 = boto3.resource('s3')

def get_access_time(bucket_Name):
    '''Given a bucket name, retrieve the events in the bucket. 
    Returns the Last bucket access time.'''

    client = boto3.client('cloudtrail')
    try:
        response = client.lookup_events(
                    LookupAttributes=[
                        {
                            'AttributeKey': 'ResourceName',
                            'AttributeValue': bucket_Name
                        },
                    ],
                    StartTime=datetime(2020, 12, 12),
                    EndTime=datetime(2021, 2, 24))

        
        event = response['Events'][0]
        Event_Time = event['EventTime']
    except:
        Event_Time = "Not accessed in last 3 months"
    return Event_Time
#EndTime=datetime.now()

def get_bucket_size(bucket_Name):
    '''returns size of a bucket'''

    total_size = 0
    paginator = conn.get_paginator('list_objects_v2')
   
    Bucket_Name = bucket.name
    
    pages = paginator.paginate(Bucket=Bucket_Name)
    try:
        for page in pages:
            try:
                for key in page['Contents']:
                    
                    total_size += key['Size']
                    
            except KeyError :
                print("Empty")
            
    except ClientError as e :
        print("Error:", e)


now = datetime.now()

timestamp = now.strftime("%Y-%m-%d-%H.%M.%S")

report_name = 's3usage' + timestamp + '.csv'

f = open(report_name, "w+", newline='')
fieldnames = ['BUCKET_NAME', 'SIZE(IN BYTES)', 'SIZE(IN GB)','ACCESS_TIME']
writer = csv.DictWriter(f, fieldnames=fieldnames)
writer.writeheader()

paginator = conn.get_paginator('list_objects_v2')
for bucket in s3.buckets.all():
    bucket_Name = bucket.name
    
    size_in_b = 0
    err = ''
    
    pages = paginator.paginate(Bucket=bucket_Name )
    try:
        for page in pages:
            try:
                for key in page['Contents']:
                    
                    size_in_b += key['Size']                  
            except KeyError :
                print("Empty")       
    except ClientError as e :
        err = err + e

    last_access_time = get_access_time(bucket_Name)
    size_in_gb =  size_in_b/(1024*1024*1024)
    
    writer.writerow({'BUCKET_NAME': bucket_Name, 'SIZE(IN BYTES)' : size_in_b , 'SIZE(IN GB)' : size_in_gb,'ACCESS_TIME': last_access_time})
f.close()
