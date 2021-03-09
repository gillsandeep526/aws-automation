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


report_name = 's3_Obj' + timestamp + '.csv'

f = open(report_name, "w+", newline='')
fieldnames = ['BUCKET_NAME', 'OBJECT NAME', 'OBJECT SIZE(IN BYTES)', 'OBJECT SIZE(IN GB)']
writer = csv.DictWriter(f, fieldnames=fieldnames)
writer.writeheader()


paginator = conn.get_paginator('list_objects_v2')
for bucket in s3.buckets.all():
    top_level_folders = dict()
    
    paginator = conn.get_paginator('list_objects_v2')
    
    Bucket_Name = bucket.name
      
    s = get_access_time(Bucket_Name)
    
    pages = paginator.paginate(Bucket=Bucket_Name)
    try:
        for page in pages:
            try:
                for key in page['Contents']:
                    
                    folder = key['Key'].split('/')[0]
                    #print("Key %s in folder %s. %d bytes" % (key['Key'], folder, key['Size']))

                    if folder in top_level_folders:
                        top_level_folders[folder] += key['Size']
                        
                        
                    else:
                        top_level_folders[folder] = key['Size']
                    
            except KeyError :
                print("Empty")
        for folder, size in top_level_folders.items():
            
            size_in_gb = size/(1024*1024*1024)
            writer.writerow({'BUCKET_NAME': Bucket_Name, 'OBJECT NAME' : folder, 'OBJECT SIZE(IN BYTES)' : size , 'OBJECT SIZE(IN GB)' : size_in_gb})
            
            
            
    except ClientError as e :
        print("Error:", e)
    
    
f.close()
