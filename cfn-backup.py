from boto3 import client
import  boto3 
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

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
        print("Last Access: ",event['EventTime'])
    except:
        print("Last Access: not accessed")


def get_bucket_size(bucket_Name):
    '''returns size of a bucket'''

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_Name)

    total_size = 0
    try:
        for k in bucket.objects.all():
            total_size += k.size
        size = total_size/(1024*1024*1024)
    except ClientError:
        size = 0
    
    return size

now = datetime.now()

timestamp = now.strftime("%Y-%m-%d-%H.%M.%S")

report_name = 's3_objects_details' + timestamp + '.csv'

f = open(report_name, "w+")
fieldnames = ['BUCKET_NAME', 'OBJECT_NAME', 'ONJECT_SIZE']
writer = csv.DictWriter(f, fieldnames=fieldnames)
writer.writeheader()
for bucket in s3.buckets.all():
    top_level_folders = dict()
    total_size = 0
    paginator = conn.get_paginator('list_objects_v2')
    
    Bucket_Name = bucket.name
    
    print("Bucket_Name: ", Bucket_Name)
    
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
            print("Folder: %s, size: %.2f GB,OR %.2f Bytes" % (folder, size_in_gb,size))
            total_size += size
        t_size = total_size/(1024*1024*1024)
        print("Bucket Size : ", t_size)
            
            
    except ClientError as e :
        print("Error:", e)

    
    