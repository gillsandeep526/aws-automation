import json
import boto3
import datetime
import os
from dateutil.tz import tzutc

iam = boto3.resource('iam')
sns = boto3.client('sns')
today = datetime.datetime.now()
client = boto3.client('iam')
iam_users = []

def lambda_handler(event, context):
    #for user in client.list_users()["Users"]:
    aws_account_id = context.invoked_function_arn.split(":")[4]
    for user in iam.users.all():
        if user.password_last_used is not None:
            delta = (today - user.password_last_used.replace(tzinfo=None)).days
            if delta >= 90:
                print("Username: ",[user.user_name], delta)
                iam_users.append(user.user_name)
    print(iam_users)
    sns_arn = os.environ['SNSARN']
    '''sns_event = {}
    sns_event["default"] = json.dumps(iam_users)'''
    try:
        sns.publish(
        TargetArn=sns_arn,
        Message=json.dumps({'default': json.dumps(iam_users)}),
        MessageStructure='json',
        Subject="Iam Inactive users from last 90 days in " + aws_account_id + "account"
        )
    except Exception as e:
        print(e)
        raise e
