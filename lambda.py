import boto3
from botocore.exceptions import ClientError
import requests
import os.path

session = boto3.Session()
lambda_client = session.client('lambda')

paginator = lambda_client.get_paginator('list_functions')
response_iterator = paginator.paginate()
client = boto3.client('lambda')
for response in response_iterator:
  functions = response["Functions"]

  for function in functions:
    #print(function)
    function_name = function["FunctionName"]

    function_name = str(function_name)
    #print("FUNCTION:::",function_name)
    response = client.list_provisioned_concurrency_configs(
    FunctionName=function_name)
    if len(response['ProvisionedConcurrencyConfigs']) == 0:
        print("Notconfigured")
    else:
        print("FUNCTION:::",function_name)
        











