import json
import boto3
import os
from datetime import datetime
from collections import OrderedDict
from botocore.exceptions import ClientError, ParamValidationError

EnvPrefix = os.environ['EnvPrefix']
BucketName = os.environ['BucketName']
ENVPREFIX = EnvPrefix.upper()


def connect_client(resource, region="us-east-1"):
    try:
        return boto3.client(resource, region_name=region)
    except ClientError as ce:
        logging.error(ce)
        return False


def EnvStackslist(EnvPrefix):
    '''Given th Env prefix,Returns the list 
       of stacks in given environment'''

    total_stacks = []
    cloudformation = connect_client('cloudformation')
    paginator = cloudformation.get_paginator('list_stacks')

    response_iterator = paginator.paginate(StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE'])
    try:
        for page in response_iterator:

            stack = page['StackSummaries']
            for output in stack:

                stackName = output['StackName']
                stack_prefix_list = stackName.split("-")

                if len(stack_prefix_list) >= 3:
                    stack_prefix = stack_prefix_list[0] + "-" + stack_prefix_list[1] + "-" + stack_prefix_list[2] + "-" + stack_prefix_list[3]
                    STACK_PREFIX = stack_prefix.upper()
                    if STACK_PREFIX == ENVPREFIX:
                        total_stacks.append(stackName)

    except IndexError as e:
        print("indexerror", e)

    print("Total num. of stacks in " + EnvPrefix + "are", len(total_stacks))
    return total_stacks


def lambda_handler(event, context):

    cloudformation = connect_client('cloudformation')
    s3 = boto3.resource("s3")
    now = datetime.now()

    timestamp = now.strftime("%Y-%m-%d-%H.%M.%S")
    EnvStackList = EnvStackslist(EnvPrefix)

    for stack in EnvStackList:
        try:
            current_template = cloudformation.get_template(
                                StackName=stack,
                                TemplateStage='Processed'
                                )

            bucket_name = BucketName
            cf = current_template['TemplateBody']
            if type(cf) == type(OrderedDict()):
                file_name = stack + '.json'
                cfn = json.dumps(cf, indent=4)
                s3_path = EnvPrefix + timestamp + "/" + file_name

                s3.Bucket(bucket_name).put_object(Key=s3_path, Body=cfn)

            elif type(cf) == type(''):
                file_name = stack + '.yml'
                s3_path = EnvPrefix + timestamp + "/" + file_name

                s3.Bucket(bucket_name).put_object(Key=s3_path, Body=cf)
        except ClientError as e:
            print("ClientError", e)
        except ParamValidationError as e:
            print("ParamValidationError", e)
        except KeyError as e:
            print("KeyError", e)

    print("Successfully Uploaded" + EnvPrefix + "templates")
