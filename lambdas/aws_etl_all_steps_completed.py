# Copyright 2011-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import os
import json
import logging
import boto3
from boto3.dynamodb.conditions import Key

LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def lambda_handler(event, context):
    '''completes state machine by sending notification'''
    emr = boto3.client('emr')
    sns = boto3.client('sns')
    region = os.environ['region']
    sns_topic = os.environ['sns_topic']
    table = os.environ['table']
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table)
    input_events = str(json.dumps(event, indent=2))
    LOGGER.info("Received event: %s" + input_events)
    params = json.loads(input_events)
    #fetch cluster id from input cloudwath event
    cluster_id = str(params['cluster_id'])
    LOGGER.info("Cluster ID :" + cluster_id)
    try:
        LOGGER.info("All steps completed !")
        LOGGER.info("Getting status from dynamodb history table")
        #Query history table with cluster id GSI to retrieve items
        response = table.query(IndexName='cluster_id-index', KeyConditionExpression=Key('cluster_id').eq(cluster_id))
        items = response['Items']
        LOGGER.info("Sending notification to SNS topic")
        message = " Job Status :  " + str(items)
        #send message to SNS topic
        sns.publish(Message=message, TopicArn=sns_topic)
        LOGGER.info("Shutting down emr cluster " + cluster_id)
        #terminate emr job flow
        shutdown_response = emr.terminate_job_flows(JobFlowIds=[cluster_id])
        LOGGER.info("All job flows terminated %s " + shutdown_response)
        return "OK"
    except Exception as err:
        LOGGER.error("An Error error has occurred %s" + str(err))
