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
import logging
import json
import uuid
import time
import boto3


LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def lambda_handler(event, context):
    '''lambda handler'''
    emr = boto3.client('emr')
    sns = boto3.client('sns')
    sns_topic = os.environ['sns_topic']
    region = os.environ['region']
    dynamodb = boto3.resource("dynamodb", region_name=region)
    # Log the received event
    input = str(json.dumps(event, indent=2))
    LOGGER.info("Received event: " + input)
    task_events = json.loads(input)
    LOGGER.info("Task events" + str(task_events))
    cluster_id = str(task_events['cluster_id'])
    step_id = str(task_events['step_id'])
    LOGGER.info("cluster_id :" + cluster_id)
    LOGGER.info("step_id :" + step_id)
    #fetch current job from cloudwatch event rule
    current_index = str(task_events['iterator']['index'])
    LOGGER.info("Current Index :" + current_index)
    job_name = task_events[current_index]
    LOGGER.info("Current Job Name : " + job_name)
    #Get history table to write step status
    table = dynamodb.Table(os.environ['history_table'])
    history_id = str(uuid.uuid4())
    history_load_date = str(time.strftime("%Y/%m/%d/%H/"))
    config_table = dynamodb.Table(os.environ['config_table'])
    #Update config with step status
    try:
        #describe emr step for step status
        response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        LOGGER.info("%s" + response['Step']['Status']['State'])
        step_state = str(response['Step']['Status']['State'])
        if step_state == 'COMPLETED':
            LOGGER.info("Step completed  successfully : %s")
            LOGGER.info("Inserting status into Job History %s")
            response = table.put_item(Item={'uuid': history_id, 'load_date' : history_load_date, 'job_name' : job_name, 'status': 'SUCCESS', 'cluster_id' : cluster_id, 'step_id' : step_id})
            LOGGER.info("Status history created successfully")
            ddb_response = config_table.update_item(Key={'job_name': job_name}, UpdateExpression="set job_status = :c", ExpressionAttributeValues={':c':'SUCCESS'}, ReturnValues="UPDATED_NEW")
            LOGGER.info("Job Config status for step id %s" + step_id  + " update to Success %s" + str(ddb_response))
            LOGGER.info("Sending notification to SNS topic")
            message = "EMR cluster step " + step_id  + " on cluster " +  cluster_id  + " has completed "
            sns.publish(Message=message, TopicArn=sns_topic)
            LOGGER.info("Response: {}".format(response))
            return "OK"
        elif step_state == 'PENDING' or step_state == 'RUNNING':
            LOGGER.info("EMR cluster step %s " + step_id  + " on cluster " +  cluster_id + " is in " + step_state)
            return step_state
        else:
            LOGGER.info("Job has failed %s" + step_state)
            LOGGER.info("Inserting status in Job history table")
            response = table.put_item(Item={'uuid': history_id, 'load_date' : history_load_date, 'job_name' : job_name, 'status': 'FAILED', 'cluster_id' : cluster_id, 'step_id' : step_id})
            LOGGER.info("Updating config table with status %s")
            ddb_response = config_table.update_item(Key={'job_name': job_name}, UpdateExpression="set job_status = :c", ExpressionAttributeValues={':c':"FAILED"}, ReturnValues="UPDATED_NEW")
            LOGGER.info("Job Config status for step id %s " + step_id  + " update to Failed")
            return "FAILED"
    except Exception as err:
        LOGGER.error("Error getting Cluster status %s" + str(err))
