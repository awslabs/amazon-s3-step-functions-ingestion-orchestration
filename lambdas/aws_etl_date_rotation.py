'''
#Copyright 2011-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
'''
import os
import json
from datetime import datetime, timedelta
import logging
import boto3
from boto3.dynamodb.conditions import Attr

LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

REGION = os.environ['region']
TABLE_NAME = os.environ['table_name']
SNS_TOPIC = os.environ['sns_topic']
SNS = boto3.client('sns')
RUNNING_JOB_STATUS = "RUNNING"
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
DYNAMODB = boto3.resource("dynamodb", region_name=REGION)
CONFIG_TABLE = DYNAMODB.Table(TABLE_NAME)

def lambda_handler(event, context):
    '''Lambda handler'''
    current_time = datetime.now()
    lastdate_time = current_time - timedelta(days=1)
    load_date = current_time.strftime("%Y-%m-%d")
    #start load time
    job_start_load_time = lastdate_time.strftime("%Y%m%d %H:%M:%S")
    #stop load time
    job_stop_load_time = current_time.strftime("%Y%m%d %H:%M:%S")
    #validate jobs to run - check for cloudwath double trigger
    if check_if_jobs_can_run() == 'true':
        LOGGER.info("LOAD-DATE : %s" + str(load_date))
        LOGGER.info("START-LOAD-TIME : %s" + str(job_start_load_time))
        LOGGER.info("STOP-LOAD-TIME : %s" + str(job_stop_load_time))
        #get events from cloudwatch events
        input = str(json.dumps(event, indent=2))
        input_event = json.loads(input)
        #get size of all jobs to execute
        size = str(input_event['iterator']['count'])
        current_index = str(input_event['iterator']['index'])
        # for each job in input cloudwatch event get job status and update appropriately
        for i in range(int(current_index), int(size)):
            job_name = input_event[str(i)]
            try:
                get_response = CONFIG_TABLE.get_item(Key={'job_name': job_name})
                LOGGER.info(get_response)
                item = get_response['Item']
                job_status = item['job_status']
                LOGGER.info("Job Status : " + job_status)
            except Exception as err:
                LOGGER.error("Encountered an error %s " +str(err))
                return 'FAIL'
            '''This should execute on subsequent runs and increment incremental
            windows for successful jobs
            '''
            if job_status == "SUCCESS":
                try:
                    #get previous window_load_stop and use as new window_load_start
                    window_load_stop_response = CONFIG_TABLE.get_item(Key={'job_name': job_name})
                    item = window_load_stop_response['Item']
                    job_start_load_time = item['window_load_stop']
                    ddb_response = CONFIG_TABLE.update_item(Key={'job_name': job_name}, UpdateExpression="set load_date = :c, window_load_start = :d, window_load_stop = :e, job_status = :f", ExpressionAttributeValues={':c': load_date, ':d': job_start_load_time, ':e': job_stop_load_time, ':f': RUNNING_JOB_STATUS}, ReturnValues="UPDATED_NEW")
                    LOGGER.info("DDB RESPONSE : %s" + str(ddb_response))
                except Exception as err:
                    LOGGER.error("Encountered an error %s " + str(err))
                    return 'FAIL'
            #This should execute only on the first run and update incremental windows
            elif job_status == "PENDING":
                try:
                    window_load_start = "18001105 00:00:00"
                    ddb_response = CONFIG_TABLE.update_item(Key={'job_name': job_name}, UpdateExpression="set load_date = :c, window_load_start = :d, window_load_stop = :e, job_status = :f", ExpressionAttributeValues={':c': load_date, ':d': window_load_start, ':e': job_stop_load_time, ':f': RUNNING_JOB_STATUS}, ReturnValues="UPDATED_NEW")
                    LOGGER.info("DDB RESPONSE : %s" + str(ddb_response))
                except Exception as err:
                    LOGGER.error("Encountered an error %s " +str(err))
                    return 'FAIL'
            #this should execute on subsequent run but will skip updates to incremental windows for failed jobs
            elif job_status == "FAILED":
                try:
                    LOGGER.info("Skipping ... updates to incremental window for " + str(item) + " JOB STATUS  : " + job_status)
                    ddb_response = CONFIG_TABLE.update_item(Key={'job_name': job_name}, UpdateExpression="set load_date = :c, job_status = :f", ExpressionAttributeValues={':c': load_date, ':f': RUNNING_JOB_STATUS}, ReturnValues="UPDATED_NEW")
                except Exception as err:
                    LOGGER.error("Encountered an error %s " +str(err))
                    return "FAIL"
        return "DONE"

    elif check_if_jobs_can_run() == 'false':
        LOGGER.info("Jobs are in running phase, Terminating Step Function %s")
        message = "Jobs are in running phase and Step Function is Terminating"
        SNS.publish(Message=message, TopicArn=SNS_TOPIC)
        return "DUPLICATED RUN"
def check_if_jobs_can_run():
    '''
    Check to mitigate the triggering of jobs twice from Cloudwatch Rule
    '''
    job_status_keys = CONFIG_TABLE.scan(FilterExpression=Attr('job_status').eq(RUNNING_JOB_STATUS))
    count = job_status_keys['Count']
    LOGGER.info("Count of running Jobs %s" + str(count))
    if count == 0:
        return 'true'
    return  'false'
