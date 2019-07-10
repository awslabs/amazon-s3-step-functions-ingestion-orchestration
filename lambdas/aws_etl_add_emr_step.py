'''
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
'''
import os
import logging
import json
import time
import boto3


LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def lambda_handler(event, context):
    '''Get the received event from cloudwatch rule'''
    region = os.environ['region']
    input = json.dumps(event, indent=2)
    input_event = json.loads(input)
    LOGGER.info("Received event: %s" + str(input_event))
    #get config from dynamodb and feed into the step_spark_submit function
    table_name = os.environ['table_name']
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)
    #fetch the current index from cloudwath rule received event
    current_index = str(input_event['iterator']['index'])
    LOGGER.info("Current Index : %s" + current_index)
    job_name = input_event[current_index]
    LOGGER.info("Current Job Name : %s" + job_name)
    try:
        #get the job configuration from dynamodb using job name as key
        response = table.get_item(Key={'job_name': job_name})
    except Exception as error:
        LOGGER.info("Encountered and Error %s " +  str(error))
    else:
        item = response['Item']
        LOGGER.info("GetItem succeeded: %s" + str(item))
        cluster_id = str(input_event['cluster_id'])
        LOGGER.info("Cluster_id : %s" + cluster_id)
        try:
            step_ids = step_spark_submit(cluster_id, item['table_name'], item['window_load_start'], item['window_load_stop'], item['output_dir'], item['script_src'], item['window_db_column'], item['source_db'], item['partition_by_col'], item['lower_bound'], item['upper_bound'], item['num_partitions'])
            step_id = step_ids[0]
            LOGGER.info("StepId : " + step_id)
            LOGGER.info("Good now update Dynamodb with cluster_id %s")
            #Update configuration table with job's cluster id
            ddb_response = table.update_item(Key={'job_name': job_name}, UpdateExpression="set cluster_id = :c", ExpressionAttributeValues={':c':cluster_id}, ReturnValues="UPDATED_NEW")
            LOGGER.info("Update succeeded: %s" + str(ddb_response))
            return step_id
        except Exception as error:
            LOGGER.error("Error adding EMR step %s " +str(error))
def step_spark_submit(cluster_id, table_name, start_date, end_date, output_dir, script_src, window_db_column, src_db, partition_by_col, lower_bound, upper_bound, num_partitions):
    '''submits job to emr cluster'''
    partition_date = time.strftime("%Y/%m/%d/%H/")
    LOGGER.info("Output Directory %s" + output_dir+partition_date)
    #submit job to EMR cluster
    emr = boto3.client('emr')
    action = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[{'Name': 'ETLSparkBoto32Lambda', 'ActionOnFailure': 'CANCEL_AND_WAIT', 'HadoopJarStep': {'Jar':'command-runner.jar', 'Args':["spark-submit", script_src, output_dir+partition_date, table_name, start_date, end_date, window_db_column, src_db, partition_by_col, lower_bound, upper_bound, num_partitions]}}])
    LOGGER.info("Output Directory %s " + str(action['StepIds']))
    return action['StepIds']
