'''ETL Dynamodb Config Table Lambda function Custom Resource '''
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
import csv
import logging
from urllib.request import urlopen, Request, HTTPError, URLError
import boto3

LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def handler(event, context):
    try:
        if event['RequestType'] == 'Create':
            LOGGER.info('creating a cloudformation custom resource!')
            if(insert_job_configuration() == "done"):
                send_response(event, context, "SUCCESS",
                            {"Message": "Dynamodb config table : " + os.environ['table_name'] + " loaded successfully"})
            elif(insert_job_configuration() == "fail"):
                send_response(event, context, "FAILED",
                              {"Message": "Loading Dynamodb config table : " + os.environ['table_name'] + " failed"})
        elif event['RequestType'] == 'Update':
            LOGGER.info('UPDATE!')
            #Todo: Update JOB Configuration
            #However very risky if not managed properly as this can
            #potentially overwrite what is in the dynamodb table if the contents of the dynaodb config.txt is not updated
            #Recommendation is to update the config table outside of the stack
            send_response(event, context, "SUCCESS",
                          {"Message": "Dynamodb config table updated successful!"})
        elif event['RequestType'] == 'Delete':
            LOGGER.info('DELETE!')
            #:Todo Delete Job Configuration Not neccessary as a stack deletion will delete the  table
            send_response(event, context, "SUCCESS",
                          {"Message": "ETL COnfig Dynamodb Custom Resource deletion successful!"})
        else:
            LOGGER.info('FAILED!')
            #:Todo
            send_response(event, context, "FAILED",
                          {"Message": "Unexpected event received from CloudFormation"})
    except: #pylint: disable=W0702
        LOGGER.info('FAILED!')
        send_response(event, context, "FAILED", {
            "Message": "Exception during processing"})

def send_response(event, context, response_status, response_data):
    response_body = json.dumps({
        "Status": response_status,
        "Reason":  context.log_stream_name,
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event['StackId'],
        "RequestId": event['RequestId'],
        "LogicalResourceId": event['LogicalResourceId'],
        "Data": response_data
    })
    binary_data = response_body.encode('utf-8')
    LOGGER.info('ResponseURL: %s', event['ResponseURL'])
    LOGGER.info('ResponseBody: %s', response_body)
    request = Request(event['ResponseURL'], data=binary_data)
    request.add_header('Content-Type', '')
    request.add_header('Content-Length', len(response_body))
    request.get_method = lambda: 'PUT'
    try:
        urlopen(request)
    except HTTPError as err:
        LOGGER.error("Callback to CFN API failed with status %d" % err.code)
        LOGGER.error("Response: %s" % err.reason)
    except URLError as err2:
        LOGGER.error("Failed to reach the server - %s" % err2.reason)


def insert_job_configuration():
    dynamodb = boto3.resource("dynamodb", region_name=os.environ['region_name'])
    table = dynamodb.Table(os.environ['table_name'])
    s3 = boto3.client('s3')
    s3Resource = boto3.resource('s3')
    s3_bucket = os.environ['s3_bucket']
    s3_object_key = os.environ['s3_object_key']
    try:
        #inserts all job config into dynamodb table
        s3Resource.Bucket(s3_bucket).download_file(s3_object_key, '/tmp/blog-etl-config.csv')
        with open('/tmp/blog-etl-config.csv', newline='\n') as csvfile:
            configreader = csv.reader(csvfile, delimiter=',')
            for record in configreader:
                #job_name,job_status,load_date,output_dir,script_src,source_db,table_name,window_db_column,window_load_start,window_load_stop = str(row).split(",")
                #record = record.replace("'","")
                response = table.put_item(
                Item={
                    'job_name': str(record[0]).replace("'", "").strip(),
    	               'load_date' : str(record[1]).replace("'", "").strip(),
    	                   'window_load_start' : str(record[2]).replace("'", "").strip(),
    	                       'window_load_stop': str(record[3]).replace("'", "").strip(),
    	                          'cluster_id' : str(record[4]).replace("'", "").strip(),
    	                             'job_status': str(record[5]).replace("'", "").strip(),
    	                                'output_dir' : str(record[6]).replace("'", "").strip(),
                                            'script_src' : str(record[7]).replace("'", "").strip(),
    	                                       'source_db' : str(record[8]).replace("'", "").strip(),
    	                                          'table_name' : str(record[9]).replace("'", "").replace("]", "").strip(),
                                                        'window_db_column' : str(record[10]).replace("'", "").replace("]", "").strip(),
                                                            'partition_by_col' : str(record[11]).replace("'", "").replace("]", "").strip(),
                                                                'lower_bound' : str(record[12]).replace("'", "").replace("]", "").strip(),
                                                                    'upper_bound' : str(record[13]).replace("'", "").replace("]", "").strip(),
                                                                        'num_partitions' : str(record[14]).replace("'", "").strip()
                                                                    }
                                                                    )
                LOGGER.info("Response is " + str(response))
        return "done"
    except Exception as err:
        LOGGER.error("Encountered an exception " + str(err))
    return "fail"
