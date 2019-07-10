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
import boto3


LOGGER = logging.getLogger("com.aws.etl")
LOGGER.setLevel(logging.INFO)
LOGGER.info('Loading function')

def lambda_handler(event, context):
    '''lambda handler for polling emr cluster status'''
    emr = boto3.client('emr')
    sns = boto3.client('sns')
    sns_topic = os.environ['sns_topic']
    # Log the received event
    event_received = str(json.dumps(event, indent=2))
    task_events = json.loads(event_received)
    LOGGER.info("Received event: " + str(task_events))
    #get cluster id from event
    LOGGER.info("CLUSTER_ID :" + str(task_events['cluster_id']))
    input_cluster_id = task_events['cluster_id']
    LOGGER.info("input_cluster_id :" + str(input_cluster_id))
    try:
        # describe emr cluster to fetch status
        response = emr.describe_cluster(ClusterId=input_cluster_id)
        LOGGER.info(response['Cluster']['Status']['State'])
        cluster_state = str(response['Cluster']['Status']['State'])
        if cluster_state == 'WAITING':
            LOGGER.info("Sending waiting cluster notification to SNS topic")
            message = "EMR cluster " + input_cluster_id  + " is in waiting state "
            sns.publish(Message=message, TopicArn=sns_topic)
            LOGGER.info("Response: {}".format(response))
            return 'OK'
        elif cluster_state == 'BOOTSTRAPPING' or cluster_state == 'STARTING' or cluster_state == 'RUNNING':
            LOGGER.info("Sending BSR notification to SNS topic")
            message = "EMR cluster " + input_cluster_id  + " is in starting / bootstrapping / running state "
            sns.publish(Message=message, TopicArn=sns_topic)
            LOGGER.info("Response: {}".format(response))
            return 'BOOTSTRAPPING/STARTING/RUNNING'
        elif cluster_state == 'TERMINATING' or cluster_state == 'TERMINATED' or cluster_state == 'TERMINATED_WITH_ERRORS':
            LOGGER.info("Sending TTT notification to SNS topic")
            message = "Failed EMR cluster " + input_cluster_id  + " is in terminating / terminated state "
            sns.publish(Message=message, TopicArn=sns_topic)
            LOGGER.info("Response: {}".format(response))
            return 'FAILED'
        else:
            return 'FAILED'
    except Exception as err:
        LOGGER.error("Error getting Cluster status %s " + str(err))
