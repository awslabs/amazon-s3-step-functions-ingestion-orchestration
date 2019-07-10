'''ETL Glue Crawler Lambda function Custom Resource '''

import json
import logging
import signal
from urllib2 import build_opener, HTTPHandler, Request
import boto3
import uuid
import time
import os


print('Loading function')
depcrawler = os.environ['depcrawler'].strip()
loancrawler = os.environ['loancrawler'].strip()
invcrawler = os.environ['invcrawler'].strip()
shipcrawler = os.environ['shipcrawler'].strip()

client = boto3.client('glue')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
def handler(event, context):
    try:
        if event['RequestType'] == 'Create':
            LOGGER.info('creating a cloudformation custom resource!')
            if(start_glue_crawlers() == "Succeeded"):
                send_response(event, context, "SUCCESS",
                            {"Message": "Glue crawlers started successfull" })
            elif(start_glue_crawlers() == "Failed"):
                send_response(event, context, "FAILED",
                              {"Message": "One or more Glue crawlers failed to start" })
        elif event['RequestType'] == 'Update':
            LOGGER.info('UPDATE!')
            #:Todo Update JOB Configuration
            send_response(event, context, "SUCCESS",
                          {"Message": "Glue Crawlers updated successful!"})
        elif event['RequestType'] == 'Delete':
            LOGGER.info('DELETE!')
            #:Todo Delete Job Configuration
            send_response(event, context, "SUCCESS",
                          {"Message": "Glue crawler Custom Resource deletion successful!"})
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
    LOGGER.info('ResponseURL: %s', event['ResponseURL'])
    LOGGER.info('ResponseBody: %s', response_body)
    opener = build_opener(HTTPHandler)
    request = Request(event['ResponseURL'], data=response_body)
    request.add_header('Content-Type', '')
    request.add_header('Content-Length', len(response_body))
    request.get_method = lambda: 'PUT'
    response = opener.open(request)
    LOGGER.info("Status code: %s", response.getcode())
    LOGGER.info("Status message: %s", response.msg)
def start_glue_crawlers():
    try:
        LOGGER.info("Starting Glue Crawler " + depcrawler)
        response = client.start_crawler(Name=depcrawler)
        LOGGER.info("Started Glue Crawler : " +  depcrawler + " with response " + str(response))
        LOGGER.info("Starting Glue Crawler " + loancrawler)
        response = client.start_crawler(Name=loancrawler)
        LOGGER.info("Started Glue Crawler : " +  loancrawler + " with response " + str(response))
        LOGGER.info("Starting Glue Crawler " + invcrawler)
        response = client.start_crawler(Name=invcrawler)
        LOGGER.info("Started Glue Crawler : " +  invcrawler + " with response " + str(response))
        LOGGER.info("Starting Glue Crawler " + shipcrawler)
        response = client.start_crawler(Name=shipcrawler)
        LOGGER.info("Started Glue Crawler : " +  shipcrawler + " with response " + str(response))
        return "Succeeded"
    except Exception as e:
        print("Message " + str(e))
        raise e
    return "Failed"
