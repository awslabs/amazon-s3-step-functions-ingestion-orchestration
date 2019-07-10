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
jobname = os.environ['jobname']
depcrawler = os.environ['depcrawler']
loancrawler = os.environ['loancrawler']
invcrawler = os.environ['invcrawler']
shipcrawler = os.environ['shipcrawler']

client = boto3.client('glue')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def handler(event, context):
    try:
        if event['RequestType'] == 'Create':
            crawlers = [depcrawler, loancrawler, invcrawler, shipcrawler]
            crawler_count = 0
            LOGGER.info("creating a cloudformation custom resource")
            #for i in range(len(crawlers)):
            #    check = check_crawler_status(crawlers[i])
            #    if check == "Succeeded":
            #        crawler_count = crawler_count + 1
            #        LOGGER.info("Crawler Count is : " + crawler_count)

            #if (crawler_count == len(crawlers)):
            if(start_job_run() == "Succeeded"):
                send_response(event, context, "SUCCESS",
                            {"Message": "Glue Job started successfully" })
            elif(start_job_run() == "Failed"):
                send_response(event, context, "FAILED",
                              {"Message": " Glue Jobs failed to start" })
        elif event['RequestType'] == 'Update':
            LOGGER.info('UPDATE!')
            #:Todo Update JOB Configuration
            send_response(event, context, "SUCCESS",
                          {"Message": "Glue Job updated successful!"})
        elif event['RequestType'] == 'Delete':
            LOGGER.info('DELETE!')
            #:Todo Delete Job Configuration
            send_response(event, context, "SUCCESS",
                          {"Message": "Glue Job Custom Resource deletion successful!"})
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

def check_crawler_status(crawler):
    try:
        LOGGER.info("Checking Glue Crawler  " + crawler + " for Last Crawl Status")
        resp = client.get_crawler(Name=str(crawler))
        LOGGER.info("One")
        LOGGER.info("Received response for : " + str(crawler) + " : " +str(resp))
        #crawler_status = resp['Crawler']['LastCrawl']['Status']
        #LOGGER.info("Last Crawl status for  " + str(crawler) + " is " + str(crawler_status))
        return "Succeeded"
    except Exception as e:
        print("Message- " + str(e))
        raise e
    return "Failed"

def start_job_run():
    try:
        LOGGER.info('Starting Glue Job Run ' + jobname)
        response = client.start_job_run(JobName=jobname)
        LOGGER.info("Started Glue Job " + jobname + " with response " + str(response))
        return "Succeeded"
    except Exception as e:
        print("Message " + str(e))
        raise e
    return "Failed"
