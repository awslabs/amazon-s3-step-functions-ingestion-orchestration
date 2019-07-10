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
    '''creates emr cluster'''
    emr = boto3.client('emr')
    emr_name = os.environ['emr_name']
    LOGGER.info('emr_name :' + emr_name)
    release_label = os.environ['release_label']
    LOGGER.info('release_label :' + release_label)
    log_uri = os.environ['log_uri']
    LOGGER.info('log_uri :' + log_uri)
    ec2_keyname = os.environ['ec2_keyname']
    LOGGER.info('ec2_keyname :' + ec2_keyname)
    ec2_subnetid = os.environ['ec2_subnetid']
    LOGGER.info('ec2_subnetid :' + ec2_subnetid)
    master_sg = os.environ['master_sg']
    LOGGER.info('master_sg :' + master_sg)
    slave_sg = os.environ['slave_sg']
    LOGGER.info('slave_sg :' + slave_sg)
    serviceaccess_sg = os.environ['serviceaccess_sg']
    LOGGER.info('serviceaccess_sg :' + serviceaccess_sg)
    emr_step_location = os.environ['emr_step_location']
    LOGGER.info('emr_step_location :' + emr_step_location)
    emr_ba_location = os.environ['emr_ba_location']
    LOGGER.info('emr_ba_location :' + emr_ba_location)
    job_flowrole = os.environ['job_flowrole']
    LOGGER.info('job_flowrole :' + job_flowrole)
    service_role = os.environ['service_role']
    LOGGER.info('service_role :' + service_role)
    commandrunner_region = os.environ['commandrunner_region']
    LOGGER.info("Received event: " + json.dumps(event, indent=2))
    try:
        #run job flow to create emr cluster
        response = emr.run_job_flow(
            Name=emr_name,
            LogUri=log_uri,
            ReleaseLabel=release_label,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'MasterGroup',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm3.xlarge',
                        'InstanceCount': 1
                    },
                    {
                        'Name': 'CoreGroup',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm3.xlarge',
                        'InstanceCount': 2
                    }],
                        'Ec2KeyName': ec2_keyname,
                        'KeepJobFlowAliveWhenNoSteps': True,
                        'TerminationProtected': False,
                        'HadoopVersion': 'Amazon 2.8.4',
                        'Ec2SubnetId': ec2_subnetid,
                        'EmrManagedMasterSecurityGroup': master_sg,
                        'EmrManagedSlaveSecurityGroup': slave_sg,
                        'ServiceAccessSecurityGroup': serviceaccess_sg,

                    },
            Steps=[
                    {
                        'Name': 'AddDBLibs',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                        'Jar': commandrunner_region,
                        'Args': [emr_step_location,]}},],
            BootstrapActions=[
                    {
                        'Name': 'InstallBoto3',
                        'ScriptBootstrapAction': {
                        'Path': emr_ba_location
                    }},],
            Applications=[
                    {
                        'Name': 'Hive'
                    },
                    {
                        'Name': 'Pig'
                    },
                    {
                        'Name': 'Spark'
                    }],
            VisibleToAllUsers=True,
            JobFlowRole=job_flowrole,
            ServiceRole=service_role,
            Tags=[
                {
                    'Key': 'Name',
                    'Value': 'EMRFromPython'
                },
                {
                    'Key': 'department',
                    'Value': 'NONEMP'
                },
                {
                    'Key': 'applicationid',
                    'Value': '110000'
                },
                {
                    'Key': 'User',
                    'Value': 'ljdtyg'
                }])
        LOGGER.info("%s" + str(response))
        my_jobflow_id = response['JobFlowId']
        LOGGER.info("My Job Flow Id is %s" + str(my_jobflow_id))
        return my_jobflow_id
    except Exception as err:
        LOGGER.error("Error Creating Cluster %s" + str(err))
