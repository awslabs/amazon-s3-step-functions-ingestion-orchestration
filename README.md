# Incremental Ingestion Pipeline Example for a Data Lake on AWS Cloud

### 1. Intent and Purpose
A datalake is a centralized repository that allows you to store structured and unstructured data at any scale. Business organizations require datalake because it has been shown that those organizations with datalake are able to retrieve and use actionable business intelligence from their lakes and outperform their peers.
There are four essential elements of a Datalake Analytics solution

##1. Data Movement (Batch and or Streaming)
##2. Data Catalog and Security
##3. Analytics
##4. Machine Learning

This project falls into the first element, which is the Data Movement and the intent is to provide an example pattern for designing an incremental ingestion pipeline on the AWS cloud using a AWS Step Functions and a combination of multiple AWS Services such as Amazon S3, Amazon DynamoDB, Amazon ElasticMapReduce and Amazon Cloudwatch Events Rule. This pattern does not replace what is already provided within AWS Glue and or Amazon Datapipeline, it only serves to provide an example pattern for Engineers who are interested in using a combination of AWS services to achieve a similar purpose.


### 2. Services

1. Amazon Cloudwatch Events
2. AWS Step StepFunctions
3. Amazon Lambda Functions
4. Amazon DynamoDB
5. Spark on Elastic MapReduce
6. Amazon S3
7. Amazon Aurora RDS
8. Amazon Cloudformation

### 3. Requirements

1. An Amazon Web Services Account
2. AWS CLI Installed and configured

### 4. Architecture Diagram

![alt text](https://github.com/awslabs/amazon-s3-step-functions-ingestion-orchestration/blob/master/IncrementalIngestionDataLake.png)


### 5. Two Major Steps

1. Create an Aurora RDS database (Optional if you have an existing Aurora Postgresql database)
2. Load data into RDS Aurora database tables
3. Execute Incremental Ingestion pipeline

### 6. Step By Step Setup

## Part I
####Prerequisites:
1. A VPC with at least one Private and Public Subnet
2.  An EC2 instance that can used as a Bastion Host for connection to the created database

#### Steps
1. Clone the repository
2. Create an S3 bucket <my-bucket> and sync the repository to the bucket. aws s3 sync . s3://<my-bucket>
3. Create a Aurora RDS database using this cloudformation template glue/postgredb.yml
4. Navigate to the /glue folder and open the aws-glue-etl-job.py, replace the values for database (etl) with your database name, save and upload to s3.
5. Create Glue Crawler and Job load  stack using the aws-etl-load-rds.yml cloudformation template.  This cloudformation stack will create Glue crawlers that will crawl the public s3 bucket locations(dfw-meeetup-emr/Deposits, Loans, Investments and Shipments) and a glue job to load data from the s3 bucket locations into the Aurora database you created. The data in these locations are made up.
6. Parameter Values for above

| Parameter Name |	Parameter Value |
|----------------|------------------|
|CFNConnectionName	| cfn-connection-spark-1 |
|CFNDatabaseName	| cfn-database-s3 |
|CFNDepositsCrawlerName	| cfn-crawler-spark-dep |
|CFNInvestmentsCrawlerName	| cfn-crawler-spark-inv |
|CFNJDBCPassword	| <Change Me> |
|CFNJDBCString	| <Change Me> |
|CFNJDBCUser	| <Change Me> |
|CFNJobName	| cfn-glue-job-s3-to-JDBC |
|CFNLoansCrawlerName	| cfn-crawler-spark-loa |
|CFNS3PATHDEPOSIT	| s3://dfw-meetup-emr/Deposits |
|CFNS3PATHINV	| s3://dfw-meetup-emr/Investments |
|CFNS3PATHLOAN	| s3://dfw-meetup-emr/Loans |
|CFNS3PATHSHIP	| s3://dfw-meetup-emr/Shipments |
|CFNScriptLocation	| s3://<ChangeMe>/aws-glue-etl-job.py |
|CFNShipmentsCrawlerName	| cfn-crawler-spark-shi |
|CFNTablePrefixName	| cfn_s3_sprk_1_ |
|GlueCrawlerCustomKey	| glue/aws-etl-start-crawler-custom-resource.py.zip |
|GlueCrawlerCustomModule	| aws-etl-start-crawler-custom-resource |
|GlueJobCustomKey	| glue/aws-etl-start-job-custom-resource.py.zip |
|GlueJobCustomModule	| aws-etl-start-job-custom-resource |
|S3Bucket	| <ChangeMe> |
|SubnetId |	<ChangeMe> |

#####At the end of this part we would have created
1. An AWS Aurora database
2. Created Glue Crawlers and Glue Job to populate AWS Aurora Database with Sample data
3. Successfully loaded data into Aurora database tables

The Aurora Database in this context represents the on premises database

## Part II

####Prerequisites:
1. An S3 Bucket
2. EC2 Key pair
3. VPC Private Subnet

![alt text](https://github.com/awslabs/amazon-s3-step-functions-ingestion-orchestration/blob/master/stepfunction.png)

#### Steps

1. Navigate to the cfn/aws-sns-topic.yml and use it to create an SNS topic. This creates a cloudformation export that its value are then imported into the aws-etl-stepfunction stack. Confirm the subscription.
2. Navigate to the cfn/aws-roles.yml and use it to create the roles that will be used by the step function , lambda  ETL process. This creates a cloudformation export whose values are then imported into the aws-etl-stepfunction stack.
3. Navigate to the cfn/emr-roles and use it to create the EMR roles. This creates a cloudformation export whose values are then imported into the aws-etl-stepfunction stack.
4. Navigate to the cfn/emr-security-groups.yml and use it to create EMR security groups. This creates a cloudformation export for the security groups and its values are  imported into the aws-etl-stepfunction stack.
5. Navigate to the lambdas folder and upload all the zip files to an S3 bucket location <my_bucket_name>/lambdas. aws s3 sync lambdas s3://<my-bucket>/lambdas/
6. Note the location and the names of the lambda functions , it will be used in the cloudformation stack to kick off the incremental ingestion execution run.
7. Create AWS your database secrets using below commands from the AWSCLI
aws ssm put-parameter --name postgre-psswd --type SecureString --value <P@ssw0rd>
aws ssm put-parameter --name postgre-user --type SecureString --value <admin>
aws ssm put-parameter --name postgre-jdbcurl --type String --value <jdbc:postgresql://<RDS-NAME>-instance.2.rds.amazonaws.com:5432/example>
This will be required from the sample spark script.
9. Download the postgresql jdbc jar https://jdbc.postgresql.org/download.html and upload it to an S3 location. Note this location.
aws s3 cp postgresql-42.2.6.jar s3://<my-bucket>/
10. Navigate to the ba folder in the repository, open the bootstrap-emr-step.sh and replace the value of the location of the postgresql jdbc jar, save the file and upload it to an s3 location.
aws s3 sync ba s3://<my-bucket>/ba/
aws s3 sync spark s3://<my-bucket>/spark/
11. Modify cfn/config.txt and replace the table names in columns 7,8 and 9 to yours. save and syn to s3 bucket folder
aws s3 sync cfn s3://<my-bucket>/cfn/
12. Navigate to the cfn/aws-etl-stepfunction.json template and the cfn/stepfunction-parameters.json file. Replace the parameter values with your own parameter values.

Parameters to change in stepfunction-parameters.json

| ParameterKey |	ParameterValue |
|--------------|-----------------|
| CreateEMRModuleName	| aws_etl_emr_cluster_create |
| AllJobsCompletedModule	| aws_etl_all_steps_completed |
| AllJobsCompletedS3Key	| lambdas/aws_etl_all_steps_completed.zip |
| ClusterStatusModuleName	| aws_etl_emr_cluster_status |
| ClusterStatusS3Key	| lambdas/aws_etl_emr_cluster_status.zip |
| configtable	| aws_etl_conf |
| CreateEMRS3Key	| lambdas/aws_etl_emr_cluster_create.zip |
| ec2keyname	| <Change-Me> |
| ec2subnetid	| <Change-Me> |
| emrbalocation	| s3://<Change-Me>/ba/bootstrap-emr.sh |
| emrname	| AWS_SF_ETL_CLUSTER |
| emrsteplocation	| s3://<Change-Me>/ba/bootstrap-emr-step.sh |
| EMRStepStatusModuleName	| aws_etl_emr_step_status |
| EMRStepStatusS3Key |	lambdas/aws_etl_emr_step_status.zip |
| EMRStepSubmitModuleName |	aws_etl_add_emr_step |
| EMRStepSubmitS3Key |	lambdas/aws_etl_add_emr_step.zip |
| GetNextEMRJobModule	| aws_etl_iterator |
| GetNextEMRJobS3Key |	lambdas/aws_etl_iterator.zip |
| historytable |	aws_etl_history |
| loguri | s3n://aws-logs-<MY-ACCOUNT_NUMBR>-us-west-2/elasticmapreduce |
| regionname | us-west-2 |
| releaselabel |	emr-5.17.0 |
| S3Bucket |	<Change-Me> |
| DDBConfigModule	| aws_etl_conf_jobs_custom_resource |
| DDBConfigS3Key |	lambdas/aws_etl_conf_jobs_custom_resource.zip
| CustomResourceS3Key	| cfn/config.txt |
| emrcommandrunnerscript |	s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar |
| Environment	| NonProd |
| SubEnvironment |	dev2 |
AccountName	aws-etl-state-machine
| ETLStateMachineDateRotationS3Key |	lambdas/aws_etl_date_rotation.zip |
| ETLStateMachineDateRotationModuleName	| aws_etl_date_rotation |


13. Navigate to the AWS management console for Cloudformation and browse to the cfn folder,, load the aws-roles.yml to create the roles that will be used by the pipeline.
14. Modify the config.txt replace the bucket name values with your bucket name.


| job_name | load_date | load_window_start | load_window_stop | job_flow_id | job_status | output_dir | script_source | database_name | table_name | window_db_column | partition_by_col | lower_bound | upper_bound | num_partitions |
|-------|-------|-------------|---------------|------------|--------|--------------|------------|-------|-------|-------|-------|-------|-------|-------|
| deposit |11/5/18 | 2018-11-04 00:00:000 | 2018-11-05 00:00:000 | j-0000000000000 | PENDING | s3://my-bucketholder/RAW/ | s3://my-bucketholder/spark/ingest_on_prem_db_tables.py | spark | cfn_s3_sprk_1_deposits | shipmt_date_tstmp | quarter | 1 | 1000 | 10 |
| investment | 11/5/18 | 2018-11-04 00:00:000 | 2018-11-05 00:00:000 | j-0000000000000 | PENDING | s3://my-bucketholder/RAW/ | s3://my-bucketholder/spark/ingest_on_prem_db_tables.py | spark | cfn_s3_sprk_1_investments | shipmt_date_tstmp | quarter | 1 | 1000 | 10 |
| loan | 11/5/18 | 2018-11-04 00:00:000 | 2018-11-05 00:00:000 | j-0000000000000 | PENDING | s3://my-bucketholder/RAW/ | s3://my-bucketholder/spark/ingest_on_prem_db_tables.py | spark | cfn_s3_sprk_1_loans | shipmt_date_tstmp | quarter | 1 | 1000 | 10 |
| shipment | 11/5/18 | 2018-11-04 00:00:000 | 2018-11-05 00:00:000 | j-0000000000000 | PENDING | s3://my-bucketholder/RAW/ | s3://my-bucketholder/spark/ingest_on_prem_db_tables.py | spark | cfn_s3_sprk_1_shipments | shipmt_date_tstmp | quarter | 1 | 1000 | 10 |


15. Navigate to the CFN folder, From the AWS command line execute below command to create the cloudformation stack.


aws cloudformation create-stack --stack-name gwfstepfunction --template-body file://aws-etl-stepfunction.json  --region us-west-2 --capabilities CAPABILITY_IAM  --parameters file://stepfunction-parameters.json

Below is an example folder structure for an S3 datalake

![alt text](https://github.com/awslabs/amazon-s3-step-functions-ingestion-orchestration/blob/master/S3BucketDatalakeExampleLayout.png)

RAW (immutable)
•	RAW-us-east-1/sourcename/tablename/original/full (full load)
partitioned by arrival date as-is
•	RAW-us-east-1/sourcename/tablename/original/incremental (changes/updates/inserts/deletes)
partitioned by arrival date as-is incoming format
•	FORMAT-us-east-1/sourcename/tablename/masked/full (w/sensitive data masked, if any) partitioned by arrival date as-is incoming format
•	FORMAT-us-east-1/sourcename/tablename/masked/incremental (w/sensitive data masked, if any) partitioned by arrival date as-is

FORMAT (mutable)
•	FORMAT-us-east-1/sourcename/tablename/original/full (w/ original data)
	partitioned by
•	xx-FORMAT-us-east-1/sourcename/tablename/original/incremental (w/ original data.
•	xx-FORMAT-us-east-1/sourcename/tablename/masked/full (w/ sensitive data masked, if any.
•	xx-FORMAT-us-east-1/sourcename/tablename/masked/incremental (w/ sensitive data masked, if any.

####At the end of this part we would have created the following:
1. An EMR Cluster
2. Two DynamoDB Tables (Config and History)
3. AWS Step Function State machine
4. Eight Lambda Functions
5. AWS Events ScheduledRule
6. A Cloudformation Lambda function Custom Resource
7. SSM Parameters

Confirm that data has been added to the output directory that you specified in your config.txt.

Now it is time to tear down the Cloudformation stacks and delete the dynamodb tables.
