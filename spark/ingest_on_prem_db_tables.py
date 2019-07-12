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
from __future__ import print_function
import sys
from pyspark.sql import SparkSession
import boto3
ssm = boto3.client('ssm')
if __name__ == "__main__":
    if len(sys.argv) != 11:
        print("args len" + str(len(sys.argv)))
        print("Usage: shpmnt_spark  ")
        exit(-1)
    spark = SparkSession.builder.appName("TestSparkOne").getOrCreate()
    print("args 1 = "+ sys.argv[1])
    print("args 2 = "+ sys.argv[2])
    print("args 3 = "+ sys.argv[3])
    print("args 4 = "+ sys.argv[4])
    print("args 5 = "+ sys.argv[5])
    print("args 6 = "+ sys.argv[6])
    print("args 7 = "+ sys.argv[7])
    print("args 8 = "+ sys.argv[8])
    print("args 9 = "+ sys.argv[9])
    print("args 10 = "+ sys.argv[10])

    user = ssm.get_parameter(Name='postgre-user', WithDecryption=True)['Parameter']['Value']
    psswd = ssm.get_parameter(Name='postgre-psswd', WithDecryption=True)['Parameter']['Value']
    jdbcurl = ssm.get_parameter(Name='postgre-jdbcurl', WithDecryption=True)['Parameter']['Value']
    connectionProperties = {"user":str(user) , "password": str(psswd), "driver": "org.postgresql.Driver"}

    if sys.argv[5] == "full_load":
        pushdown_query = "(select * from " +  sys.argv[2] + ") shipmt_alias_full"
    else:
        pushdown_query = "(select * from " + sys.argv[2] + " where " + sys.argv[5] + " > " + "'"+  sys.argv[3] + "'"+  " and " +  sys.argv[5] + " < " + "'"+ sys.argv[4] + "'"+ ") shpmt_alias"

    df = spark.read.jdbc(url=str(jdbcurl), table=pushdown_query, properties=connectionProperties, column=sys.argv[7], lowerBound=sys.argv[8], upperBound=sys.argv[9], numPartitions=sys.argv[10])
    #df = spark.read.format("jdbc").option(jdbcurl,
    df.write.parquet(sys.argv[1]+"/"+sys.argv[2]+".parquet", mode="overwrite")
    spark.stop()
