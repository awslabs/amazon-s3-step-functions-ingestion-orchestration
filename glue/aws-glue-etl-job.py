import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "sampledb", table_name = "postgreglue_public_loans", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
table1 =  "cfn_s3_sprk_1_loans"
table2 =  "cfn_s3_sprk_1_shipments"
table3 =  "cfn_s3_sprk_1_investments"
table4 =  "cfn_s3_sprk_1_deposits"
dbName = "cfn-database-s3"
connectionName = "cfn-connection-spark-1"
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = dbName, table_name = table1, transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = dbName, table_name = table2, transformation_ctx = "datasource1")
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = dbName, table_name = table3, transformation_ctx = "datasource2")
datasource3 = glueContext.create_dynamic_frame.from_catalog(database = dbName, table_name = table4, transformation_ctx = "datasource3")
## @type: ApplyMapping
## @args: [mapping = [("dest_ma", "long", "dest_ma", "long"), ("dest_state", "long", "dest_state", "long"), ("orig_state", "long", "orig_state", "long"), ("orig_ma", "long", "orig_ma", "long"), ("shipmt_dist_routed", "long", "shipmt_dist_routed", "long"), ("shipmt_wght", "long", "shipmt_wght", "long"), ("shipmt_id", "long", "shipmt_id", "long"), ("shipmt_date_tstmp", "timestamp", "shipmt_date_tstmp", "timestamp"), ("export_yn", "string", "export_yn", "string"), ("mode", "long", "mode", "long"), ("shipmt_date_str", "string", "shipmt_date_str", "string"), ("wgt_factor", "double", "wgt_factor", "double"), ("shipmt_value", "long", "shipmt_value", "long"), ("temp_cntl_yn", "string", "temp_cntl_yn", "string"), ("sctg", "string", "sctg", "string"), ("export_cntry", "string", "export_cntry", "string"), ("hazmat", "string", "hazmat", "string"), ("orig_cfs_area", "string", "orig_cfs_area", "string"), ("dest_cfs_area", "string", "dest_cfs_area", "string"), ("shipmt_dist_gc", "long", ##"shipmt_dist_gc", "long"), ("naics", "long", "naics", "long"), ("quarter", "long", "quarter", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("dest_ma", "long", "dest_ma", "long"), ("dest_state", "long", "dest_state", "long"), ("orig_state", "long", "orig_state", "long"), ("orig_ma", "long", "orig_ma", "long"), ("shipmt_dist_routed", "long", "shipmt_dist_routed", "long"), ("shipmt_wght", "long", "shipmt_wght", "long"), ("shipmt_id", "long", "shipmt_id", "long"), ("shipmt_date_tstmp", "timestamp", "shipmt_date_tstmp", "timestamp"), ("export_yn", "string", "export_yn", "string"), ("mode", "long", "mode", "long"), ("shipmt_date_str", "string", "shipmt_date_str", "string"), ("wgt_factor", "double", "wgt_factor", "double"), ("shipmt_value", "long", "shipmt_value", "long"), ("temp_cntl_yn", "string", "temp_cntl_yn", "string"), ("sctg", "string", "sctg", "string"), ("export_cntry", "string", "export_cntry", "string"), ("hazmat", "string", "hazmat", "string"), ("orig_cfs_area", "string", "orig_cfs_area", "string"), ("dest_cfs_area", "string", "dest_cfs_area", "string"), ("shipmt_dist_gc", "long", "shipmt_dist_gc", "long"), ("naics", "long", "naics", "long"), ("quarter", "long", "quarter", "long")], transformation_ctx = "applymapping1")
applymapping2 = ApplyMapping.apply(frame = datasource0, mappings = [("dest_ma", "long", "dest_ma", "long"), ("dest_state", "long", "dest_state", "long"), ("orig_state", "long", "orig_state", "long"), ("orig_ma", "long", "orig_ma", "long"), ("shipmt_dist_routed", "long", "shipmt_dist_routed", "long"), ("shipmt_wght", "long", "shipmt_wght", "long"), ("shipmt_id", "long", "shipmt_id", "long"), ("shipmt_date_tstmp", "timestamp", "shipmt_date_tstmp", "timestamp"), ("export_yn", "string", "export_yn", "string"), ("mode", "long", "mode", "long"), ("shipmt_date_str", "string", "shipmt_date_str", "string"), ("wgt_factor", "double", "wgt_factor", "double"), ("shipmt_value", "long", "shipmt_value", "long"), ("temp_cntl_yn", "string", "temp_cntl_yn", "string"), ("sctg", "string", "sctg", "string"), ("export_cntry", "string", "export_cntry", "string"), ("hazmat", "string", "hazmat", "string"), ("orig_cfs_area", "string", "orig_cfs_area", "string"), ("dest_cfs_area", "string", "dest_cfs_area", "string"), ("shipmt_dist_gc", "long", "shipmt_dist_gc", "long"), ("naics", "long", "naics", "long"), ("quarter", "long", "quarter", "long")], transformation_ctx = "applymapping2")
applymapping3 = ApplyMapping.apply(frame = datasource0, mappings = [("dest_ma", "long", "dest_ma", "long"), ("dest_state", "long", "dest_state", "long"), ("orig_state", "long", "orig_state", "long"), ("orig_ma", "long", "orig_ma", "long"), ("shipmt_dist_routed", "long", "shipmt_dist_routed", "long"), ("shipmt_wght", "long", "shipmt_wght", "long"), ("shipmt_id", "long", "shipmt_id", "long"), ("shipmt_date_tstmp", "timestamp", "shipmt_date_tstmp", "timestamp"), ("export_yn", "string", "export_yn", "string"), ("mode", "long", "mode", "long"), ("shipmt_date_str", "string", "shipmt_date_str", "string"), ("wgt_factor", "double", "wgt_factor", "double"), ("shipmt_value", "long", "shipmt_value", "long"), ("temp_cntl_yn", "string", "temp_cntl_yn", "string"), ("sctg", "string", "sctg", "string"), ("export_cntry", "string", "export_cntry", "string"), ("hazmat", "string", "hazmat", "string"), ("orig_cfs_area", "string", "orig_cfs_area", "string"), ("dest_cfs_area", "string", "dest_cfs_area", "string"), ("shipmt_dist_gc", "long", "shipmt_dist_gc", "long"), ("naics", "long", "naics", "long"), ("quarter", "long", "quarter", "long")], transformation_ctx = "applymapping3")
applymapping4 = ApplyMapping.apply(frame = datasource0, mappings = [("dest_ma", "long", "dest_ma", "long"), ("dest_state", "long", "dest_state", "long"), ("orig_state", "long", "orig_state", "long"), ("orig_ma", "long", "orig_ma", "long"), ("shipmt_dist_routed", "long", "shipmt_dist_routed", "long"), ("shipmt_wght", "long", "shipmt_wght", "long"), ("shipmt_id", "long", "shipmt_id", "long"), ("shipmt_date_tstmp", "timestamp", "shipmt_date_tstmp", "timestamp"), ("export_yn", "string", "export_yn", "string"), ("mode", "long", "mode", "long"), ("shipmt_date_str", "string", "shipmt_date_str", "string"), ("wgt_factor", "double", "wgt_factor", "double"), ("shipmt_value", "long", "shipmt_value", "long"), ("temp_cntl_yn", "string", "temp_cntl_yn", "string"), ("sctg", "string", "sctg", "string"), ("export_cntry", "string", "export_cntry", "string"), ("hazmat", "string", "hazmat", "string"), ("orig_cfs_area", "string", "orig_cfs_area", "string"), ("dest_cfs_area", "string", "dest_cfs_area", "string"), ("shipmt_dist_gc", "long", "shipmt_dist_gc", "long"), ("naics", "long", "naics", "long"), ("quarter", "long", "quarter", "long")], transformation_ctx = "applymapping4")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
resolvechoice3 = ResolveChoice.apply(frame = applymapping2, choice = "make_cols", transformation_ctx = "resolvechoice3")
resolvechoice4 = ResolveChoice.apply(frame = applymapping3, choice = "make_cols", transformation_ctx = "resolvechoice4")
resolvechoice5 = ResolveChoice.apply(frame = applymapping4, choice = "make_cols", transformation_ctx = "resolvechoice5")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
dropnullfields4 = DropNullFields.apply(frame = resolvechoice3, transformation_ctx = "dropnullfields4")
dropnullfields5 = DropNullFields.apply(frame = resolvechoice4, transformation_ctx = "dropnullfields5")
dropnullfields6 = DropNullFields.apply(frame = resolvechoice5, transformation_ctx = "dropnullfields6")
## @type: DataSink
## @args: [catalog_connection = "Postgresql-JDBC", connection_options = {"dbtable": "postgreglue_public_loans", "database": "spark"}, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = connectionName, connection_options = {"dbtable": table1, "database": "blog1"}, transformation_ctx = "datasink4")
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields4, catalog_connection = connectionName, connection_options = {"dbtable": table2, "database": "blog1"}, transformation_ctx = "datasink5")
datasink6 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields5, catalog_connection = connectionName, connection_options = {"dbtable": table3, "database": "blog1"}, transformation_ctx = "datasink6")
datasink7 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields6, catalog_connection = connectionName, connection_options = {"dbtable": table4, "database": "blog1"}, transformation_ctx = "datasink7")

job.commit()
