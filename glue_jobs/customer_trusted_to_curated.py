import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node a
a_node1779689821247 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="a_node1779689821247")

# Script generated for node c
c_node1779689819819 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="c_node1779689819819")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct c.*
from c 
join a 
on c.email = a.user

'''
SQLQuery_node1779689870567 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"c":c_node1779689819819, "a":a_node1779689821247}, transformation_ctx = "SQLQuery_node1779689870567")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779689870567, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779689742219", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779689957860 = glueContext.getSink(path="s3://stedi-sthiti/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779689957860")
AmazonS3_node1779689957860.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1779689957860.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779689957860.writeFrame(SQLQuery_node1779689870567)
job.commit()
