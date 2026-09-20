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

# Script generated for node Customer Landing
CustomerLanding_node1789841719532 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-d609-project-2026/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1789841719532")

# Script generated for node Privacy Filter SQL
SqlQuery0 = '''
SELECT *
FROM myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL

'''
PrivacyFilterSQL_node1789933692042 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":CustomerLanding_node1789841719532}, transformation_ctx = "PrivacyFilterSQL_node1789933692042")

# Script generated for node Customer Trusted
EvaluateDataQuality().process_rows(frame=PrivacyFilterSQL_node1789933692042, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1789841705898", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrusted_node1789841919534 = glueContext.getSink(path="s3://stedi-d609-project-2026/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1789841919534")
CustomerTrusted_node1789841919534.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
CustomerTrusted_node1789841919534.setFormat("json")
CustomerTrusted_node1789841919534.writeFrame(PrivacyFilterSQL_node1789933692042)
job.commit()