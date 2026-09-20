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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1789856021714 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1789856021714")

# Script generated for node Customer Trusted
CustomerTrusted_node1789857213938 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1789857213938")

# Script generated for node Join Customer
JoinCustomer_node1789857237025 = Join.apply(frame1=AccelerometerTrusted_node1789856021714, frame2=CustomerTrusted_node1789857213938, keys1=["user"], keys2=["email"], transformation_ctx="JoinCustomer_node1789857237025")

# Script generated for node Select Distinct Customers
SqlQuery0 = '''
SELECT DISTINCT
    customerName,
    email,
    phone,
    birthDay,
    serialNumber,
    registrationDate,
    lastUpdateDate,
    shareWithResearchAsOfDate,
    shareWithPublicAsOfDate,
    shareWithFriendsAsOfDate
FROM myDataSource
'''
SelectDistinctCustomers_node1789857335919 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomer_node1789857237025}, transformation_ctx = "SelectDistinctCustomers_node1789857335919")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SelectDistinctCustomers_node1789857335919, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1789855244072", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1789857389344 = glueContext.getSink(path="s3://stedi-d609-project-2026/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1789857389344")
CustomerCurated_node1789857389344.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_curated")
CustomerCurated_node1789857389344.setFormat("json")
CustomerCurated_node1789857389344.writeFrame(SelectDistinctCustomers_node1789857335919)
job.commit()