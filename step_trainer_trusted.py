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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1789937675385 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1789937675385")

# Script generated for node Customer Curated
CustomerCurated_node1789937711720 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_curated", transformation_ctx="CustomerCurated_node1789937711720")

# Script generated for node Filter Trusted Step Trainer
SqlQuery0 = '''
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM stepTrainer s
WHERE s.serialnumber IN (
    SELECT c.serialnumber
    FROM customerCurated c
)
'''
FilterTrustedStepTrainer_node1789937761390 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customerCurated":CustomerCurated_node1789937711720, "stepTrainer":StepTrainerLanding_node1789937675385}, transformation_ctx = "FilterTrustedStepTrainer_node1789937761390")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=FilterTrustedStepTrainer_node1789937761390, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1789937221591", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1789937860328 = glueContext.getSink(path="s3://stedi-d609-project-2026/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1789937860328")
StepTrainerTrusted_node1789937860328.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1789937860328.setFormat("json")
StepTrainerTrusted_node1789937860328.writeFrame(FilterTrustedStepTrainer_node1789937761390)
job.commit()