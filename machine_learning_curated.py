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
AccelerometerTrusted_node1789938449056 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1789938449056")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1789938451454 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1789938451454")

# Script generated for node Join Sensor Readings
SqlQuery0 = '''
SELECT
    a.user,
    a.timestamp,
    a.x,
    a.y,
    a.z,
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM accelerometer a
INNER JOIN stepTrainer s
    ON a.timestamp = s.sensorreadingtime
'''
JoinSensorReadings_node1789938548173 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"stepTrainer":StepTrainerTrusted_node1789938451454, "accelerometer":AccelerometerTrusted_node1789938449056}, transformation_ctx = "JoinSensorReadings_node1789938548173")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinSensorReadings_node1789938548173, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1789938256462", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1789938623359 = glueContext.getSink(path="s3://stedi-d609-project-2026/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1789938623359")
MachineLearningCurated_node1789938623359.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1789938623359.setFormat("json")
MachineLearningCurated_node1789938623359.writeFrame(JoinSensorReadings_node1789938548173)
job.commit()