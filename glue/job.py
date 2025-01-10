import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define input and output paths
input_path = "s3://weather-data-pipeline-muaaz/raw-data/"
output_path = "s3://weather-data-pipeline-muaaz/processed-data/"

# Read JSON files from S3 bucket
json_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="json"
)

# Convert the DynamicFrame to a DataFrame for transformation
dataframe = json_data.toDF()

# Select specific columns: city name, temperature, and weather
dataframe = dataframe.select(
    dataframe["name"].alias("city_name"),
    dataframe["main"]["temp"].alias("temperature"),
    dataframe["weather"][0]["main"].alias("weather"),
    dataframe["weather"][0]["description"].alias("description")
)

# Convert back to DynamicFrame
transformed_frame = DynamicFrame.fromDF(dataframe, glueContext)

# Write the output as Parquet files
glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

# Commit the job
job.commit()
