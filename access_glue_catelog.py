import sys, json, boto3,os
from datetime import datetime, timezone
#from awsglue.transforms import *
#from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
#from awsglue.context import GlueContext
#from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, current_timestamp, lit


from delta import configure_spark_with_delta_pip  # optional if using delta too
from delta.tables import DeltaTable
from delta.tables import *
from delta import *

# Initialize Spark and Glue contexts
warehouse = "s3://glue-practice-31052025/staging/employee/"
spark = (
    SparkSession.builder
    .appName("iceberg-glue-demo")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
            "org.apache.iceberg:iceberg-aws:1.5.0",
            "software.amazon.awssdk:glue:2.20.160",
            "software.amazon.awssdk:s3:2.20.160",
            "software.amazon.awssdk:sts:2.20.160",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ])
    )
    # Enable Iceberg
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Configure Iceberg → Glue Catalog
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", warehouse)

    # Disable Hive
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.glue_catalog.use-hive-client", "false")

    # Hadoop AWS
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate()
)

spark.sql("SHOW NAMESPACES IN glue_catalog").show()

input_df = spark.read.format("iceberg").load("glue_catalog.targetemployeedb.emp_stg")

print("Input DataFrame:", input_df.count())