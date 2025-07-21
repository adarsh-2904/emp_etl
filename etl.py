#from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pyspark.sql import SparkSession
import psycopg2
import logging
#
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
#
# airflow_jar = "/opt/jars/postgresql-42.7.3.jar"
#
# airflow_url = "jdbc:postgresql://host.docker.internal:5432/etl"

spark = SparkSession.builder \
    .appName("airflow-practice") \
    .config("spark.jars", "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar") \
    .getOrCreate()

def extract_and_load_to_landing():
    # Set JAVA_HOME
    #os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    spark = SparkSession.builder \
        .appName("airflow-practice") \
        .config("spark.jars", "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar") \
        .getOrCreate()
    # Get the include folder path using the Airflow home
    #include_path = os.path.join(os.environ["AIRFLOW_HOME"], "include", "base_dataset.csv")
    csv_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/base_dataset.csv"
    # Create Spark session
    # spark = SparkSession.builder \
    #     .appName("airflow-practice") \
    #     .config("spark.jars", "/opt/jars/postgresql-42.7.3.jar") \
    #     .getOrCreate()

    # Read CSV
    #For airflow path
    #df = spark.read.csv(include_path, header=True, inferSchema=True)
    #for local path
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    df.show()

    # Write to PostgreSQL
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable", "landing.employee") \
        .option("user", "postgres") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
#
# def load_to_staging():
#     # conn = psycopg2.connect(
#     #     dbname="postgres",
#     #     user="postgres",
#     #     password="postgres",
#     #     host="localhost",  # or an IP address
#     #     port="5432"  # default PostgreSQL port
#     # )
#
#     df =   spark.read.format("jdbc") \
#         .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
#         .option("dbtable", "staging.control_tbl") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .load()
#
#     processed_ts = df["last_processed_ts"]
#
#     if processed_ts is None:
#         processed_ts = "01-01-1999 00:00:00"
#
#     query = f"(SELECT * FROM landing.employee WHERE load_ts > '{processed_ts}') AS emp_subquery"
#     emp_df = spark.read.format("jdbc") \
#         .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
#         .option("dbtable", query) \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .load()
#
#     logging.info("control table: ", df.show(10))
#     logging.info("last_processed_ts:")
#     logging.info(processed_ts)
#     logging.info(emp_df)
#
#     emp_df.write.format("jdbc") \
#         .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
#         .option("dbtable", "staging.emp_stg") \
#         .option("user", "postgres") \
#         .option("password", "postgres") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("overwrite") \
#         .save()

def main():
    extract_and_load_to_landing()


if __name__ == "__main__":
    main()

    
# ------------------------it is for airflow--------------------------------------

# with DAG(
#     dag_id="weather_etl",
#     start_date=datetime(2025, 7, 10),
#     schedule="@hourly",
#     catchup=False
# ) as dag:
#
#     extract_and_load = PythonOperator(
#         task_id="extract_and_load_to_db",
#         python_callable=extract_and_load_to_landing
#     )
#
#     load_to_stg = PythonOperator(
#         task_id="load_to_staging",
#         python_callable= load_to_staging
#     )
#
#     # Set dependencies between tasks
#     extract_and_load >> load_to_stg
