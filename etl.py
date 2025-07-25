#from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pyspark.sql import SparkSession
import psycopg2
import logging


def initialize_spark():
    spark = SparkSession.builder \
        .appName("airflow-practice") \
        .config("spark.jars", "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark


def extract_and_load_to_landing(spark):
    csv_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/delta_dataset.csv"

    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    df = df.drop(df.action_type)

    print("df: ",df.count())

    # Write to PostgreSQL
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable", "landing.employee") \
        .option("user", "postgres") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

def load_to_staging(spark):
    jdbc_url = "jdbc:postgresql://localhost:5432/etl"
    connection_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    df =   spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable", "staging.contrl_tbl") \
        .option("user", "postgres") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    row_num = df.count()

    if row_num == 0:
        print("df is none")
        processed_ts = "01-01-1999 00:00:00"
    else:
        processed_ts = df.filter(df.table_name == "employee").select("last_processed_ts").collect()[0][0]
    

    print("processed_ts: ", processed_ts)

    query = f"(SELECT * FROM landing.employee WHERE load_ts > '{processed_ts}') AS emp_subquery"
    emp_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable", query) \
        .option("user", "postgres") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    emp_df.show()
    print(emp_df.columns)



    emp_df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/etl") \
        .option("dbtable", "staging.emp_stg") \
        .option("user", "postgres") \
        .option("password", "root") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    # create jvm connection
    jconn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, connection_properties["user"],connection_properties["password"])
    stmt = jconn.createStatement()
    merge_sql = """
    INSERT INTO staging.employee (
    education,
    joiningyear,
    city,
    paymenttier,
    age,
    gender,
    everbenched,
    experienceincurrentdomain,
    leaveornot,
    nk_id,
    load_ts
)
SELECT
    education,
    joiningyear,
    city,
    paymenttier,
    age,
    gender,
    everbenched,
    experienceincurrentdomain,
    leaveornot,
    nk_id,
    load_ts
FROM staging.emp_stg
ON CONFLICT (nk_id) DO UPDATE SET
    education = EXCLUDED.education,
    joiningyear = EXCLUDED.joiningyear,
    city = EXCLUDED.city,
    paymenttier = EXCLUDED.paymenttier,
    age = EXCLUDED.age,
    gender = EXCLUDED.gender,
    everbenched = EXCLUDED.everbenched,
    experienceincurrentdomain = EXCLUDED.experienceincurrentdomain,
    leaveornot = EXCLUDED.leaveornot,
    load_ts = EXCLUDED.load_ts; 
    """
    stmt.executeUpdate(merge_sql)

    if row_num == 0:
        update_cntrl_tbl = """
        INSERT INTO staging.contrl_tbl(
	    table_name, last_processed_ts)
	    select 'employee' as table_name,
	    max(load_ts)
	    from staging.emp_stg;
        """
    else:
        update_cntrl_tbl = """
            UPDATE staging.contrl_tbl
            SET last_processed_ts = (SELECT MAX(load_ts) FROM staging.emp_stg)
            WHERE table_name = 'employee';
        """
    stmt.executeUpdate(update_cntrl_tbl)

    stmt.close()
    jconn.close()


def main():
    spark = initialize_spark()
    extract_and_load_to_landing(spark)
    load_to_staging(spark)


if __name__ == "__main__":
    main()

    
