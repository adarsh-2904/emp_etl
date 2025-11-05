#from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from pyspark.sql import SparkSession
import psycopg2
import logging
from pyspark.sql.functions import to_timestamp


def initialize_spark():
    spark = SparkSession.builder \
        .appName("airflow-practice") \
        .config("spark.jars", "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar") \
        .getOrCreate()
    return spark

def check_base_and_delta_dataset(spark):
    csv_base_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/base_dataset.csv"
    csv_delta_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/delta_dataset.csv"

    base_df = spark.read.csv(csv_base_file_path, header=True, inferSchema=True)
    delta_df = spark.read.csv(csv_delta_file_path, header=True, inferSchema=True)

    base_df = base_df.toDF(*[c.lower() for c in base_df.columns])
    delta_df = delta_df.toDF(*[c.lower() for c in delta_df.columns])
    
    #create a temp view
    base_df.createOrReplaceTempView("base_table")
    delta_df.createOrReplaceTempView("delta_table")

    #Perform sql operattion to find the common records between two datasets and their values (If there is any change in the values of common records )
    changed_records = spark.sql("""
    SELECT * FROM delta_table d
    iNNER JOIN base_table b
    ON d.nk_id = b.nk_id""")

    changed_records.show()
    print("changed_records count: ", changed_records.count())



def extract_and_load_to_landing(spark):
    csv_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/delta_dataset.csv"

    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
    df = df.toDF(*[c.lower() for c in df.columns])
    # Convert load_ts to timestamp (assuming your format is 'dd-MM-yyyy HH:mm')
    df = df.withColumn("load_ts", to_timestamp("load_ts", "dd-MM-yyyy HH:mm"))

    if "action_type" in df.columns:
        df = df.drop("action_type")

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
    print("query: ", query)
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
	    from staging.employee;
        """
    else:
        update_cntrl_tbl = """
            UPDATE staging.contrl_tbl
            SET last_processed_ts = (SELECT MAX(load_ts) FROM staging.employee)
            WHERE table_name = 'employee';
        """
    stmt.executeUpdate(update_cntrl_tbl)

    stmt.close()
    jconn.close()

def load_to_curated(spark):
     jdbc_url = "jdbc:postgresql://localhost:5432/etl"
     connection_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }
    
    # read from curated.cntrl_tbl
     cntrl_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable","curated.cntrl_tbl")\
        .option("user", "postgres")\
        .option("password", "root")\
        .option("driver", "org.postgresql.Driver")\
        .load()
     
     row_num = cntrl_df.count()

     if row_num == 0:
        print("df is none")
        processed_ts = "01-01-1999 00:00:00"
     else:
        processed_ts = cntrl_df.filter(cntrl_df.table_name == "employee").select("last_processed_ts").collect()[0][0]
    
     print("curated_processed_ts: ",processed_ts)

     query = f"(SELECT * FROM staging.employee WHERE load_ts > '{processed_ts}') AS emp_subquery"

     print("curated_query: ",query)

    #load data from staging.employee in incremental way
     emp_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable",query)\
        .option("user", "postgres")\
        .option("password", "root")\
        .option("driver", "org.postgresql.Driver")\
        .load()
     
     #write the incremental data to curated.empl_stg
     emp_df.write.format("jdbc")\
     .option("url",jdbc_url)\
     .option("dbtable","curated.emp_stg")\
     .option("user","postgres")\
     .option("password","root")\
     .option("driver", "org.postgresql.Driver")\
     .mode("overwrite")\
     .save()

     #apply merge logic to curated.employee
     jconn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url,connection_properties["user"],connection_properties["password"])
     stmt = jconn.createStatement()
     merge_sql = """
        INSERT INTO curated.employee (
        education,
        joiningyear,
        city,
        paymenttier,
        age,
        gender,
        everbenched,
        experienceincurrentdomain,
        leaveornot,
        employee_id,
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
        FROM curated.emp_stg
        ON CONFLICT (employee_id) DO UPDATE SET
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

     if row_num==0:
          update_cntrl_tbl = """
        INSERT INTO curated.cntrl_tbl(
	    table_name, last_processed_ts)
	    select 'employee' as table_name,
	    max(load_ts)
	    from curated.employee;
        """
     else:
        update_cntrl_tbl = """
            UPDATE curated.cntrl_tbl
            SET last_processed_ts = (SELECT MAX(load_ts) FROM curated.employee)
            WHERE table_name = 'employee';
        """
     stmt.executeUpdate(update_cntrl_tbl)

     stmt.close()
     jconn.close()
         

def load_to_scd2dimension(spark): 
     jdbc_url = "jdbc:postgresql://localhost:5432/etl"
     connection_properties = {
        "user": "postgres",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }
    
    # read from curated.cntrl_tbl
     cntrl_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable","curated.cntrl_tbl")\
        .option("user", "postgres")\
        .option("password", "root")\
        .option("driver", "org.postgresql.Driver")\
        .load()
     
     row_num = cntrl_df.count()

     if row_num == 0:
        print("df is none")
        processed_ts = "01-01-1999 00:00:00"
     else:
        processed_ts = cntrl_df.filter(cntrl_df.table_name == "employee").select("last_processed_ts").collect()[0][0]
    
     print("curated_processed_ts: ",processed_ts)

     query = f"(SELECT * FROM staging.employee WHERE load_ts > '{processed_ts}') AS emp_subquery"

     print("curated_query: ",query)

    #load data from staging.employee in incremental way
     emp_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable",query)\
        .option("user", "postgres")\
        .option("password", "root")\
        .option("driver", "org.postgresql.Driver")\
        .load()
     
     #write the incremental data to curated.empl_stg
     emp_df.write.format("jdbc")\
     .option("url",jdbc_url)\
     .option("dbtable","curated.emp_stg")\
     .option("user","postgres")\
     .option("password","root")\
     .option("driver", "org.postgresql.Driver")\
     .mode("overwrite")\
     .save()

     #apply merge logic to curated.employee
     jconn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url,connection_properties["user"],connection_properties["password"])
     stmt = jconn.createStatement()
     expire_sql = """
                UPDATE curated.scd2_dim_employee AS target
        SET effective_end_date = NOW(),
            is_current = 0
        FROM curated.emp_stg AS source
        WHERE target.employee_id = source.nk_id
          AND target.is_current = 1
          AND (
                target.education IS DISTINCT FROM source.education OR
                target.joiningyear IS DISTINCT FROM source.joiningyear OR
                target.city IS DISTINCT FROM source.city OR
                target.paymenttier IS DISTINCT FROM source.paymenttier OR
                target.age IS DISTINCT FROM source.age OR
                target.gender IS DISTINCT FROM source.gender OR
                target.everbenched IS DISTINCT FROM source.everbenched OR
                target.experienceincurrentdomain IS DISTINCT FROM source.experienceincurrentdomain OR
                target.leaveornot IS DISTINCT FROM source.leaveornot
          );
                """

     insert_sql = """
                INSERT INTO curated.scd2_dim_employee (
            employee_id, education, joiningyear, city, paymenttier, age, gender,
            everbenched, experienceincurrentdomain, leaveornot, load_ts,
            effective_start_date, effective_end_date, is_current
        )
        SELECT
            s.nk_id, s.education, s.joiningyear, s.city, s.paymenttier, s.age, s.gender,
            s.everbenched, s.experienceincurrentdomain, s.leaveornot, s.load_ts,
            s.load_ts, NULL, 1
        FROM curated.emp_stg AS s
        LEFT JOIN curated.scd2_dim_employee AS t
          ON t.employee_id = s.nk_id AND t.is_current = 1
        WHERE t.employee_id IS NULL
           OR (
                t.education IS DISTINCT FROM s.education OR
                t.joiningyear IS DISTINCT FROM s.joiningyear OR
                t.city IS DISTINCT FROM s.city OR
                t.paymenttier IS DISTINCT FROM s.paymenttier OR
                t.age IS DISTINCT FROM s.age OR
                t.gender IS DISTINCT FROM s.gender OR
                t.everbenched IS DISTINCT FROM s.everbenched OR
                t.experienceincurrentdomain IS DISTINCT FROM s.experienceincurrentdomain OR
                t.leaveornot IS DISTINCT FROM s.leaveornot
           );
                """

     stmt.executeUpdate(expire_sql)
     stmt.executeUpdate(insert_sql)

     if row_num==0:
          update_cntrl_tbl = """
        INSERT INTO curated.cntrl_tbl(
	    table_name, last_processed_ts)
	    select 'employee' as table_name,
	    max(load_ts)
	    from curated.scd2_dim_employee;
        """
     else:
        update_cntrl_tbl = """
            UPDATE curated.cntrl_tbl
            SET last_processed_ts = (SELECT MAX(load_ts) FROM curated.scd2_dim_employee)
            WHERE table_name = 'employee';
        """
     stmt.executeUpdate(update_cntrl_tbl)

     scd2df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable","curated.scd2_dim_employee")\
        .option("user", "postgres")\
        .option("password", "root")\
        .option("driver", "org.postgresql.Driver")\
        .load()
     
     print("scd2df: ")
     scd2df.show()

     stmt.close()
     jconn.close()   

     
     


def main():
    spark = initialize_spark()
    extract_and_load_to_landing(spark)
    load_to_staging(spark)
    #load_to_curated(spark)
    load_to_scd2dimension(spark)
    #check_base_and_delta_dataset(spark)


if __name__ == "__main__":
    main()

    
