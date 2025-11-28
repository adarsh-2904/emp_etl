from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col,lit,current_timestamp
from delta.tables import DeltaTable

def initialize_spark():
    
    builder = (
        SparkSession.builder.appName("airflow-practice")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", "C:/Users/Exavalu/OneDrive - exavalu/jar/postgresql-42.7.3.jar")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark

def extract_and_load_to_landing(spark):

    try:
        csv_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/delta_dataset.csv"

        df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
        df = df.toDF(*[c.lower() for c in df.columns])
        # Convert load_ts to timestamp (assuming your format is 'dd-MM-yyyy HH:mm')
        df = df.withColumn("load_ts", to_timestamp("load_ts", "dd-MM-yyyy HH:mm"))

        if "action_type" in df.columns:
            df = df.drop("action_type")

        print("df: ",df.count())

        # Write DataFrame to Delta Table
        df.write.format("delta").mode("append").save("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/landing")
        print("Data successfully written to Delta Table.")
    except Exception as e:
        print("Error during extraction and loading to landing:", str(e))

def load_to_staging(spark):
    try:
        print("Loading to staging...")

        jdbc_url = "jdbc:postgresql://localhost:5432/etl"
        connection_properties = {
            "user": "postgres",
            "password": "root",
            "driver": "org.postgresql.Driver"
        }
    

        cntrl_df =   spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "staging.contrl_tbl") \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .option("driver", connection_properties["driver"]) \
            .load()

        row_num = cntrl_df.count()

        if row_num == 0:
            print("df is none")
            processed_ts = "1999-01-01 00:00:00"
        else:
            processed_ts = cntrl_df.filter(cntrl_df.table_name == "employee").select("last_processed_ts").collect()[0][0]
        
        print("processed_ts: ", processed_ts)

        #read from landing delta table incrementally
        emp_inc_df = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/landing") \
            .filter(col("load_ts") > to_timestamp(lit(processed_ts), "yyyy-MM-dd HH:mm:ss"))
        print("Incremental df count: ", emp_inc_df.count())

        #define staging path
        staging_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/staging"
        #perform upsert to staging delta table
        if DeltaTable.isDeltaTable(spark,staging_path):
            print("Staging delta table exists: Performing upsert")
            staging_delta_table = DeltaTable.forPath(spark, staging_path)
            staging_delta_table.alias("tgt")\
            .merge(emp_inc_df.alias("src"),
                   "tgt.nk_id = src.nk_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
        else:
            print("staging delta table does not exist: Creating new delta table")
            emp_inc_df.write.format("delta").mode("overwrite").save(staging_path)
        
        #comopute max load_ts to update control table later
        max_ts_row = emp_inc_df.agg({"load_ts": "max"}).collect()
        print("max_ts_row: ", max_ts_row)
        max_ts = None
        if max_ts_row and len(max_ts_row) > 0:  
            max_ts = max_ts_row[0]["max(load_ts)"]
            updt_cntrl_tbl(spark,max_ts)
    except Exception as e:
        print("Error during loading to staging:", str(e))

def scd2_from_staging_delta(spark):
    
    try:
        print("Loading to staging scd2...")

        jdbc_url = "jdbc:postgresql://localhost:5432/etl"
        connection_properties = {
            "user": "postgres",
            "password": "root",
            "driver": "org.postgresql.Driver"
        }
    

        cntrl_df =   spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "staging.contrl_tbl") \
            .option("user", connection_properties["user"]) \
            .option("password", connection_properties["password"]) \
            .option("driver", connection_properties["driver"]) \
            .load()

        row_num = cntrl_df.count()

        if row_num == 0:
            print("df is none")
            processed_ts = "1999-01-01 00:00:00"
        else:
            processed_ts = cntrl_df.filter(cntrl_df.table_name == "employee").select("last_processed_ts").collect()[0][0]
        
        print("processed_ts: ", processed_ts)

        #read from landing delta table incrementally
        src = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/landing") \
            .filter(col("load_ts") > to_timestamp(lit(processed_ts), "yyyy-MM-dd HH:mm:ss"))
        print("Incremental df count: ", src.count())

        #define staging path
        scd_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee"
        # If SCD table does not exist, initialize it with current rows
        initialized = False
        if not DeltaTable.isDeltaTable(spark, scd_path):
            init = src.withColumnRenamed("nk_id", "employee_id") \
                    .withColumn("effective_start_date", current_timestamp()) \
                    .withColumn("effective_end_date", lit(None).cast("timestamp")) \
                    .withColumn("is_current", lit(True))
            init.write.format("delta").mode("overwrite").save(scd_path)
            print("Initialized SCD2 delta table with current rows:", init.count())
            initialized = True
        
        # Normal SCD Type 2 processing (expire current, then insert new versions)
        target = DeltaTable.forPath(spark, scd_path)
        print("target delta table object created")
        # 1) Expire current rows where incoming load_ts is newer
        target.alias("t").merge(
            src.alias("s"),
            "t.employee_id = s.nk_id AND t.is_current = true"
        ).whenMatchedUpdate(
            condition="s.load_ts > t.load_ts",
            set={
                "effective_end_date": "current_timestamp()",
                "is_current": "false"
            }
        ).execute()

        print("Expired SCD2 current rows where necessary.")

        # 2) Insert new versions for new keys or where source.load_ts > current.load_ts
        current_snapshot = target.toDF().filter(col("is_current") == True).select(
            col("employee_id").alias("t_employee_id"),
            col("load_ts").alias("t_load_ts")
        )

        print("Current snapshot of SCD2 table prepared.")

        to_insert = src.alias("s") \
            .join(current_snapshot.alias("t"), col("s.nk_id") == col("t.t_employee_id"), "left") \
            .filter((col("t.t_employee_id").isNull()) | (col("s.load_ts") > col("t.t_load_ts"))) \
            .selectExpr(
                "s.nk_id as employee_id",
                "s.education",
                "s.joiningyear",
                "s.city",
                "s.paymenttier",
                "s.age",
                "s.gender",
                "s.everbenched",
                "s.experienceincurrentdomain",
                "s.leaveornot",
                "s.load_ts"
            ) \
            .withColumn("effective_start_date", current_timestamp()) \
            .withColumn("effective_end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current", lit(True))
        
        print("Prepared new SCD2 rows to insert.")
        to_insert.write.format("delta").mode("append").save(scd_path)
        print("Inserted SCD2 rows:", to_insert.count())
        
        
        print("SCD2 upsert operation completed.")
        #comopute max load_ts to update control table later
        max_ts_row = src.agg({"load_ts": "max"}).collect()
        print("max_ts_row: ", max_ts_row)
        max_ts = None
        if max_ts_row and len(max_ts_row) > 0:  
            max_ts = max_ts_row[0]["max(load_ts)"]
            updt_cntrl_tbl(spark,max_ts)
        print("SCD2 processing completed.")
    except Exception as e:
        print("Error during loading to staging:", str(e))
    
def updt_cntrl_tbl(spark,max_ts):
    try:
        print("Updating control table...")

        jdbc_url = "jdbc:postgresql://localhost:5432/etl"
        connection_properties = {
            "user": "postgres",
            "password": "root",
            "driver": "org.postgresql.Driver"
        }
        #update control table
        jconn = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, connection_properties["user"],connection_properties["password"])
        stmt = jconn.createStatement()

        upsert_cntrl = f"""
                INSERT INTO staging.contrl_tbl (table_name, last_processed_ts)
                VALUES ('employee','{max_ts}')
                ON CONFLICT (table_name) DO UPDATE SET last_processed_ts = EXCLUDED.last_processed_ts;
                """
        stmt.executeUpdate(upsert_cntrl)
    except Exception as e:
        print("Error during updating control table:", str(e))
    finally:
        stmt.close()
        jconn.close()

def history_demo(spark):
    table = DeltaTable.forPath(spark,"C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee")
    history_df =table.history().select("version", "timestamp", "operation")
    history_df.show(truncate=False)

    old_table = spark.read.format("delta").option("versionAsOf",0).load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee")
    print("Old Table at version 0:")    
    old_table.filter(old_table.employee_id.isin([2890,2387])).show()

    latest_table = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee")
    print("Latest Table:")  
    latest_table.filter(latest_table.employee_id.isin([2890,2387])).show()

    #restore to version 0
    table.restoreToVersion(0)
    restored_table = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee")
    print("Restored Table to version 0:")   
    restored_table.filter(restored_table.employee_id.isin([2890,2387])).show()

def main():
    spark = initialize_spark()
    extract_and_load_to_landing(spark)
    load_to_staging(spark)
    scd2_from_staging_delta(spark)
    history_demo(spark)

    # for verification purpose
    # staging_df = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/curated_scd2_employee")
    # staging_df.filter(staging_df.employee_id.isin([2890,2387])).show()


if __name__ == "__main__":
    main()

    

    

