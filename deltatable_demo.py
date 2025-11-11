from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col,lit

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
        csv_file_path = "C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/base_dataset.csv"

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
            processed_ts = "01-01-1999 00:00:00"
        else:
            processed_ts = cntrl_df.filter(cntrl_df.table_name == "employee").select("last_processed_ts").collect()[0][0]
        
        print("processed_ts: ", processed_ts)

        #read from landing delta table incrementally
        emp_inc_df = spark.read.format("delta").load("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/delta_table/landing") \
            .filter(col("load_ts") > to_timestamp(lit(processed_ts), "dd-MM-yyyy HH:mm"))
        print("Incremental df count: ", emp_inc_df.count())
        emp_inc_df.show()
    except Exception as e:
        print("Error during loading to staging:", str(e))

    

if __name__ == "__main__":
    spark = initialize_spark()
    extract_and_load_to_landing(spark)
    load_to_staging(spark)