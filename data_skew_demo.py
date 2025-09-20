from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id, countDistinct,desc
import time

spark = SparkSession.builder.appName("DataSkewDemo").getOrCreate()

#Simulating uniform dataset
df_uniform  = spark.range(1000000)


print("Uniform Partitioning.............................")
print("Number of partitions in uniform dataset: ",df_uniform.rdd.getNumPartitions())

#df_uniform.show()

#df_uniform.withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().show()

#Skewed dataset
df0= spark.range(1000000).repartition(1)
df1= spark.range(0,10).repartition(1)
df2= spark.range(0,10).repartition(1)

#df_skewed = df0.union(df1).union(df2)
print("Skewed Partitioning.............................")
# df_skewed.show()

# df_skewed.withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().show()

#Skew join
#Reading a file
df_transaction = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/transactions")
df_customer = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/customers")

df_transaction.groupBy("cust_id").agg(countDistinct("txn_id").alias("distinct_txn")).orderBy(desc("distinct_txn")).show()



time.sleep(300)