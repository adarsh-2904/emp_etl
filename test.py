from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, broadcast,countDistinct
import time

spark = SparkSession.builder.appName("DAG-Analysis").getOrCreate()

#Reading a file
df_transaction = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/transactions")
df_customer = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/customers")
print("Number of partition: ",df_transaction.rdd.getNumPartitions())

print("transaction data: ")
df_transaction.show()

print("customer data: ")
df_customer.show()

#Narrorw transformation

# df_narrow_transform = df_customer.filter(col("city") == "boston")\
#     .withColumn("first_name",split(col("name")," ").getItem(0))\
#     .withColumn("last_name",split(col("name")," ").getItem(1))

#df_narrow_transform.show()

# df_narrow_transform.write.mode("overwrite").parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/narrow_transform")

#wide transformation
#join operation is a wide transformation
#disable the broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

#repartion the dataset
df_transaction_partitioned = df_transaction.repartition(12,"cust_id")

print("Joining two datasets")
joined_df = df_transaction_partitioned.join(df_customer, on="cust_id", how="inner").drop(df_customer.city)

print("writing joined data")
joined_df.write.format("noop").mode("overwrite").parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/joined_data")

#broadcast join
#broadcast join is used when one of the datasets is small enough to fit in memory   

# spark.conf.set("spark.sql.autoBroadcastJoinThreshold",10485760) #10MB
# print("Joining two datasets using broadcast join")
# joined_df_broadcast = df_transaction.join(broadcast(df_customer),on="cust_id",how="inner").drop(df_customer.city)
# print("writing joined data using broadcast join")
# joined_df_broadcast.write.format("noop").mode("overwrite").parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/joined_data_broadcast")

# #Aggregation
# agg_df = df_transaction.groupby("cust_id").count()
# agg_df.show()

# #Aggregation with distinct
# agg_distinct_df = df_transaction.groupby("cust_id").agg(countDistinct("city")).alias("distinct_city_count")
# agg_distinct_df.show()







time.sleep(300)  # Keeps the app alive for 5 minutes
