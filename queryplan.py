from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split,lit

spark = SparkSession.builder.appName("queryPlan").getOrCreate()

customerDf = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/customers.parquet")
customerDf.show()

transactionDf = spark.read.parquet("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/transactions.parquet")
transactionDf.show()

# 1. Pushdown Projection
# What: Only the columns you actually need are read from the data source.
# Why: Reduces data transfer and memory usage.
# Example:
# customerDf.select("customer_id", "customer_name")
# Spark will only read customer_id and customer_name columns from the file, not all columns.

# 2. Filter Pushdown
# What: Filters (WHERE conditions) are applied as early as possible, ideally at the data source.
# Why: Reduces the amount of data Spark needs to load and process.
# Example:
# customerDf.filter(col("country") == "India")
# Spark will try to apply this filter while reading the data, so only rows where country == "India" are loaded.

# Summary:
# Pushdown Projection: Only needed columns are read.
# Filter Pushdown: Only needed rows are read.

# Narrow Transformation
df_narrow_transform = (
    customerDf
    .filter(col("city") == "boston")
    .withColumn("first_name", split("name", " ").getItem(0))
    .withColumn("last_name", split("name", " ").getItem(1))
    .withColumn("age", col("age") + lit(5))
    .select("cust_id", "first_name", "last_name", "age", "gender", "birthday")
)

df_narrow_transform.show(5, False)
#df_narrow_transform.explain(True)

# Wide Transformation
# Repartition
# Coalesce
# Joins
# GroupBy
#   -count
#   -countDistinct
#   -sum

partition_num = transactionDf.rdd.getNumPartitions()

print("Initial partititon num:",partition_num)

# transactionDf.repartition(10).explain(True)

# Repartition transactionDf based on the "customer_id" column into 10 partitions
transactionDf.repartition(10, col("year")).explain(True)

#disabling the broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

#Join operation
#During shuffle data is sit on shuffle partition

joinedDf = transactionDf.join(customerDf, transactionDf.cust_id == customerDf.cust_id,"inner")

joinedDf.explain(True)


# group by operation
df_city_count  = transactionDf.groupBy("city").count()

print("df_city_count")
df_city_count.show()

df_city_count.explain(True)