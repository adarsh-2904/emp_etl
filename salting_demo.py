from pyspark.sql.types import *
from pyspark.sql.functions import spark_partition_id, countDistinct,desc,rand,col, lit,array, explode, concat_ws
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SaltingDemo").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.set("spark.sql.adaptive.enabled", "false")

#Simulating Skewed Join

#Uniform dataset
df_uniform  = spark.range(1000000)


print("Uniform Partitioning.............................")
print("Number of partitions in uniform dataset: ",df_uniform.rdd.getNumPartitions())

df_uniform.show()

df_uniform.withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().show()

#Skewed dataset
#Skewed dataset
df0= spark.range(1000000).repartition(1)
df1= spark.range(0,10).repartition(1)
df2= spark.range(0,10).repartition(1)

df_skewed = df0.union(df1).union(df2)
print("Skewed Partitioning.............................")
df_skewed.show()


df_skewed.withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().show()


print("Skewed Join without Salting.............................")
df_joined_c1 = df_skewed.join(df_uniform, "id", 'inner')

(
    df_joined_c1
    .withColumn("partition", spark_partition_id())
    .groupBy("partition")
    .count()
    .show(5, False)
)

#Simulating Uniform Distribution Through Salting

#get salt value
SALT_NUMBER = int(spark.conf.get("spark.sql.shuffle.partitions"))
print(f"SALT_NUMBER: {SALT_NUMBER}")

df_skewed = df_skewed.withColumn("salt", (rand() * SALT_NUMBER).cast("int"))
print("Skewed dataset with salt column.............................")
df_skewed.show(10, truncate=False)


df_uniform = (
    df_uniform
    .withColumn("salt_values", array([lit(i) for i in range(SALT_NUMBER)]))
    .withColumn("salt", explode(col("salt_values")))
)
print("Uniform dataset with salt column.............................")
df_uniform.show(10, truncate=False)

print("Skewed Join with Salting.............................")
df_joined = df_skewed.join(df_uniform, ["id", "salt"], 'inner')
(
    df_joined
    .withColumn("partition", spark_partition_id())
    .groupBy("id", "partition")
    .count()
    .orderBy("id", "partition")
    .show()
)
