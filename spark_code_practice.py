from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum,row_number, dense_rank, date_format, lag, round
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Spark Code Practice").getOrCreate()

orders_df = spark.read.csv("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/orders_dataset.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("C:/Users/Exavalu/OneDrive - exavalu/airflow_practice/output_dataset/customers_dataset.csv", header=True, inferSchema=True)

print("Orders Dataset Schema: ")
orders_df.show()

print("Customers Dataset Schema: ")
customers_df.show()

#calculate total and average order amount for each city.
print("Joined Dataset: ")
joined_df = orders_df.join(customers_df, orders_df.customer_id == customers_df.customer_id, "inner")
joined_df.show()

joined_df = joined_df.groupBy("city").agg(sum("order_amount").alias("total_order_amount"), avg("order_amount").alias("average_order_amount")    )
print("Total and Average Order Amount by City: ")
joined_df.show()

#For each customer, find their top 2 highest order amounts
window_spec = Window.partitionBy("customer_id").orderBy(col("order_amount").desc())
ranked_df = orders_df.withColumn("rank",dense_rank().over(window_spec))
print("ranked Dataset: ")
ranked_df.show()
top_2_orders_df = ranked_df.filter(col("rank")<=2)
print("Top 2 Orders per Customer: ")
top_2_orders_df.show()

#Return customer names from customers who do not appear in orders

left_anti_df = customers_df.join(orders_df,customers_df.customer_id ==orders_df.customer_id,"left_anti")
print("Customers without Orders: ")
left_anti_df.show()

#For each month, calculate the percentage growth in total revenue compared to the previous month.
monthly_sale = orders_df.withColumn("month", date_format(col("order_date"),"yyyy-MM")).groupBy("month").agg(sum("order_amount").alias("total_revenue"))
print("Monthly Sale: ")
monthly_sale.show()

window_spec = Window.orderBy("month")

previous_month_revenue = lag(col("total_revenue")).over(window_spec)