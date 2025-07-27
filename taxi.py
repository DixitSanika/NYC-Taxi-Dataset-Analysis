from pyspark.sql import SparkSession #type:ignore
from pyspark.sql.functions import col, round  #type:ignore
from pyspark.sql.functions import sum as _sum #type:ignore
from pyspark.sql.functions import avg #type:ignore
from pyspark.sql.functions import to_date, count #type:ignore
from pyspark.sql.functions import sum as _sum, desc #type:ignore
from pyspark.sql.window import Window #type:ignore
from pyspark.sql.functions import row_number #type:ignore 

# 1. Creating Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Dataset Analysis Assignment") \
    .getOrCreate()

# 2. Load Parquet file
df = spark.read.parquet("C:/Users/user/Downloads/yellow_tripdata_2020-01.parquet")

# 3. First 5 rows
df.show(5)

# 4. Show column names
print(df.columns)

# ---------------- Query 1: Add Revenue Column ----------------
df_with_revenue = df.withColumn(
    "Revenue",
    round(
        col("fare_amount") +
        col("extra") +
        col("mta_tax") +
        col("tip_amount") +
        col("tolls_amount") +
        col("improvement_surcharge"),
        2
    )
)

# Show first 10 rows with new Revenue column
df_with_revenue.select(
    "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge", "Revenue"
).show(10)


# ---------------- Query 2: Total Passengers Grouped by Pickup Area ----------------
passenger_count_by_area = df.groupBy("PULocationID") \
    .agg(_sum("passenger_count").alias("Total_Passengers")) \
    .orderBy("Total_Passengers", ascending=False)

# Top 10 areas with the highest total passengers
passenger_count_by_area.show(10)

# ---------------- Query 3: Average Fare & Earnings by Vendor ----------------
vendor_avg_earnings = df.groupBy("VendorID") \
    .agg(
        round(avg("fare_amount"), 2).alias("Avg_Fare"),
        round(avg("total_amount"), 2).alias("Avg_Total_Earning")
    )

vendor_avg_earnings.show()

# ---------------- Query 4: Moving Count of Payments by Payment Mode ----------------
# Convert datetime to just date for grouping
df_with_date = df.withColumn("pickup_date", to_date("tpep_pickup_datetime"))

# Group by date and payment_type, count payments
payment_counts = df_with_date.groupBy("pickup_date", "payment_type") \
    .agg(count("*").alias("Payments_Made")) \
    .orderBy("pickup_date", ascending=True)

payment_counts.show(20)

# ---------------- Query 5: Top 2 Vendors by Earnings on a Particular Date ----------------
# Change the date to any date we want to filter
selected_date = '2020-01-01'

top_vendors = df_with_date.filter(col("pickup_date") == selected_date) \
    .groupBy("VendorID") \
    .agg(
        _sum("total_amount").alias("Total_Earning"),
        _sum("passenger_count").alias("Total_Passengers"),
        _sum("trip_distance").alias("Total_Distance")
    ) \
    .orderBy(desc("Total_Earning")) \
    .limit(2)

top_vendors.show()

# ---------------- Query 6: Most Passengers Between Two Locations ----------------
most_popular_route = df.groupBy("PULocationID", "DOLocationID") \
    .agg(_sum("passenger_count").alias("Total_Passengers")) \
    .orderBy(desc("Total_Passengers")) \
    .limit(1)

most_popular_route.show()

# ---------------- Query 7: Simulated Top Pickup Locations in Last Seconds ----------------
# Sorting by pickup time (latest first)
windowSpec = Window.orderBy(col("tpep_pickup_datetime").desc())

# Add row numbers and take top 10 latest rows 
latest_rows = df.withColumn("row_num", row_number().over(windowSpec)).filter(col("row_num") <= 10)

# Group by pickup location
latest_pickups = latest_rows.groupBy("PULocationID") \
    .agg(_sum("passenger_count").alias("Recent_Passengers")) \
    .orderBy(desc("Recent_Passengers"))

latest_pickups.show()

# Stop the spark session
spark.stop()