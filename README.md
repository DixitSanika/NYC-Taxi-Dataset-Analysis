# NYC Yellow Taxi Dataset Analysis using PySpark

This project is a beginner-friendly PySpark analysis of NYC Yellow Taxi trip data.  
The dataset is from January 2020, and all analysis was done using PySpark on VS Code.  
I performed different queries to understand taxi operations in NYC — from fare calculations to most popular routes and vendor performance.

# Dataset Used
- [yellow_tripdata_2020-01.parquet](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)  
- This dataset includes: fare details, vendor IDs, pickup/drop locations, timestamps, and more.

# Queries Performed
1. Added a “Revenue” Column:
Calculated the revenue per ride by summing:  
fare_amount + extra + mta_tax + improvement_surcharge + tip_amount + tolls_amount + total_amount

2. Passenger Count by Pickup Area:
Grouped by PULocationID to check which pickup areas have the most passengers.

3. Real-time Avg Fare & Total Earning per Vendor:
Grouped data by VendorID to calculate:
- Average fare amount
- Average total earning per vendor

4. Moving Count of Payments by Payment Type:
Grouped payment count by pickup_date and payment_type to simulate a moving count over time.

5. Top 2 Earning Vendors on a Specific Date:
Filtered rides from 2020-01-01, calculated:
- Total revenue per vendor
- Total passengers
- Total trip distance

6. Most Popular Route:
Found the route (from pickup to dropoff location) with the highest number of passengers.

7. Top Pickup Locations in Recent Trips:
Simulated real-time behavior by selecting the **last 10 pickup timestamps**  
Grouped by location to get the most recent popular pickup spots.

# Tools Used
- PySpark
- VS Code
- Parquet file format
- Python 3.x

# What I Learned:
- How to use PySpark for real-world data analysis  
- How to work with Parquet files and run DataFrame operations  
- How to simulate real-time logic even with static data  
- Debugging Spark window functions and understanding performance warnings

# Output Samples:
The dataset is large, but I used .show(5) and .show(20) to display top rows for reference.


