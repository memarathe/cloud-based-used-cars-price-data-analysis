# -*- coding: utf-8 -*-
"""
UsedCarAnalysis - Spark SQL Queries for Analyzing Used Car Data
This script leverages Apache Spark to analyze a dataset of used car listings, 
identifying several key insights based on the data.

The following SQL queries are executed on the data:
1. **Top 5 Regions with the Highest Average Price**:
   - Retrieves the top 5 regions (zip codes) with the highest average car price.
   - The result helps identify regions where the highest-value cars are sold.

2. **Total Car Sales Revenue by Year**:
   - Calculates the total sales revenue for each year.
   - Provides an overview of the yearly sales performance.

3. **Count of Cars by Transmission Type per Region**:
   - Counts the number of cars available for each transmission type (manual/automatic) in each region (zip code).
   - Shows the distribution of transmission types across different regions.

4. **Monthly Trend in Car Listings**:
   - Displays the monthly trend in the number of car listings posted.
   - Helps track the fluctuation in listings over time, indicating seasonal trends.

5. **Top 3 Car Types with the Highest Total Sales in Each Region**:
   - Identifies the top 3 car types (e.g., sedan, SUV) by total sales value in each region.
   - Highlights which car types are driving the most revenue in each area.

The script executes these queries on a dataset stored in CSV format and uses Apache Spark for distributed processing. 
The results are displayed without truncation for full visibility.
The Spark session is created, queries are run against a temporary SQL view, 
and results are shown. The session is stopped at the end to free up resources.
"""


from pyspark.sql import SparkSession
# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("UsedCarAnalysis").getOrCreate()
# Step 2: Read CSV file
file_path = "/content/part-00000-f6621328-5892-4709-89ba-3ff15e6b235f-c000.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
# Step 3: Create a temp SQL table
df.createOrReplaceTempView("car_data")
# Step 4: Define SQL queries
queries = {
    "Query 1: Top 5 Regions with the Highest Average Price":
    """
    SELECT zip_code, ROUND(AVG(price), 2) AS avg_price
    FROM car_data
    WHERE zip_code IS NOT NULL
    GROUP BY zip_code
    ORDER BY avg_price DESC
    LIMIT 5
    """,

    "Query 2: Total Car Sales Revenue by Year":
    """
    SELECT year, SUM(price) AS total_revenue
    FROM car_data
    WHERE zip_code IS NOT NULL
    GROUP BY year
    ORDER BY year ASC
    """,

    "Query 3: Count of Cars by Transmission Type per Region":
    """
    SELECT zip_code, transmission, COUNT(*) AS car_count
    FROM car_data
    WHERE zip_code IS NOT NULL
    GROUP BY zip_code, transmission
    ORDER BY zip_code
    """,

    "Query 4: Monthly Trend in Car Listings":
    """
    SELECT SUBSTRING(posting_date, 1, 7) AS month, COUNT(*) AS car_listings
    FROM car_data
    WHERE zip_code IS NOT NULL
    GROUP BY SUBSTRING(posting_date, 1, 7)
    ORDER BY month ASC
    """,

    "Query 5: Top 3 Car Types with the Highest Total Sales in Each Region":
    """
    SELECT zip_code, type, SUM(price) AS total_sales
    FROM car_data
    WHERE zip_code IS NOT NULL
    GROUP BY zip_code, type
    ORDER BY zip_code, total_sales DESC
    LIMIT 3
    """
}
# Step 5: Execute queries and print results
for query_name, query in queries.items():
    print(f"\n--- {query_name} ---")
    print(f"SQL Query Executed: {query}")
    result_df = spark.sql(query)
    result_df.show(truncate=False)  # Display full output without truncation
# Step 6: Stop Spark session
spark.stop()