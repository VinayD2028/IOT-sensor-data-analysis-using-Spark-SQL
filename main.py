from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, dense_rank
from pyspark.sql.window import Window

def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("IoT Sensor Analysis") \
        .getOrCreate()

    # Task 1: Load Data & Basic Exploration
    print("\n=== TASK 1 ===")
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)
    df.createOrReplaceTempView("sensor_readings")
    
    print("First 5 rows:")
    df.show(5)
    print(f"Total records: {df.count()}")
    print("Distinct locations:")
    spark.sql("SELECT DISTINCT location FROM sensor_readings").show()
    
    df.limit(5).write.csv("task1_output.csv", header=True, mode="overwrite")

    # Task 2: Filtering & Aggregations
    print("\n=== TASK 2 ===")
    in_range = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
    out_of_range = df.count() - in_range.count()
    print(f"In-range: {in_range.count()}, Out-of-range: {out_of_range}")

    agg_df = df.groupBy("location") \
        .agg({"temperature": "avg", "humidity": "avg"}) \
        .orderBy("avg(temperature)", ascending=False)
    
    agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")

    # Task 3: Time-Based Analysis
    print("\n=== TASK 3 ===")
    df_time = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    df_time.createOrReplaceTempView("sensor_readings")
    
    hourly_avg = df_time.groupBy(hour("timestamp").alias("hour_of_day")) \
        .agg({"temperature": "avg"}) \
        .orderBy("avg(temperature)", ascending=False)
    
    hourly_avg.write.csv("task3_output.csv", header=True, mode="overwrite")

    # Task 4: Window Ranking
    print("\n=== TASK 4 ===")
    window_spec = Window.orderBy(col("avg_temp").desc())
    sensor_ranking = df.groupBy("sensor_id") \
        .agg({"temperature": "avg"}) \
        .withColumnRenamed("avg(temperature)", "avg_temp") \
        .withColumn("rank_temp", dense_rank().over(window_spec)) \
        .limit(5)
    
    sensor_ranking.write.csv("task4_output.csv", header=True, mode="overwrite")

    # Task 5: Pivot Analysis
    print("\n=== TASK 5 ===")
    df_with_hour = df_time.withColumn("hour_of_day", hour("timestamp"))
    pivot_df = df_with_hour.groupBy("location") \
        .pivot("hour_of_day", list(range(24))) \
        .agg({"temperature": "avg"})
    
    # Find maximum temperature point
    max_row = pivot_df.rdd.map(lambda row: (row.location, [(h, row[str(h)]) for h in range(24)])).map(
        lambda x: (x[0], max(x[1], key=lambda y: y[1] if y[1] is not None else -1))).collect()
    max_temp_point = max(max_row, key=lambda x: x[1][1])
    
    print(f"Highest temperature at {max_temp_point[0]} during hour {max_temp_point[1][0]}: {max_temp_point[1][1]:.1f}°C")
    
    pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")

    # Cleanup
    spark.stop()
    print("\nAll tasks completed successfully!")

if __name__ == "__main__":
    main()
