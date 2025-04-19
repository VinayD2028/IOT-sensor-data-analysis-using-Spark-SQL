# iot-sensor-data-spark-sql

## Overview
This project analyzes IoT sensor data using Spark SQL to uncover temperature patterns, humidity trends, and sensor performance metrics. The structured data from simulated IoT devices is processed to gain insights into environmental conditions across different locations.

## Dataset Description
**sensor_data.csv** contains IoT sensor readings with:
- `sensor_id` - Unique identifier for each sensor
- `timestamp` - Date and time of reading (e.g., 2025-03-28 08:15:22)
- `temperature` - Temperature reading in Celsius (rounded to 1 decimal)
- `humidity` - Humidity percentage (rounded to 1 decimal)
- `location` - Physical location of sensor (e.g., BuildingA_Floor1)
- `sensor_type` - Device model/type (TypeA, TypeB, TypeC)

## Tasks and Outputs
The analysis includes these tasks with CSV outputs:

1. **Basic Data Exploration**  
   - First 5 records validation  
   - Total record count  
   - Distinct locations  
   *Output: task1_output.csv*

2. **Temperature Filtering & Aggregations**  
   - Identify in-range (18-30°C) vs out-of-range readings  
   - Average temperature/humidity per location  
   *Output: task2_output.csv*

3. **Time-Based Analysis**  
   - Convert timestamp to proper format  
   - Hourly average temperature patterns  
   *Output: task3_output.csv*

4. **Sensor Performance Ranking**  
   - Rank sensors by average temperature  
   - Top 5 highest temperature sensors  
   *Output: task4_output.csv*

5. **Location-Hour Pivot Analysis**  
   - 24-hour temperature heatmap per location  
   - Identify hottest (location, hour) combination  
   *Output: task5_output.csv*

## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Faker*:
   - Install using pip:
     ```bash
     pip install faker
     ```

### *2. Running the Analysis Tasks*

You can run the analysis tasks either locally or using Docker.

#### *a. Running Locally*

1. *Generate the Input*:
   ```bash
   python3 data_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
   python3 main.py
   ``` 
   

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   bash
   ls outputs/
   

## Key Insights
1. **Location Trends**  
   Upper floors (Floor2) tend to be warmer than lower floors  
   BuildingB shows higher average temperatures than BuildingA

2. **Temporal Patterns**  
   Peak temperatures typically occur in afternoon hours (14:00-16:00)  
   Night hours (00:00-05:00) show most stable readings

3. **Sensor Performance**  
   TypeA sensors report higher average temperatures  
   Top-performing sensors show <0.5°C variation in readings

## Error Handling
1. **Timestamp Parsing Errors**  
   *Solution:* Explicitly specify format using `to_timestamp()`
    ```bash
    df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    ```

2. **Temperature Outliers**  
   *Solution:* Filter using conditional expressions
   ```bash
   df.filter((col("temperature") >= 18) & (col("temperature") <= 30))
   ```  
3. **Pivot Table Memory Issues**  
   *Solution:* Limit pivot columns using explicit hour range
   ```bash
   .pivot("hour_of_day", list(range(24)))
   ```  
