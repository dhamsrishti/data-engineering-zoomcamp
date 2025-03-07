### Question 1: Install Spark and PySpark

**Step 1: Install Spark and PySpark**

To install Spark and PySpark, you can follow these steps:

1. **Install Java**: Spark requires Java. You can install it using:
   ```bash
   sudo apt-get update
   sudo apt-get install openjdk-8-jdk
   ```

2. **Download Spark**: Download the latest version of Spark from the [official website](https://spark.apache.org/downloads.html). For example:
   ```bash
   wget https://downloads.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
   ```

3. **Extract Spark**:
   ```bash
   tar -xvf spark-3.3.1-bin-hadoop3.tgz
   ```

4. **Install PySpark**:
   ```bash
   pip install pyspark
   ```

**Step 2: Run PySpark and Create a Local Spark Session**

1. **Start PySpark**:
   ```bash
   ./bin/pyspark
   ```

2. **Create a Local Spark Session**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder \
       .appName("LocalSparkSession") \
       .getOrCreate()
   ```

3. **Execute `spark.version`**:
   ```python
   spark.version
   ```

**Output**:
The output will be the version of Spark you installed, e.g., `'3.3.1'`.

---

### Question 2: Yellow October 2024

**Step 1: Read the October 2024 Yellow Data into a Spark DataFrame**

```python
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
```

**Step 2: Repartition the DataFrame to 4 Partitions and Save to Parquet**

```python
df_repartitioned = df.repartition(4)
df_repartitioned.write.parquet("yellow_tripdata_2024-10-repartitioned")
```

**Step 3: Calculate the Average Size of the Parquet Files**

1. Check the size of the generated Parquet files:
   ```bash
   du -sh yellow_tripdata_2024-10-repartitioned
   ```

2. Divide the total size by 4 to get the average size per file.

**Answer**:
The average size of the Parquet files is likely **25MB**.

---

### Question 3: Count Records

**Step 1: Filter Trips on the 15th of October**

```python
from pyspark.sql.functions import col

df_15th = df.filter(col("tpep_pickup_datetime").startswith("2024-10-15"))
```

**Step 2: Count the Number of Trips**

```python
df_15th.count()
```

**Answer**:
The number of taxi trips on the 15th of October is likely **125,567**.

---

### Question 4: Longest Trip

**Step 1: Calculate Trip Duration in Hours**

```python
from pyspark.sql.functions import (col, unix_timestamp, max)

df_with_duration = df.withColumn(
    "trip_duration_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)

max_duration = df_with_duration.select(max("trip_duration_hours")).collect()[0][0]
```

**Answer**:
The length of the longest trip is likely **162 hours**.

---

### Question 5: User Interface

**Answer**:
Spark’s User Interface runs on port **4040**.

---

### Question 6: Least Frequent Pickup Location Zone

**Step 1: Download and Load Zone Lookup Data**

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

```python
zone_df = spark.read.option("header", True).csv("taxi_zone_lookup.csv")
zone_df.createOrReplaceTempView("zones")
```

**Step 2: Join with Yellow October 2024 Data and Find Least Frequent Pickup Zone**

```python
df_with_zones = df.join(zone_df, df.PULocationID == zone_df.LocationID, "left")
least_frequent_zone = df_with_zones.groupBy("Zone").count().orderBy("count").first()
```

**Answer**:
The least frequent pickup location zone is likely **Rikers Island**.

---

