

#### 1. **Loading Data into GCS Bucket**
I manually downloaded the Yellow Taxi Trip Records Parquet files for January 2024 - June 2024 from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and uploaded them to my Google Cloud Storage (GCS) bucket using the following script:

```python
from google.cloud import storage

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Upload all 6 files
files = [
    "yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet",
    "yellow_tripdata_2024-03.parquet",
    "yellow_tripdata_2024-04.parquet",
    "yellow_tripdata_2024-05.parquet",
    "yellow_tripdata_2024-06.parquet"
]

bucket_name = "your-bucket-name"
for file in files:
    upload_to_gcs(bucket_name, file, file)
```

---

#### 2. **BigQuery Setup**
##### a. **Create an External Table**
I created an external table in BigQuery using the Parquet files stored in the GCS bucket:

```sql
CREATE OR REPLACE EXTERNAL TABLE `your_project.your_dataset.yellow_taxi_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket-name/yellow_tripdata_2024-*.parquet']
);
```

##### b. **Create a Regular Table**
I created a regular (materialized) table in BigQuery using the external table:

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.yellow_taxi_materialized`
AS SELECT * FROM `your_project.your_dataset.yellow_taxi_external`;
```

---

### Answers to Questions

#### **Question 1: Count of Records for the 2024 Yellow Taxi Data**
```sql
SELECT COUNT(*) AS record_count
FROM `your_project.your_dataset.yellow_taxi_materialized`;
```
**Answer:** `85,431,289`

---

#### **Question 2: Distinct PULocationIDs and Estimated Data Read**
```sql
-- For External Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `your_project.your_dataset.yellow_taxi_external`;

-- For Materialized Table
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocation_ids
FROM `your_project.your_dataset.yellow_taxi_materialized`;
```
**Answer:** `0 MB for the External Table and 155.12 MB for the Materialized Table`

---

#### **Question 3: Retrieving PULocationID and DOLocationID**
```sql
-- Query 1: Retrieve PULocationID
SELECT PULocationID
FROM `your_project.your_dataset.yellow_taxi_materialized`;

-- Query 2: Retrieve PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM `your_project.your_dataset.yellow_taxi_materialized`;
```
**Answer:** `BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.`

---

#### **Question 4: Records with fare_amount = 0**
```sql
SELECT COUNT(*) AS zero_fare_records
FROM `your_project.your_dataset.yellow_taxi_materialized`
WHERE fare_amount = 0;
```
**Answer:** `8,333`

---

#### **Question 5: Optimized Table Strategy**
**Answer:** `Partition by tpep_dropoff_datetime and Cluster on VendorID`

---

#### **Question 6: Distinct VendorIDs Between 2024-03-01 and 2024-03-15**
```sql
-- Query on Non-Partitioned Table
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_taxi_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Query on Partitioned Table
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
```
**Answer:** `310.24 MB for non-partitioned table and 26.84 MB for the partitioned table`

---

#### **Question 7: Data Storage in External Table**
**Answer:** `GCP Bucket`

---

#### **Question 8: Best Practice to Always Cluster Data**
**Answer:** `False`

---

#### **Question 9: Bonus - SELECT COUNT(*) Query**
```sql
SELECT COUNT(*)
FROM `your_project.your_dataset.yellow_taxi_materialized`;
```
**Answer:** The estimated bytes read will be `0 MB` because BigQuery caches metadata about the table, and `COUNT(*)` does not require scanning the actual data.

