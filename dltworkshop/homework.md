

### Question 1: dlt Version

To check the version of `dlt`, you can run the following code:

```python
import dlt
print("dlt version:", dlt.__version__)
```

**Output:**
```
dlt version: 1.6.1
```

Replace `x.x.x` with the actual version number you see in the output.

### Question 2: Define & Run the Pipeline (NYC Taxi API)

Here’s how you can define and run the pipeline to extract data from the NYC Taxi API and load it into DuckDB:

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Define the API source with pagination
@dlt.resource
def ny_taxi():
    client = RESTClient(base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api")
    paginator = PageNumberPaginator(starting_page=1)
    while True:
        response = client.get("/data", params={"page": paginator.current_page})
        yield response.json()
        if not response.json():
            break
        paginator.next_page()

# Define the pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# Run the pipeline
load_info = pipeline.run(ny_taxi)
print(load_info)
```

After running the pipeline, you can connect to the DuckDB database and check the tables:

```python
import duckdb
from google.colab import data_table
data_table.enable_dataframe_formatter()

# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
conn.sql("DESCRIBE").df()
```

**Question:** How many tables were created?

**Answer:** 4

### Question 3: Explore the loaded data

To inspect the `rides` table and get the total number of records extracted:

```python
df = pipeline.dataset(dataset_type="default").rides.df()
df
```

**Question:** What is the total number of records extracted?

**Answer:** 10000

### Question 4: Trip Duration Analysis

To calculate the average trip duration in minutes, run the following SQL query:

```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """
        )
    # Prints column values of the first row
    print(res)
```

**Question:** What is the average trip duration?

**Answer:** 22.3049

