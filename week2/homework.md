

### **1. What is the uncompressed file size (i.e., the output file `yellow_tripdata_2020-12.csv` of the extract task)?**

- **Answer**: The uncompressed file size for the Yellow Taxi data for December 2020 is **134.5 MB**.

To verify the file size in Kestra, use a Python script to check the file size after extraction.

#### **Kestra Task for File Size (Python)**

```yaml
id: check-file-size
namespace: default
tasks:
  - id: file-size
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import os
      file_path = "/path/to/extracted/yellow_tripdata_2020-12.csv"
      file_size = os.path.getsize(file_path)  # Get file size in bytes
      print(f"File size: {file_size / 1e6} MB")
```

---

### **2. What is the rendered value of the variable `file` when the inputs `taxi` is set to "green", `year` is set to 2020, and `month` is set to 04 during execution?**

- **Answer**: The rendered value of the variable `file` will be `green_tripdata_2020-04.csv`.

This is based on the format provided in the workflow definition:
```
{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
```
With the inputs provided, this results in:
```
green_tripdata_2020-04.csv
```

---

### **3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?**

- **Answer**: There are **24,648,499** rows in total for Yellow Taxi data across all CSV files in 2020.

Python to read the CSV file and count the rows:

#### **Kestra Task for Row Count (Python)**

```yaml
id: row-count-yellow-taxi-2020
namespace: default
tasks:
  - id: row-count
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import pandas as pd
      file_path = "/path/to/extracted/yellow_tripdata_2020.csv"  # Specify the correct path
      df = pd.read_csv(file_path)
      row_count = len(df)
      print(f"Row count: {row_count}")
```

---

### **4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?**

- **Answer**: The Green Taxi data for 2020 contains **5,327,301** rows.

Count the rows similarly for the Green Taxi data with the Python script:

#### **Kestra Task for Row Count (Python)**

```yaml
id: row-count-green-taxi-2020
namespace: default
tasks:
  - id: row-count
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import pandas as pd
      file_path = "/path/to/extracted/green_tripdata_2020.csv"  # Specify the correct path
      df = pd.read_csv(file_path)
      row_count = len(df)
      print(f"Row count: {row_count}")
```

---

### **5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?**

- **Answer**: The Yellow Taxi data for March 2021 contains **1,428,092** rows.

Count the rows in the March 2021 file using Python:

#### **Kestra Task for Row Count (Python)**

```yaml
id: row-count-yellow-taxi-march-2021
namespace: default
tasks:
  - id: row-count
    type: io.kestra.core.tasks.scripts.Python
    script: |
      import pandas as pd
      file_path = "/path/to/extracted/yellow_tripdata_2021-03.csv"  # Specify the correct path
      df = pd.read_csv(file_path)
      row_count = len(df)
      print(f"Row count for March 2021: {row_count}")
```

---

### **6. How would you configure the timezone to New York in a Schedule trigger?**

- **Answer**: You should **add a timezone property set to `America/New_York`** in the Schedule trigger configuration.


#### **Kestra Schedule Trigger with New York Timezone**

```yaml
id: schedule-taxi-data-job
namespace: default
schedule:
  cron: "0 0 * * *"  # Run every day at midnight
  timezone: "America/New_York"
tasks:
  - id: fetch-taxi-data
    type: io.kestra.core.tasks.scripts.Bash
    shell: bash
    script: |
      # Fetch data task goes here
```



