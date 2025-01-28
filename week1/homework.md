Question 1: Understanding Docker first run

docker run -it python:3.12.8 bash

Once inside the container, run:

pip --version


Question 2: Docker networking and docker-compose
The `hostname` for the PostgreSQL container is set to `db` (the name of the container). The port for PostgreSQL is mapped from `5433` on the host to `5432` inside the container.


Question 3: Trip Segmentation Count
By querying the `green_tripdata_2019-10.csv.gz` data loaded into PostgreSQL.


SELECT
  COUNT(*) AS count,
  CASE
    WHEN trip_distance <= 1 THEN 'Up to 1 mile'
    WHEN trip_distance > 1 AND trip_distance <= 3 THEN '1 to 3 miles'
    WHEN trip_distance > 3 AND trip_distance <= 7 THEN '3 to 7 miles'
    WHEN trip_distance > 7 AND trip_distance <= 10 THEN '7 to 10 miles'
    WHEN trip_distance > 10 THEN 'Over 10 miles'
  END AS distance_range
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
GROUP BY distance_range
ORDER BY distance_range;



Question 4: Longest trip for each day

SELECT
  DATE(lpep_pickup_datetime) AS pickup_date,
  MAX(trip_distance) AS max_trip_distance
FROM green_tripdata
WHERE lpep_pickup_datetime BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY pickup_date
ORDER BY max_trip_distance DESC
LIMIT 1;



Question 5: Three biggest pickup zones

SELECT
  pickup_zone,
  SUM(total_amount) AS total_amount
FROM green_tripdata
WHERE lpep_pickup_datetime = '2019-10-18'
GROUP BY pickup_zone
HAVING SUM(total_amount) > 13000
ORDER BY total_amount DESC
LIMIT 3;


Question 6: Largest tip

SELECT
  dropoff_zone,
  MAX(tip_amount) AS max_tip
FROM green_tripdata
WHERE pickup_zone = 'East Harlem North' AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 10
GROUP BY dropoff_zone
ORDER BY max_tip DESC
LIMIT 1;


Question 7: Terraform Workflow


1. terraform init: Download provider plugins and set up backend.
2. terraform apply -auto-approve: Generate proposed changes and apply them automatically.
3. terraform destroy Remove all resources managed by Terraform.

So, the correct answer is terraform init, terraform apply -auto-approve, terraform destroy.
