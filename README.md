# Project: Sparkify - Data Pipelines with Airflow

---
## Project Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

---
## Problem Discription

In this project I fulfilled the complete ETL process by creating a data pipeline using Apache Airflow. Beginning with the Sparkify's raw datasets which are stored in S3 bucket, I formulated then custom operators for inserting automatically the raw data into redshift tables. These table follow a Star Schema.
Finally I ran data quality chacks, which are basically simple SQL queries for each table.

The whole process can be conducted and observed via Apache Airflow

The intent for tis for Data Scientists to use the solution to train machine learning models or for Data Analysts to answer dedicated question about the customers behaviour. 

---
## Project Datasets
The provided datasets for customers and music sessions are stored in JSON format in the `udacity-dend` S3 bucket and has following links.
* Log-Data: `s3://udacity-dend/log_data`
* JSON form: `s3://udacity-dend/log_json_path.json`
* Song-Data: `s3://udacity-dend/log_data`

---
## Prerequisites

* Create an IAM User in AWS with the necessary rights.
* Configure a Redshift Serverless workspace in AWS, which is open for external access.
* By using the Query Editor from Redshift Serverless it is mandatory to run the sql queries from `create_tables.sql` to prior create the star schema tables.

---
## Structure

<details>
<summary>
/dags
</summary>

This folder contains `final_project.py`, which describes the code for the complete DAG. It also descibes different tasks and orders them in after appropriate dependencies. 

**1- Customer Landing Table:**

![alt text](AthenaQueries/Screenshots/customer_landing.png)

**2- Accelerometer Landing Table:**

![alt text](AthenaQueries/Screenshots/accelerometer_landing.png)

**3- Step Trainer Landing Table:**

![alt text](AthenaQueries/Screenshots/step_trainer_landing.png)

</details>

<details>
<summary>
Trusted Zone
</summary>

In the Trusted Zone, I created AWS Glue jobs to transform the raw data from the landing zones to the corresponding trusted zones. In conclusion, it only contains customer records from people who agreed to share their data.

**Glue job scripts**

[1. customer_landing_to_trusted.py](GlueETL/Customer/customer_landing_to_trusted.py) - This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.

[2. accelerometer_landing_to_trusted.py](GlueETL/accelerometer/accelerometer_landing_to_trusted.py) - This script transfers accelerometer data from the 'landing' to 'trusted' zones. Using a join on customer_trusted and accelerometer_landing, It filters for Accelerometer readings from customers who have agreed to share data with researchers.

[3. step_trainer_landing_to_trusted.py](GlueETL/StepTrainer/step_trainer_landing_to_trusted.py) - This script transfers Step Trainer data from the 'landing' to 'trusted' zones. Using a join on customer_curated and step_trainer_landing, It filters for customers who have accelerometer data and have agreed to share their data for research with Step Trainer readings.

The customer_trusted table was queried in Athena.
The following images show relevant Athena Queries to verify the correct table creation and the correct amount of data points.

![alt text](AthenaQueries/Screenshots/customer_trusted.png)

Verification, that customer_trusted only shows customers, who agreed using their data (therefore "sharewithresearchasofdate" column must be empty).
![alt text](AthenaQueries/Screenshots/customer_trusted_verified.png)

</details>

<details>
<summary>
Curated Zone
</summary>

In the Curated Zone I created AWS Glue jobs to make further transformations, to meet the specific needs of a particular analysis. E.g. the tables were reduced to only show necessary data.

**Glue job scripts**

[customer_trusted_to_curated.py](GlueETL/Customer/customer_trusted_to_curated.py) - This script transfers customer data from the 'trusted' to 'curated' zones. Using a join on customer_trusted and accelerometer_landing, It filters for customers with Accelerometer readings and have agreed to share data with researchers.

[create_machine_learning_curated.py](GlueETL/StepTrainer/create_machine_learning_curated.py): This script is used to build aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data.

The following images show relevant Athena Queries to verify the correct table creation and the correct amount of data points.

![alt text](AthenaQueries/Screenshots/machine_learning_curated.png)

</details>
