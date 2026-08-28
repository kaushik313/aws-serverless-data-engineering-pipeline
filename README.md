AWS Serverless Data Engineering Pipeline:
----------------------------------------
An AWS-based serverless ETL pipeline for processing Zomato restaurant data using Amazon S3, AWS Lambda, AWS Glue with PySpark, AWS Glue Crawler, AWS Glue Data Catalog, and Amazon Athena.

Project Overview:
-----------------
This project demonstrates an end-to-end cloud data engineering workflow in which raw restaurant data is stored in Amazon S3, an S3 event triggers an AWS Lambda function, and the Lambda function starts an AWS Glue ETL job.

The Glue ETL job uses PySpark to clean, transform, and analyze the restaurant dataset before writing curated datasets in Parquet format to Amazon S3. AWS Glue Crawler is then used to discover the schema and create metadata in the Glue Data Catalog. Amazon Athena uses the cataloged tables to perform SQL-based analytical queries.

Architecture:
-------------
![AWS Serverless ETL Pipeline Architecture](architecture/aws-pipeline-architecture.png)

Data Flow:
----------
```text
Raw CSV Data
     |
     v
Amazon S3
     |
     | S3 Object Event
     v
AWS Lambda
     |
     | Starts Glue Job
     v
AWS Glue ETL
(PySpark)
     |
     | Data Cleaning & Transformation
     v
Amazon S3
(Curated Parquet Data)
     |
     v
AWS Glue Crawler
     |
     v
AWS Glue Data Catalog
     |
     v
Amazon Athena
(SQL Analytics)