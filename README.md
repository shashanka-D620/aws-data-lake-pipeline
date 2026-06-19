# Automated AWS Data Lake Pipeline Using S3, Lambda, Glue, Athena and Airflow

## Project Overview

Designed and implemented an end-to-end serverless AWS Data Lake pipeline for automated data processing, transformation, cataloging, validation, and analytics.

## Architecture

S3 Raw Layer
↓
Lambda Trigger
↓
Glue ETL Job
↓
S3 Processed Layer
↓
Airflow Scheduler
↓
S3KeySensor
↓
Glue Crawler
↓
Glue Data Catalog
↓
Data Quality Check
↓
Athena Query
↓
Email Notification

## Technologies Used

* Amazon S3
* AWS Lambda
* AWS Glue
* AWS Glue Crawler
* AWS Glue Data Catalog
* Amazon Athena
* Apache Airflow
* Python
* IAM

## Workflow

* Customer data files are uploaded to the S3 Raw Layer.
* S3 event notifications trigger an AWS Lambda function.
* Lambda starts an AWS Glue ETL job.
* Glue transforms and cleans the data.
* Processed data is stored in the S3 Processed Layer.
* Apache Airflow monitors the processed folder using S3KeySensor.
* Airflow triggers a Glue Crawler to update the Glue Data Catalog.
* Data quality validation checks are executed.
* Athena queries are run on the cataloged data.
* Email notifications are sent after successful pipeline completion.

## Features

* Automated file ingestion
* Serverless ETL processing
* Metadata cataloging
* Data quality validation
* Athena-based analytics
* Automated workflow orchestration
* Email notifications and monitoring

## Project Outcome

Built a fully automated data pipeline that eliminates manual intervention, improves data availability for analytics, and enables SQL-based querying directly on data stored in Amazon S3 using Amazon Athena.
