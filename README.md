# Real-Time Data Quality Monitor

A real-time data quality monitoring pipeline built using Kafka, Apache Spark Structured Streaming, Python, and AWS-ready data engineering concepts.

## Project Overview

The system continuously generates simulated business transaction data and publishes it to Apache Kafka.

Spark Structured Streaming consumes the Kafka stream and performs automated data quality validation including:

- Missing value detection
- Invalid value detection
- Schema violation detection
- Duplicate transaction detection
- Rule-based anomaly detection
- Machine learning based anomaly detection using Isolation Forest

The processed data is stored in Parquet format as a Silver data layer along with quality results and real-time data quality metrics.

## Architecture

Python Data Generator
        |
        v
Apache Kafka
        |
        v
Spark Structured Streaming
        |
        +----------------------+
        |                      |
        v                      v
Data Quality Rules       Isolation Forest
        |                      |
        +----------+-----------+
                   |
                   v
          Quality-Enriched Data
                   |
          +--------+--------+
          |        |        |
          v        v        v
       Silver   Quality   Metrics
       Layer    Results

## Data Quality Checks

### 1. Missing Values

Detects missing values in important transaction fields.

### 2. Invalid Values

Validates business rules such as:

- Transaction amount
- Payment method
- Transaction status

### 3. Schema Violations

Detects missing or unexpected fields.

### 4. Duplicate Records

Identifies duplicate transaction IDs.

### 5. Rule-Based Anomalies

Detects unusually high transaction amounts.

### 6. Machine Learning Anomalies

Isolation Forest is used to identify unusual transaction patterns.

## Technology Stack

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PySpark
- Pandas
- Scikit-learn
- Parquet
- Linux / WSL
- Git / GitHub

## Project Structure

```text
config/
    quality_rules.json

models/
    isolation_forest.pkl

src/
    anomaly/
    ml/
    producer/
    streaming/

data/
    silver/
    quality_results/
    metrics/

requirements.txt
