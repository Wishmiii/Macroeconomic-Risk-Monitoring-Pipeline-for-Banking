# End-to-end Data Pipeline for a Banking Risk Monitoring system

An end-to-end cloud-based data engineering pipeline that ingests macroeconomic data from the FRED API, processes it using AWS Glue, and loads it into a star schema data mart for banking risk analytics.

## Project Overview

Banks use macroeconomic indicators such as unemployment, inflation, interest rates, and GDP to assess economic conditions and support decisions related to lending, risk management, and financial strategy.

This project automates that process by building a data pipeline that:

- extracts macroeconomic data from the FRED API
- stores raw and processed data in Amazon S3
- performs ETL transformations using AWS Glue and Spark
- calculates a business-oriented risk score
- organizes the final output into a star schema for BI/reporting use cases

## Use Case

The pipeline is designed to support a **banking risk monitoring dashboard**.

The final dataset enables downstream BI tools to:

- track economic conditions over time
- identify high-risk periods
- analyze macroeconomic risk trends
- support lending and credit policy decisions

## Dataset

The project uses data from the **FRED (Federal Reserve Economic Data) API**.

Selected indicators:

- `UNRATE` - Unemployment Rate
- `CPIAUCSL` - Consumer Price Index (Inflation)
- `FEDFUNDS` - Federal Funds Rate
- `GDP` - Gross Domestic Product

These indicators were selected based on their relevance to macroeconomic banking risk.

## Architecture

```text
FRED API
   ↓
Glue Python Shell Job (Ingestion)
   ↓
Amazon S3 Raw Layer
   ↓
Amazon S3 Processed Layer
   ↓
Glue Spark ETL Job
   ↓
Amazon S3 Analytics Layer
   ↓
Glue Spark Star Schema Job
   ↓
Amazon S3 Datamart Layer
