# Incremental Sales Data Loading using Azure

## Overview
This project implements an incremental data loading pipeline for sales data using Microsoft Azure services. The solution efficiently ingests new and updated records while ensuring data consistency and performance.

## Architecture
The solution leverages the following Azure services:

- **Azure Data Lake Storage (ADLS)**: Stores raw and processed sales data.
- **Azure Data Factory (ADF)**: Orchestrates data movement and transformation.
- **Azure Databricks**: Processes incremental data and prepares it for analysis.


## Data Flow
1. **Ingestion**: Sales data is ingested from various sources (CSV, JSON, databases, APIs) into ADLS.
2. **Incremental Processing**: ADF triggers Databricks notebooks to identify new and updated records using a watermark strategy.
3. **Transformation**: Databricks cleans and transforms the data, ensuring quality and consistency.


## Incremental Load Strategy
- **Watermarking**: ADF or Databricks tracks the last processed timestamp to fetch only new or modified records.
- **Merge (Upsert) Operation**: Ensures new records are inserted, and existing records are updated.

## Prerequisites
- Azure Subscription with required permissions
- Azure Data Lake Storage configured
- Azure Data Factory instance
- Databricks workspace


## Technologies Used
- Azure Data Lake Storage (ADLS Gen2)
- Azure Data Factory (ADF)
- Azure Databricks (PySpark, Delta Lake)

## Usage
1. Upload new sales data to the source system.
2. ADF triggers the pipeline to detect and process changes.
3. Databricks cleans and transforms the incremental data.
4. Processed data is loaded into Synapse Analytics for reporting.
5. Monitor pipeline execution using Azure Monitor.
   
## Screenshorts
![App Screenshot](https://github.com/salmansajidsattar/Incremental_Sales_Data_Load_Azure/blob/main/img/Screenshot%202025-02-07%20204831.png)
![App Screenshot](https://github.com/salmansajidsattar/Incremental_Sales_Data_Load_Azure/blob/main/img/Screenshot%202025-02-08%20013306.png)
![App Screenshot](https://github.com/salmansajidsattar/Incremental_Sales_Data_Load_Azure/blob/main/img/Incremental_pipeline.png)


## Contact
For any queries or issues, reach out via [GitHub Issues](https://github.com/your-repo/issues) or email `salmansajidsattar@gmail.com`.

