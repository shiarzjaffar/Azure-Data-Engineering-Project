/********************************************************************
 AZURE SYNAPSE SERVERLESS – EXTERNAL TABLE SETUP
 --------------------------------------------------------------------
 Purpose:
 - Use Managed Identity to securely access ADLS Gen2
 - Define external data sources for Silver and Gold layers
 - Create an external table in Gold for optimized querying
 - Persist query results as Parquet files in ADLS

 Architecture:
 Synapse Serverless → ADLS Gen2 (Gold)
********************************************************************/


-------------------------------
-- Create Database Scoped Credential
-- Description:
-- Uses Azure Managed Identity for secure, passwordless access
-- Avoids hardcoded secrets and supports enterprise security practices
-------------------------------
CREATE DATABASE SCOPED CREDENTIAL cred_shiraz
WITH IDENTITY = 'Managed Identity';


-------------------------------
-- External Data Source: Silver Layer
-- Description:
-- Points to curated Silver-layer data stored in ADLS Gen2
-------------------------------
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://mystorageaccount123.dfs.core.windows.net/silver/',
    CREDENTIAL = cred_shiraz
);


-------------------------------
-- External Data Source: Gold Layer
-- Description:
-- Target location for Gold-layer analytical outputs
-------------------------------
CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://mystorageaccount123.dfs.core.windows.net/gold/',
    CREDENTIAL = cred_shiraz
);


-------------------------------
-- External File Format: Parquet
-- Description:
-- Defines Parquet as the storage format
-- Uses Snappy compression for performance and storage efficiency
-------------------------------
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


-------------------------------
-- Create External Table: Gold.extsales
-- Description:
-- Materializes the Gold sales view into Parquet files
-- Enables faster query performance and reuse across analytics
-- Data is stored in ADLS Gen2 under the Gold layer
-------------------------------
CREATE EXTERNAL TABLE gold.extsales
WITH (
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT *
FROM gold.sales;


-------------------------------
-- Validation Query
-- Description:
-- Verifies that the external table is accessible and queryable
-------------------------------
SELECT *
FROM gold.extsales;
