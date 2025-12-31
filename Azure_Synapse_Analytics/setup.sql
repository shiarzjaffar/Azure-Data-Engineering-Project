/********************************************************************
 AZURE SYNAPSE SERVERLESS – GOLD SCHEMA SETUP
 --------------------------------------------------------------------
 Purpose:
 - Create a logical schema for Gold-layer analytics
 - Gold schema contains curated, business-ready datasets
********************************************************************/


-------------------------------
-- Create Gold Schema
-- Description:
-- Logical container for curated analytical objects (views / tables)
-- Follows Bronze → Silver → Gold lakehouse architecture
-------------------------------
CREATE SCHEMA gold;
GO


-------------------------------
-- Create Master Key (Required for credentials & security features)
-- Description:
-- Used by Synapse to encrypt sensitive metadata
-- NOTE:
-- Password is NOT included here for security reasons.
-- In real environments, this should be created once
-- and stored securely (Key Vault / secure deployment).
-------------------------------
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<STRONG_PASSWORD>';
GO


-------------------------------
-- Validation Query
-- Description:
-- Simple query to verify that Gold-layer views are accessible
-- Confirms schema creation and external view configuration
-------------------------------
SELECT *
FROM gold.customers;
