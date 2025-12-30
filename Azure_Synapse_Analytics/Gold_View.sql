/********************************************************************
 GOLD LAYER – AZURE SYNAPSE SERVERLESS VIEWS
 --------------------------------------------------------------------
 Purpose:
 - Expose curated Silver-layer Parquet data as analytical views
 - Enable fast, cost-efficient querying using Synapse Serverless
 - Serve data directly to Power BI without data duplication

 Architecture:
 ADLS Gen2 (Silver) → Synapse Serverless Views (Gold) → Power BI
********************************************************************/


-------------------
-- Gold View: Calendar
-- Description:
-- Creates a logical view over calendar data stored in Silver layer
-- Adds a semantic layer for reporting and time-based analysis
-------------------
CREATE VIEW gold.calendar
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) AS calendar_view;


-------------------
-- Gold View: Customers
-- Description:
-- Exposes cleaned and enriched customer data for analytics
-- Used in customer segmentation and sales analysis
-------------------
CREATE VIEW gold.customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) AS customers_view;


-------------------
-- Gold View: Product Subcategories
-- Description:
-- Provides product subcategory hierarchy for reporting
-- Used to enrich product-level analytics
-------------------
CREATE VIEW gold.product_subcategories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT = 'PARQUET'
) AS product_subcategories_view;


-------------------
-- Gold View: Products
-- Description:
-- Exposes standardized product data
-- Supports sales, inventory, and category-based analysis
-------------------
CREATE VIEW gold.products
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS products_view;


-------------------
-- Gold View: Returns
-- Description:
-- Provides product return data for quality and loss analysis
-- Can be joined with sales to compute return rates
-------------------
CREATE VIEW gold.returns
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS returns_view;


-------------------
-- Gold View: Sales
-- Description:
-- Central fact view containing transactional sales data
-- Primary dataset consumed by Power BI dashboards
-------------------
CREATE VIEW gold.sales
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) AS sales_view;


-------------------
-- Gold View: Territories
-- Description:
-- Provides geographical hierarchy for regional analysis
-- Used to analyze sales and customers by territory
-------------------
CREATE VIEW gold.territories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS territories_view;
