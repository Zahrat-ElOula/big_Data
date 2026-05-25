# Databricks notebook source
# DBTITLE 1,Gold Layer - Star Schema Data Model Documentation
# MAGIC %md
# MAGIC # Gold Layer - Star Schema Data Model Documentation
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook documents the **Gold Layer** dimensional model implemented as a **star schema** for analytics and reporting. The star schema design provides:
# MAGIC
# MAGIC * **Simplified queries** - Denormalized dimensions enable straightforward joins
# MAGIC * **Fast aggregations** - Optimized for analytical workloads
# MAGIC * **Business-friendly** - Dimension tables contain descriptive attributes
# MAGIC * **Scalable** - Efficient for large fact tables with millions of rows
# MAGIC
# MAGIC ## Data Model Architecture
# MAGIC The Gold layer transforms the Silver layer (cleaned operational data) into a dimensional model with:
# MAGIC * **1 Fact Table**: `fact_sales` - measures and metrics (sales transactions)
# MAGIC * **3 Dimension Tables**: `dim_customers`, `dim_products`, `dim_date` - descriptive attributes
# MAGIC
# MAGIC **Catalog**: `bigdata_catalog`  
# MAGIC **Schema**: `gold`

# COMMAND ----------

# DBTITLE 1,Star Schema Diagram
# MAGIC %md
# MAGIC ## Star Schema Diagram
# MAGIC
# MAGIC ```
# MAGIC                     ┌─────────────────────────┐
# MAGIC                     │     dim_customers       │
# MAGIC                     ├─────────────────────────┤
# MAGIC                     │ • customer_key (PK)     │
# MAGIC                     │ • customer_id           │
# MAGIC                     │ • customer_number       │
# MAGIC                     │ • first_name            │
# MAGIC                     │ • last_name             │
# MAGIC                     │ • gender                │
# MAGIC                     │ • marital_status        │
# MAGIC                     │ • country               │
# MAGIC                     │ • birth_date            │
# MAGIC                     └──────────┬──────────────┘
# MAGIC                                │
# MAGIC                                │ 1:N
# MAGIC                                │
# MAGIC     ┌──────────────────────────▼──────────────────────────┐
# MAGIC     │                   fact_sales                         │
# MAGIC     ├──────────────────────────────────────────────────────┤
# MAGIC     │ • sales_key (PK)                                     │
# MAGIC     │ • customer_key (FK) ──────────────────────┐          │
# MAGIC     │ • product_key (FK) ────────────┐          │          │
# MAGIC     │ • order_date_key (FK) ──┐      │          │          │
# MAGIC     │ • order_number          │      │          │          │
# MAGIC     │ • sales_amount          │      │          │          │
# MAGIC     │ • quantity              │      │          │          │
# MAGIC     │ • unit_price            │      │          │          │
# MAGIC     └─────────────────────────┼──────┼──────────┼──────────┘
# MAGIC                               │      │          │
# MAGIC                               │ N:1  │ N:1      │ N:1
# MAGIC                               │      │          │
# MAGIC          ┌────────────────────▼──┐   │          │
# MAGIC          │     dim_date          │   │          │
# MAGIC          ├───────────────────────┤   │          │
# MAGIC          │ • date_key (PK)       │   │          │
# MAGIC          │ • date                │   │          │
# MAGIC          │ • year                │   │          │
# MAGIC          │ • quarter             │   │          │
# MAGIC          │ • month               │   │          │
# MAGIC          │ • month_name          │   │          │
# MAGIC          │ • day                 │   │          │
# MAGIC          │ • day_of_week         │   │          │
# MAGIC          │ • day_name            │   │          │
# MAGIC          │ • is_weekend          │   │          │
# MAGIC          └───────────────────────┘   │          │
# MAGIC                                      │          │
# MAGIC               ┌──────────────────────▼──┐       │
# MAGIC               │     dim_products        │       │
# MAGIC               ├─────────────────────────┤       │
# MAGIC               │ • product_key (PK)      │       │
# MAGIC               │ • product_id            │       │
# MAGIC               │ • product_number        │       │
# MAGIC               │ • product_name          │       │
# MAGIC               │ • category              │       │
# MAGIC               │ • subcategory           │       │
# MAGIC               │ • product_cost          │       │
# MAGIC               │ • product_line          │       │
# MAGIC               └─────────────────────────┘       │
# MAGIC                                                 │
# MAGIC                                                 │
# MAGIC ```
# MAGIC
# MAGIC **Legend:**
# MAGIC * PK = Primary Key
# MAGIC * FK = Foreign Key
# MAGIC * 1:N = One-to-Many Relationship
# MAGIC * N:1 = Many-to-One Relationship

# COMMAND ----------

# DBTITLE 1,Setup - Configuration
# Configuration
CATALOG = "bigdata_catalog"
GOLD_DB = "gold"

print(f"📊 Gold Layer Documentation")
print(f"Catalog: {CATALOG}")
print(f"Schema: {GOLD_DB}")
print(f"="*60)

# COMMAND ----------

# DBTITLE 1,Fact Table Documentation
# MAGIC %md
# MAGIC ## Fact Table: fact_sales
# MAGIC
# MAGIC The central fact table contains **transactional sales data** with foreign keys to dimension tables and numerical measures for analysis.

# COMMAND ----------

# DBTITLE 1,Fact Table - Structure
# MAGIC %sql
# MAGIC -- Fact table structure
# MAGIC DESCRIBE bigdata_catalog.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,Fact Table - Sample Records
# MAGIC %sql
# MAGIC -- Sample records from fact table
# MAGIC SELECT 
# MAGIC     sales_key,
# MAGIC     customer_key,
# MAGIC     product_key,
# MAGIC     order_date_key,
# MAGIC     order_number,
# MAGIC     sales_amount,
# MAGIC     quantity,
# MAGIC     unit_price
# MAGIC FROM bigdata_catalog.gold.fact_sales
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Fact Table - Key Metrics
# MAGIC %sql
# MAGIC -- Fact table metrics
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(DISTINCT order_number) as unique_orders,
# MAGIC     MIN(order_date_key) as earliest_date_key,
# MAGIC     MAX(order_date_key) as latest_date_key,
# MAGIC     ROUND(SUM(sales_amount), 2) as total_revenue,
# MAGIC     SUM(quantity) as total_units_sold,
# MAGIC     ROUND(AVG(sales_amount), 2) as avg_order_value,
# MAGIC     ROUND(AVG(unit_price), 2) as avg_unit_price
# MAGIC FROM bigdata_catalog.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,Dimension Tables Documentation
# MAGIC %md
# MAGIC ## Dimension Tables
# MAGIC
# MAGIC Dimension tables provide **descriptive context** for the facts. Each dimension contains business attributes used for filtering, grouping, and reporting.

# COMMAND ----------

# DBTITLE 1,dim_customers Overview
# MAGIC %md
# MAGIC ### Dimension: dim_customers
# MAGIC
# MAGIC Contains **customer master data** including demographics and location information.

# COMMAND ----------

# DBTITLE 1,dim_customers - Structure
# MAGIC %sql
# MAGIC -- Customer dimension structure
# MAGIC DESCRIBE bigdata_catalog.gold.dim_customers;

# COMMAND ----------

# DBTITLE 1,dim_customers - Sample Records
# MAGIC %sql
# MAGIC -- Sample customer records
# MAGIC SELECT 
# MAGIC     customer_key,
# MAGIC     customer_id,
# MAGIC     customer_number,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     gender,
# MAGIC     country,
# MAGIC     marital_status,
# MAGIC     birth_date
# MAGIC FROM bigdata_catalog.gold.dim_customers
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,dim_customers - Distribution Analysis
# MAGIC %sql
# MAGIC -- Customer distribution by country and gender
# MAGIC SELECT 
# MAGIC     country,
# MAGIC     gender,
# MAGIC     COUNT(*) as customer_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
# MAGIC FROM bigdata_catalog.gold.dim_customers
# MAGIC WHERE country != 'Unknown'
# MAGIC GROUP BY country, gender
# MAGIC ORDER BY country, customer_count DESC;

# COMMAND ----------

# DBTITLE 1,dim_products Overview
# MAGIC %md
# MAGIC ### Dimension: dim_products
# MAGIC
# MAGIC Contains **product catalog data** with hierarchical categorization and pricing information.

# COMMAND ----------

# DBTITLE 1,dim_products - Structure
# MAGIC %sql
# MAGIC -- Product dimension structure
# MAGIC DESCRIBE bigdata_catalog.gold.dim_products;

# COMMAND ----------

# DBTITLE 1,dim_products - Sample Records
# MAGIC %sql
# MAGIC -- Sample product records
# MAGIC SELECT 
# MAGIC     product_key,
# MAGIC     product_id,
# MAGIC     product_number,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     subcategory,
# MAGIC     product_cost,
# MAGIC     product_line
# MAGIC FROM bigdata_catalog.gold.dim_products
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,dim_products - Category Breakdown
# MAGIC %sql
# MAGIC -- Product breakdown by category and subcategory
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     subcategory,
# MAGIC     COUNT(*) as product_count,
# MAGIC     ROUND(AVG(product_cost), 2) as avg_cost
# MAGIC FROM bigdata_catalog.gold.dim_products
# MAGIC WHERE category IS NOT NULL
# MAGIC GROUP BY category, subcategory
# MAGIC ORDER BY category, subcategory;

# COMMAND ----------

# DBTITLE 1,dim_date Overview
# MAGIC %md
# MAGIC ### Dimension: dim_date
# MAGIC
# MAGIC Contains **date dimension** with calendar attributes for time-based analysis and reporting.

# COMMAND ----------

# DBTITLE 1,dim_date - Structure
# MAGIC %sql
# MAGIC -- Date dimension structure
# MAGIC DESCRIBE bigdata_catalog.gold.dim_date;

# COMMAND ----------

# DBTITLE 1,dim_date - Sample Records
# MAGIC %sql
# MAGIC -- Sample date records
# MAGIC SELECT 
# MAGIC     date_key,
# MAGIC     date as full_date,
# MAGIC     year,
# MAGIC     quarter,
# MAGIC     month,
# MAGIC     month_name,
# MAGIC     day as day_of_month,
# MAGIC     day_of_week,
# MAGIC     day_name,
# MAGIC     is_weekend
# MAGIC FROM bigdata_catalog.gold.dim_date
# MAGIC ORDER BY date_key
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,dim_date - Coverage Summary
# MAGIC %sql
# MAGIC -- Date dimension coverage
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_dates,
# MAGIC     MIN(date) as earliest_date,
# MAGIC     MAX(date) as latest_date,
# MAGIC     COUNT(DISTINCT year) as years_covered,
# MAGIC     COUNT(DISTINCT CONCAT(year, '-', month)) as months_covered
# MAGIC FROM bigdata_catalog.gold.dim_date;

# COMMAND ----------

# DBTITLE 1,Data Quality Validation
# MAGIC %md
# MAGIC ## Data Quality Validation
# MAGIC
# MAGIC Validation checks ensure data integrity across the star schema.

# COMMAND ----------

# DBTITLE 1,Foreign Key Integrity Checks
# MAGIC %sql
# MAGIC -- Foreign key integrity validation
# MAGIC SELECT 
# MAGIC     'customer_key' as foreign_key,
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(customer_key) as non_null_keys,
# MAGIC     COUNT(*) - COUNT(customer_key) as null_keys
# MAGIC FROM bigdata_catalog.gold.fact_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'product_key' as foreign_key,
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(product_key) as non_null_keys,
# MAGIC     COUNT(*) - COUNT(product_key) as null_keys
# MAGIC FROM bigdata_catalog.gold.fact_sales
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'order_date_key' as foreign_key,
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(order_date_key) as non_null_keys,
# MAGIC     COUNT(*) - COUNT(order_date_key) as null_keys
# MAGIC FROM bigdata_catalog.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,Orphan Records Check
# MAGIC %sql
# MAGIC -- Check for orphan records (FKs with no matching dimension records)
# MAGIC SELECT 
# MAGIC     'Customer orphans' as check_type,
# MAGIC     COUNT(DISTINCT f.customer_key) as orphan_count
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC LEFT JOIN bigdata_catalog.gold.dim_customers c ON f.customer_key = c.customer_key
# MAGIC WHERE c.customer_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Product orphans' as check_type,
# MAGIC     COUNT(DISTINCT f.product_key) as orphan_count
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC LEFT JOIN bigdata_catalog.gold.dim_products p ON f.product_key = p.product_key
# MAGIC WHERE p.product_key IS NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Date orphans' as check_type,
# MAGIC     COUNT(DISTINCT f.order_date_key) as orphan_count
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC LEFT JOIN bigdata_catalog.gold.dim_date d ON f.order_date_key = d.date_key
# MAGIC WHERE d.date_key IS NULL;

# COMMAND ----------

# DBTITLE 1,Measure Completeness Check
# MAGIC %sql
# MAGIC -- Measure completeness validation
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_records,
# MAGIC     COUNT(sales_amount) as sales_amount_populated,
# MAGIC     COUNT(*) - COUNT(sales_amount) as sales_amount_null,
# MAGIC     COUNT(quantity) as quantity_populated,
# MAGIC     COUNT(*) - COUNT(quantity) as quantity_null,
# MAGIC     COUNT(unit_price) as unit_price_populated,
# MAGIC     COUNT(*) - COUNT(unit_price) as unit_price_null
# MAGIC FROM bigdata_catalog.gold.fact_sales;

# COMMAND ----------

# DBTITLE 1,Business Metrics Summary
# MAGIC %md
# MAGIC ## Business Metrics Summary
# MAGIC
# MAGIC Key business metrics derived from the star schema for executive reporting.

# COMMAND ----------

# DBTITLE 1,Overall Business Metrics
# MAGIC %sql
# MAGIC -- Overall business performance metrics
# MAGIC SELECT 
# MAGIC     ROUND(SUM(f.sales_amount), 2) as total_revenue,
# MAGIC     COUNT(DISTINCT f.order_number) as total_orders,
# MAGIC     COUNT(*) as total_line_items,
# MAGIC     SUM(f.quantity) as total_units_sold,
# MAGIC     ROUND(AVG(f.sales_amount), 2) as avg_order_line_value,
# MAGIC     COUNT(DISTINCT f.customer_key) as unique_customers,
# MAGIC     COUNT(DISTINCT f.product_key) as unique_products_sold,
# MAGIC     ROUND(SUM(f.sales_amount) / COUNT(DISTINCT f.customer_key), 2) as revenue_per_customer
# MAGIC FROM bigdata_catalog.gold.fact_sales f;

# COMMAND ----------

# DBTITLE 1,Top Categories by Revenue
# MAGIC %sql
# MAGIC -- Top product categories by revenue
# MAGIC SELECT 
# MAGIC     p.category,
# MAGIC     p.subcategory,
# MAGIC     COUNT(DISTINCT f.order_number) as orders,
# MAGIC     SUM(f.quantity) as units_sold,
# MAGIC     ROUND(SUM(f.sales_amount), 2) as total_revenue,
# MAGIC     ROUND(AVG(f.sales_amount), 2) as avg_line_value
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC JOIN bigdata_catalog.gold.dim_products p ON f.product_key = p.product_key
# MAGIC WHERE p.category IS NOT NULL AND p.category != 'Other'
# MAGIC GROUP BY p.category, p.subcategory
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Top Markets by Revenue
# MAGIC %sql
# MAGIC -- Top geographic markets by revenue
# MAGIC SELECT 
# MAGIC     c.country,
# MAGIC     COUNT(DISTINCT f.customer_key) as customers,
# MAGIC     COUNT(DISTINCT f.order_number) as orders,
# MAGIC     SUM(f.quantity) as units_sold,
# MAGIC     ROUND(SUM(f.sales_amount), 2) as total_revenue,
# MAGIC     ROUND(AVG(f.sales_amount), 2) as avg_line_value
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC JOIN bigdata_catalog.gold.dim_customers c ON f.customer_key = c.customer_key
# MAGIC WHERE c.country != 'Unknown'
# MAGIC GROUP BY c.country
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Peak Sales Period
# MAGIC %sql
# MAGIC -- Peak sales period analysis
# MAGIC SELECT 
# MAGIC     d.year,
# MAGIC     d.month_name,
# MAGIC     COUNT(DISTINCT f.order_number) as orders,
# MAGIC     SUM(f.quantity) as units_sold,
# MAGIC     ROUND(SUM(f.sales_amount), 2) as revenue
# MAGIC FROM bigdata_catalog.gold.fact_sales f
# MAGIC JOIN bigdata_catalog.gold.dim_date d ON f.order_date_key = d.date_key
# MAGIC GROUP BY d.year, d.month, d.month_name
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 5;