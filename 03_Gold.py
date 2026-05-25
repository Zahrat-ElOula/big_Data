# Databricks notebook source
# MAGIC %md
# MAGIC # NOTEBOOK 3 — GOLD LAYER: Star Schema (Analytics-Ready)
# MAGIC Purpose : Join and model the cleaned Silver tables into a dimensional model
# MAGIC            (star schema) stored in the 'gold' schema as Delta Tables.
# MAGIC
# MAGIC -Output tables:
# MAGIC    - gold.dim_customers  — one row per customer, enriched from CRM + ERP
# MAGIC    - gold.dim_products   — one row per product, enriched from CRM + ERP category
# MAGIC    - gold.dim_date       — date dimension generated from sales order dates
# MAGIC    - gold.fact_sales     — grain: one row per sales order line
# MAGIC
# MAGIC -Business KPIs enabled:
# MAGIC    - Total revenue by product category and subcategory
# MAGIC    - Sales volume (quantity) by product line
# MAGIC    - Revenue and order count by customer country
# MAGIC    - Customer distribution by gender and marital status
# MAGIC    - Sales trend over time (by order date)
# MAGIC    - Top-performing products by total revenue

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG   = "bigdata_catalog"     
SILVER_DB = "silver"
GOLD_DB   = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create gold schema

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
spark.sql(f"USE DATABASE {GOLD_DB}")
print(f"Using catalog: {CATALOG}, database: {GOLD_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load silver tables

# COMMAND ----------

crm_customers = spark.table(f"{CATALOG}.{SILVER_DB}.crm_customers")
crm_products  = spark.table(f"{CATALOG}.{SILVER_DB}.crm_products")
crm_sales     = spark.table(f"{CATALOG}.{SILVER_DB}.crm_sales")
erp_demo      = spark.table(f"{CATALOG}.{SILVER_DB}.erp_customer_demographics")
erp_loc       = spark.table(f"{CATALOG}.{SILVER_DB}.erp_customer_location")
erp_cat       = spark.table(f"{CATALOG}.{SILVER_DB}.erp_product_category")
 
print("Silver tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dim_customers
# MAGIC  -Join logic:
# MAGIC    - crm_customers.cst_key  <-> erp_demo.CID   (both now in "AW00011000" format)
# MAGIC    - crm_customers.cst_key  <-> erp_loc.CID    (both now in "AW00011000" format)
# MAGIC
# MAGIC  -Conflict resolution:
# MAGIC    - Gender: CRM is authoritative; ERP fills in gaps (Unknown from CRM -> ERP value)
# MAGIC    - Country comes only from ERP location (not in CRM)
# MAGIC    - Birthdate comes only from ERP demographics
# MAGIC  

# COMMAND ----------

print("\n=== Building dim_customers ===")
 
dim_customers = (
    crm_customers
    # Join ERP demographics on customer key
    .join(erp_demo.select(
            F.col("CID").alias("erp_cid"),
            F.col("BDATE").alias("birth_date"),
            F.col("GEN").alias("erp_gender")),
          crm_customers.cst_key == F.col("erp_cid"),
          how="left")
    # Join ERP location on customer key
    .join(erp_loc.select(
            F.col("CID").alias("loc_cid"),
            F.col("CNTRY").alias("country")),
          crm_customers.cst_key == F.col("loc_cid"),
          how="left")
    # Resolve gender conflict: use CRM value; if Unknown, fall back to ERP
    .withColumn("gender",
        F.when(F.col("cst_gndr") != "Unknown", F.col("cst_gndr"))
         .otherwise(F.col("erp_gender")))
    # Fill null country with Unknown
    .withColumn("country",
        F.coalesce(F.col("country"), F.lit("Unknown")))
)

# Generate stable surrogate keys using row_number
window_spec = Window.orderBy("cst_id")
dim_customers = dim_customers.withColumn("customer_key", F.row_number().over(window_spec))

# Select final columns
dim_customers = dim_customers.select(
    F.col("customer_key"),
    F.col("cst_id").alias("customer_id"),
    F.col("cst_key").alias("customer_number"),
    F.col("cst_firstname").alias("first_name"),
    F.col("cst_lastname").alias("last_name"),
    F.col("cst_marital_status").alias("marital_status"),
    F.col("gender"),
    F.col("birth_date"),
    F.col("country"),
    F.col("cst_create_date").alias("create_date")
)
 
dim_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_DB}.dim_customers")
 
print(f"  Rows: {dim_customers.count()}")
dim_customers.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dim_products
# MAGIC
# MAGIC  -Join logic:
# MAGIC    - crm_products.cat_id  <->  erp_cat.cat_id   (cat_id was extracted in Silver from the first two segments of prd_key)
# MAGIC
# MAGIC  - Enrichment:
# MAGIC    - CRM provides: product name, cost, product line, start date
# MAGIC    - ERP provides: category name, subcategory name, maintenance flag
# MAGIC  

# COMMAND ----------

print("\n=== Building dim_products ===")
print("Note: Products table and sales table have no common keys.")
print("      Building dimension from both sources...\n")

# Get products from CRM products table
crm_prods = (
    crm_products
    .join(erp_cat,
          crm_products.cat_id == erp_cat.cat_id,
          how="left")
    .select(
        F.col("prd_id").cast("string").alias("product_id"),
        F.col("prd_key").alias("product_number"),
        F.col("prd_nm").alias("product_name"),
        F.col("prd_cost").alias("product_cost"),
        F.col("prd_line").alias("product_line"),
        F.col("cat_name").alias("category"),
        F.col("cat_subname").alias("subcategory"),
        F.col("cat_maintenance").alias("maintenance"),
        F.col("prd_start_dt").alias("start_date")
    )
)

# Get distinct products from sales and enrich based on naming patterns
sales_prods = (
    crm_sales
    .select(F.col("sls_prd_key").alias("product_number"))
    .distinct()
    .withColumn("product_id", F.concat(F.lit("SALES_"), F.col("product_number")))
    .withColumn("product_name", F.col("product_number"))  # Use product key as name
    .withColumn("product_cost", F.lit(None).cast("int"))
    # Derive product line from prefix patterns
    .withColumn("product_line",
        F.when(F.col("product_number").startswith("BK-M"), "Mountain")
         .when(F.col("product_number").startswith("BK-R"), "Road")
         .when(F.col("product_number").startswith("BK-T"), "Touring")
         .otherwise("Other Sales"))
    # Derive category from main prefix
    .withColumn("category",
        F.when(F.col("product_number").startswith("BK-"), "Bikes")
         .when(F.col("product_number").startswith("CL-"), "Clothing")
         .when(F.col("product_number").startswith("AC-"), "Accessories")
         .when(F.col("product_number").startswith("CO-"), "Components")
         .otherwise("Other"))
    # Derive subcategory from product line and category
    .withColumn("subcategory",
        F.when(F.col("category") == "Bikes",
            F.concat(
                F.when(F.col("product_number").startswith("BK-M"), F.lit("Mountain "))
                 .when(F.col("product_number").startswith("BK-R"), F.lit("Road "))
                 .when(F.col("product_number").startswith("BK-T"), F.lit("Touring "))
                 .otherwise(F.lit("")),
                F.lit("Bikes")
            )
        )
        .otherwise("Other"))
    .withColumn("maintenance", F.lit("Unknown"))
    .withColumn("start_date", F.lit(None).cast("date"))
    .select("product_id", "product_number", "product_name", "product_cost", "product_line", "category", "subcategory", "maintenance", "start_date")
)

# Union both sources
all_products = crm_prods.unionByName(sales_prods)

# Generate stable surrogate keys
window_spec = Window.orderBy("product_number")
dim_products = all_products.withColumn("product_key", F.row_number().over(window_spec))

# Reorder columns
dim_products = dim_products.select(
    "product_key", "product_id", "product_number", "product_name",
    "product_cost", "product_line", "category", "subcategory",
    "maintenance", "start_date"
)
 
dim_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_DB}.dim_products")
 
print(f"  Total rows: {dim_products.count()}")
print(f"  CRM products: {crm_prods.count()}")
print(f"  Sales-only products: {sales_prods.count()}")
print("\nSample of enriched sales products:")
dim_products.filter(F.col("product_id").startswith("SALES_")).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build dim_date
# MAGIC
# MAGIC - Generated from all distinct dates present in the sales fact:
# MAGIC - sls_order_dt, sls_ship_dt, sls_due_dt
# MAGIC - This ensures the date dimension covers every date referenced in the fact table.

# COMMAND ----------

print("\n=== Building dim_date ===")
 
# Collect all dates from sales
all_dates = (
    crm_sales
    .select(F.col("sls_order_dt").alias("dt"))
    .union(crm_sales.select(F.col("sls_ship_dt").alias("dt")))
    .union(crm_sales.select(F.col("sls_due_dt").alias("dt")))
    .filter(F.col("dt").isNotNull())
    .distinct()
)
 
dim_date = (
    all_dates
    .withColumn("date_key",    F.date_format(F.col("dt"), "yyyyMMdd").cast("int"))
    .withColumn("date",        F.col("dt"))
    .withColumn("year",        F.year(F.col("dt")))
    .withColumn("quarter",     F.quarter(F.col("dt")))
    .withColumn("month",       F.month(F.col("dt")))
    .withColumn("month_name",  F.date_format(F.col("dt"), "MMMM"))
    .withColumn("week",        F.weekofyear(F.col("dt")))
    .withColumn("day",         F.dayofmonth(F.col("dt")))
    .withColumn("day_of_week", F.dayofweek(F.col("dt")))
    .withColumn("day_name",    F.date_format(F.col("dt"), "EEEE"))
    .withColumn("is_weekend",
        F.when(F.dayofweek(F.col("dt")).isin(1, 7), True).otherwise(False))
    .drop("dt")
    .orderBy("date_key")
)
 
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_DB}.dim_date")
 
print(f"  Rows: {dim_date.count()}")
dim_date.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_sales
# MAGIC
# MAGIC -  Grain: one row per sales order line (sls_ord_num + sls_prd_key)
# MAGIC
# MAGIC -  Foreign keys:
# MAGIC    - customer_key  -> dim_customers.customer_key   (via cst_id)
# MAGIC    - product_key   -> dim_products.product_key     (via prd_key)
# MAGIC    - order_date_key -> dim_date.date_key           (via sls_order_dt)
# MAGIC    - ship_date_key  -> dim_date.date_key
# MAGIC    - due_date_key   -> dim_date.date_key
# MAGIC
# MAGIC -  Measures: sls_sales (revenue), sls_quantity, sls_price

# COMMAND ----------

print("\n=== Building fact_sales ===")
 
# Load gold dims to get surrogate keys
gold_customers = spark.table(f"{CATALOG}.{GOLD_DB}.dim_customers") \
    .select("customer_key", "customer_id")
gold_products  = spark.table(f"{CATALOG}.{GOLD_DB}.dim_products") \
    .select("product_key", "product_number")
gold_date      = spark.table(f"{CATALOG}.{GOLD_DB}.dim_date") \
    .select("date_key", "date")
 
fact_sales = (
    crm_sales
    # Join customer surrogate key
    .join(gold_customers,
          crm_sales.sls_cust_id == gold_customers.customer_id,
          how="left")
    # Join product surrogate key directly on product_number
    .join(gold_products,
          crm_sales.sls_prd_key == gold_products.product_number,
          how="left")
    # Join order date key
    .join(gold_date.select(
            F.col("date_key").alias("order_date_key"),
            F.col("date").alias("_ord_date")),
          crm_sales.sls_order_dt == F.col("_ord_date"),
          how="left")
    # Join ship date key
    .join(gold_date.select(
            F.col("date_key").alias("ship_date_key"),
            F.col("date").alias("_ship_date")),
          crm_sales.sls_ship_dt == F.col("_ship_date"),
          how="left")
    # Join due date key
    .join(gold_date.select(
            F.col("date_key").alias("due_date_key"),
            F.col("date").alias("_due_date")),
          crm_sales.sls_due_dt == F.col("_due_date"),
          how="left")
)

# Generate stable surrogate keys for fact table
window_spec = Window.orderBy("sls_ord_num", "sls_prd_key")
fact_sales = fact_sales.withColumn("sales_key", F.row_number().over(window_spec))

# Select final columns
fact_sales = fact_sales.select(
    F.col("sales_key"),
    F.col("sls_ord_num").alias("order_number"),
    F.col("customer_key"),
    F.col("product_key"),
    F.col("order_date_key"),
    F.col("ship_date_key"),
    F.col("due_date_key"),
    F.col("sls_sales").alias("sales_amount"),
    F.col("sls_quantity").alias("quantity"),
    F.col("sls_price").alias("unit_price")
)
 
fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_DB}.fact_sales")
 
print(f"  Rows: {fact_sales.count()}")
fact_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify gold tables

# COMMAND ----------

print("\n=== Gold tables summary ===")
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{GOLD_DB}").collect()
for t in tables:
    count = spark.table(f"{CATALOG}.{GOLD_DB}.{t.tableName}").count()
    print(f"  {t.tableName:<25} {count:>7} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business KPI validation queries
# MAGIC

# COMMAND ----------

print("\n=== KPI 1: Total revenue by product category ===")
display(spark.sql(f"""
    SELECT p.category, p.subcategory,
           ROUND(SUM(f.sales_amount), 2) AS total_revenue,
           SUM(f.quantity)               AS total_quantity
    FROM   {CATALOG}.{GOLD_DB}.fact_sales  f
    JOIN   {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    WHERE  p.category IS NOT NULL
    GROUP  BY p.category, p.subcategory
    ORDER  BY total_revenue DESC
    LIMIT  10
"""))
 
print("\n=== KPI 2: Revenue and orders by customer country ===")
display(spark.sql(f"""
    SELECT c.country,
           ROUND(SUM(f.sales_amount), 2) AS total_revenue,
           COUNT(DISTINCT f.order_number) AS total_orders
    FROM   {CATALOG}.{GOLD_DB}.fact_sales    f
    JOIN   {CATALOG}.{GOLD_DB}.dim_customers c ON f.customer_key = c.customer_key
    GROUP  BY c.country
    ORDER  BY total_revenue DESC
"""))
 
print("\n=== KPI 3: Customer distribution by gender and marital status ===")
display(spark.sql(f"""
    SELECT gender, marital_status, COUNT(*) AS customer_count
    FROM   {CATALOG}.{GOLD_DB}.dim_customers
    GROUP  BY gender, marital_status
    ORDER  BY customer_count DESC
"""))
 
print("\n=== KPI 4: Sales trend by year and month ===")
display(spark.sql(f"""
    SELECT d.year, d.month_name, d.month,
           ROUND(SUM(f.sales_amount), 2) AS monthly_revenue
    FROM   {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN   {CATALOG}.{GOLD_DB}.dim_date   d ON f.order_date_key = d.date_key
    GROUP  BY d.year, d.month, d.month_name
    ORDER  BY d.year, d.month
    LIMIT  24
"""))
 
print("\n=== KPI 5: Top 10 products by total revenue ===")
display(spark.sql(f"""
    SELECT p.product_name, p.category, p.product_line,
           ROUND(SUM(f.sales_amount), 2) AS total_revenue
    FROM   {CATALOG}.{GOLD_DB}.fact_sales  f
    JOIN   {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    GROUP  BY p.product_name, p.category, p.product_line
    ORDER  BY total_revenue DESC
    LIMIT  10
"""))
 
print("\n=== KPI 6: Sales volume by product line ===")
display(spark.sql(f"""
    SELECT p.product_line,
           SUM(f.quantity)               AS total_units_sold,
           ROUND(SUM(f.sales_amount), 2) AS total_revenue
    FROM   {CATALOG}.{GOLD_DB}.fact_sales  f
    JOIN   {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    GROUP  BY p.product_line
    ORDER  BY total_revenue DESC
"""))
 
print("\nGold layer complete. All KPIs validated.")

# COMMAND ----------

# DBTITLE 1,Visualize Sales Trend Over Time
# Line chart: Monthly revenue trend
from pyspark.sql import functions as F

print("Monthly Sales Trend:")
monthly_trend = spark.sql(f"""
    SELECT 
        d.year,
        d.month_name as month,
        ROUND(SUM(f.sales_amount), 2) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""")
display(monthly_trend)

# COMMAND ----------

# DBTITLE 1,Visualize Product Category Performance
# Horizontal bar chart: Revenue by category and subcategory
from pyspark.sql import functions as F

print("Revenue by Product Category:")
category_revenue = spark.sql(f"""
    SELECT 
        CONCAT(p.category, ' - ', p.subcategory) as category_subcategory,
        ROUND(SUM(f.sales_amount), 2) as total_revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    WHERE p.category IS NOT NULL AND p.category != 'Other'
    GROUP BY p.category, p.subcategory
    ORDER BY total_revenue DESC
""")
display(category_revenue)

# COMMAND ----------

# DBTITLE 1,Visualize Customer Geographic Distribution
# Donut/Pie chart: Revenue percentage by country
from pyspark.sql import functions as F

print("Revenue Distribution by Country:")
country_revenue = spark.sql(f"""
    SELECT 
        c.country,
        ROUND(SUM(f.sales_amount), 2) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_customers c ON f.customer_key = c.customer_key
    WHERE c.country != 'Unknown'
    GROUP BY c.country
    ORDER BY revenue DESC
""")
display(country_revenue)

# COMMAND ----------

# DBTITLE 1,Visualize Top Products
# Horizontal bar chart: Top 20 products by revenue
from pyspark.sql import functions as F

print("Top 20 Products by Revenue:")
top_products = spark.sql(f"""
    SELECT 
        p.product_name,
        ROUND(SUM(f.sales_amount), 2) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    GROUP BY p.product_name
    ORDER BY revenue DESC
    LIMIT 20
""")
display(top_products)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC