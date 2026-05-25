# Databricks notebook source
# MAGIC %md
# MAGIC # NOTEBOOK 2 — SILVER LAYER: Cleaning & Standardization
# MAGIC -  Purpose : Read the raw Bronze Delta Tables, apply data quality rules, and write cleaned, standardized data to the 'silver' schema.
# MAGIC
# MAGIC - Transformations applied:
# MAGIC     -     crm_customers        -> Fix whitespace in names, decode M/F/S codes
# MAGIC     -     crm_products         -> Extract category key, trim prd_line, handle nulls ,Keep only latest version per product (no end_date)
# MAGIC     -     crm_sales            -> Convert integer dates, fix sls_sales/price nulls
# MAGIC     -     erp_customer_demographics -> Strip 'NAS' prefix from CID, normalize gender,  Drop future birthdates (data entry errors)
# MAGIC     -     erp_customer_location     -> Normalize CID format, standardize country names
# MAGIC     -     erp_product_category      -> Already clean, pass through with rename

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Configuration

# COMMAND ----------

CATALOG         = "bigdata_catalog"    
BRONZE_DB       = "bronze"
SILVER_DB       = "silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create silver schema

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
spark.sql(f"USE DATABASE {SILVER_DB}")
print(f"Using catalog: {CATALOG}, database: {SILVER_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean: crm_customers
# MAGIC
# MAGIC -  Issues found in raw data:
# MAGIC    - Leading/trailing whitespace in cst_firstname and cst_lastname
# MAGIC    - cst_marital_status encoded as 'M'=Married, 'S'=Single
# MAGIC    - cst_gndr encoded as 'M'=Male, 'F'=Female
# MAGIC    - Duplicate cst_id rows possible (keep most recent by cst_create_date)
# MAGIC  

# COMMAND ----------

print("=== Cleaning crm_customers ===")
 
raw_customers = spark.table(f"{CATALOG}.{BRONZE_DB}.crm_customers")
 
# Deduplicate: keep row with latest create_date per customer ID
window_cust = Window.partitionBy("cst_id").orderBy(F.col("cst_create_date").desc())
 
silver_customers = (
    raw_customers
    # Trim whitespace from name fields
    .withColumn("cst_firstname",     F.trim(F.col("cst_firstname")))
    .withColumn("cst_lastname",      F.trim(F.col("cst_lastname")))
    # Decode marital status
    .withColumn("cst_marital_status",
        F.when(F.col("cst_marital_status") == "M", "Married")
         .when(F.col("cst_marital_status") == "S", "Single")
         .otherwise("Unknown"))
    # Decode gender
    .withColumn("cst_gndr",
        F.when(F.col("cst_gndr") == "M", "Male")
         .when(F.col("cst_gndr") == "F", "Female")
         .otherwise("Unknown"))
    # Parse create_date to proper date type
    .withColumn("cst_create_date", F.to_date(F.col("cst_create_date")))
    # Rank rows per customer, keep only the latest
    .withColumn("_row_rank", F.row_number().over(window_cust))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank", "_ingestion_ts")
)
 
silver_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.crm_customers")
 
print(f"  Rows: {silver_customers.count()}")
silver_customers.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %md
# MAGIC ## Clean: crm_products
# MAGIC
# MAGIC -  Issues found:
# MAGIC    - cat_id extracted in bronze layer from prd_key, format "AC-HE" needs conversion to "AC_HE" to match ERP
# MAGIC    - prd_line has trailing spaces ('R ', 'S ', 'M ', 'T ')
# MAGIC    - prd_line values: M=Mountain, R=Road, S=Other Sales, T=Touring
# MAGIC    - prd_cost has nulls -> fill with 0
# MAGIC    - prd_end_dt is empty for current products (this is the active record)
# MAGIC    - Duplicate products (same prd_key, different dates) -> keep active record
# MAGIC      (prd_end_dt is null) or most recently started one

# COMMAND ----------

# DBTITLE 1,Cell 11
print("\n=== Cleaning crm_products ===")
 
raw_products = spark.table(f"{CATALOG}.{BRONZE_DB}.crm_products")
 
# Keep only the currently active product per key (no end date = active)
# If multiple with no end date, keep the most recent start date
window_prod = Window.partitionBy("prd_key").orderBy(F.col("prd_start_dt").desc())
 
silver_products = (
    raw_products
    # Use cat_id from bronze and convert dash to underscore to match ERP format
    # "AC-HE" -> "AC_HE"
    .withColumn("cat_id", F.regexp_replace(F.col("cat_id"), "-", "_"))
    # Trim trailing spaces from prd_line and decode
    .withColumn("prd_line", F.trim(F.col("prd_line")))
    .withColumn("prd_line",
        F.when(F.col("prd_line") == "M", "Mountain")
         .when(F.col("prd_line") == "R", "Road")
         .when(F.col("prd_line") == "S", "Other Sales")
         .when(F.col("prd_line") == "T", "Touring")
         .otherwise("Unknown"))
    # Fill null cost with 0
    .withColumn("prd_cost", F.coalesce(F.col("prd_cost"), F.lit(0)))
    # Parse date columns
    .withColumn("prd_start_dt", F.to_date(F.col("prd_start_dt")))
    .withColumn("prd_end_dt",   F.to_date(F.col("prd_end_dt")))
    # Keep only active (current) products: prd_end_dt is null
    .filter(F.col("prd_end_dt").isNull())
    # Among those, keep most recent start date per key
    .withColumn("_row_rank", F.row_number().over(window_prod))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank", "_ingestion_ts")
)
 
silver_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.crm_products")
 
print(f"  Rows: {silver_products.count()}")
silver_products.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Clean: crm_sales
# MAGIC
# MAGIC -  Issues found:
# MAGIC    - sls_order_dt, sls_ship_dt, sls_due_dt stored as integers (20101229)   - sls_sales has nulls and some 0/negative values
# MAGIC    - sls_price has nulls
# MAGIC    - Business rule: sls_sales = sls_quantity * sls_price
# MAGIC      If sls_sales is null/invalid -> derive from quantity * price
# MAGIC      If sls_price is null/invalid -> derive from sales / quantity

# COMMAND ----------

print("\n=== Cleaning crm_sales ===")
 
raw_sales = spark.table(f"{CATALOG}.{BRONZE_DB}.crm_sales")
 
def int_to_date(col_name):
    """Convert integer date 20101229 -> date 2010-12-29"""
    return F.try_to_date(F.col(col_name).cast("string"), "yyyyMMdd")
 
silver_sales = (
    raw_sales
    # Convert integer dates to proper date columns
    .withColumn("sls_order_dt", int_to_date("sls_order_dt"))
    .withColumn("sls_ship_dt",  int_to_date("sls_ship_dt"))
    .withColumn("sls_due_dt",   int_to_date("sls_due_dt"))
    # Fix sls_sales: if null or <= 0, derive from quantity * price
    .withColumn("sls_sales",
        F.when(
            F.col("sls_sales").isNull() | (F.col("sls_sales") <= 0),
            F.col("sls_quantity") * F.col("sls_price")
        ).otherwise(F.col("sls_sales")))
    # Fix sls_price: if null or <= 0, derive from sales / quantity
    .withColumn("sls_price",
        F.when(
            F.col("sls_price").isNull() | (F.col("sls_price") <= 0),
            F.col("sls_sales") / F.col("sls_quantity")
        ).otherwise(F.col("sls_price")))
    # Drop rows where order date couldn't be parsed (data too corrupt)
    .filter(F.col("sls_order_dt").isNotNull())
    .drop("_ingestion_ts")
)
 
silver_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.crm_sales")
 
print(f"  Rows: {silver_sales.count()}")
silver_sales.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean: erp_customer_demographics
# MAGIC
# MAGIC -  Issues found:
# MAGIC    - CID prefixed with 'NAS': "NASAW00011000" -> strip to "AW00011000"
# MAGIC    - GEN has mixed values: 'Male','M ','M','Female','F ','F','  ',null
# MAGIC    - BDATE has some future dates (data entry errors) -> set to null

# COMMAND ----------

print("\n=== Cleaning erp_customer_demographics ===")
 
raw_demo = spark.table(f"{CATALOG}.{BRONZE_DB}.erp_customer_demographics")
from pyspark.sql.functions import current_date
 
silver_demo = (
    raw_demo
    # Strip 'NAS' prefix from CID to align with CRM key format
    .withColumn("CID",
        F.when(F.col("CID").startswith("NAS"),
               F.regexp_replace(F.col("CID"), "^NAS", ""))
         .otherwise(F.col("CID")))
    # Parse birthdate
    .withColumn("BDATE", F.to_date(F.col("BDATE")))
    # Nullify future birthdates (data entry errors)
    .withColumn("BDATE",
        F.when(F.col("BDATE") > current_date(), F.lit(None).cast("date"))
         .otherwise(F.col("BDATE")))
    # Normalize gender: trim, then map to consistent values
    .withColumn("GEN", F.trim(F.col("GEN")))
    .withColumn("GEN",
        F.when(F.col("GEN").isin("Male",   "M"), "Male")
         .when(F.col("GEN").isin("Female", "F"), "Female")
         .otherwise("Unknown"))
    .drop("_ingestion_ts")
)
 
silver_demo.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.erp_customer_demographics")
 
print(f"  Rows: {silver_demo.count()}")
silver_demo.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean: erp_customer_location
# MAGIC
# MAGIC - Issues found:
# MAGIC    - CID has dash format "AW-00011000" -> normalize to "AW00011000"
# MAGIC    - CNTRY has mixed formats: 'US','USA','United States','DE','Germany', etc.
# MAGIC    - Some CNTRY values are blank ('  ', ' ') -> set to 'Unknown'

# COMMAND ----------

print("\n=== Cleaning erp_customer_location ===")
 
raw_loc = spark.table(f"{CATALOG}.{BRONZE_DB}.erp_customer_location")
 
silver_loc = (
    raw_loc
    # Remove dashes from CID: "AW-00011000" -> "AW00011000"
    .withColumn("CID", F.regexp_replace(F.col("CID"), "-", ""))
    # Trim country and standardize
    .withColumn("CNTRY", F.trim(F.col("CNTRY")))
    .withColumn("CNTRY",
        F.when(F.col("CNTRY").isin("US", "USA", "United States"), "United States")
         .when(F.col("CNTRY").isin("DE", "Germany"),              "Germany")
         .when(F.col("CNTRY") == "United Kingdom",                "United Kingdom")
         .when(F.col("CNTRY") == "Australia",                     "Australia")
         .when(F.col("CNTRY") == "Canada",                        "Canada")
         .when(F.col("CNTRY") == "France",                        "France")
         .when(F.col("CNTRY").isNull() | (F.col("CNTRY") == ""), "Unknown")
         .otherwise(F.col("CNTRY")))
    .drop("_ingestion_ts")
)
 
silver_loc.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.erp_customer_location")
 
print(f"  Rows: {silver_loc.count()}")
silver_loc.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean: erp_product_category
# MAGIC
# MAGIC -This table is already clean. We pass it through with a rename for clarity.

# COMMAND ----------

print("\n=== Cleaning erp_product_category ===")
 
raw_cat = spark.table(f"{CATALOG}.{BRONZE_DB}.erp_product_category")
 
silver_cat = (
    raw_cat
    .withColumnRenamed("ID",          "cat_id")
    .withColumnRenamed("CAT",         "cat_name")
    .withColumnRenamed("SUBCAT",      "cat_subname")
    .withColumnRenamed("MAINTENANCE", "cat_maintenance")
    .drop("_ingestion_ts")
)
 
silver_cat.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_DB}.erp_product_category")
 
print(f"  Rows: {silver_cat.count()}")
silver_cat.show(5, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all silver tables

# COMMAND ----------

print("\n=== Silver tables summary ===")
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_DB}").collect()
for t in tables:
    count = spark.table(f"{CATALOG}.{SILVER_DB}.{t.tableName}").count()
    print(f"  {t.tableName:<40} {count:>7} rows")
 
print("\nSilver layer complete.")
 

# COMMAND ----------

# DBTITLE 1,Visualize Data Quality Improvements
# Visualize product line distribution after cleaning 
print("Product Line Distribution (Pie Chart):")
product_line_dist = spark.sql(f"""
    SELECT prd_line, COUNT(*) as count
    FROM {CATALOG}.{SILVER_DB}.crm_products
    GROUP BY prd_line
    ORDER BY count DESC
""")
display(product_line_dist, {"chart": "pie", "x": "prd_line", "y": "count"})

# COMMAND ----------

# DBTITLE 1,Visualize Customer Demographics
# Visualize customer gender distribution
from pyspark.sql import functions as F

print("Customer Gender Distribution:")
gender_dist = spark.sql(f"""
    SELECT cst_gndr as gender, COUNT(*) as count
    FROM {CATALOG}.{SILVER_DB}.crm_customers
    WHERE cst_gndr != 'Unknown'
    GROUP BY cst_gndr
    ORDER BY count DESC
""")
display(gender_dist)

# COMMAND ----------

# DBTITLE 1,Visualize Customer Location
# Visualize customer distribution by country
from pyspark.sql import functions as F

print("Customer Distribution by Country:")
country_dist = spark.sql(f"""
    SELECT CNTRY as country, COUNT(*) as customer_count
    FROM {CATALOG}.{SILVER_DB}.erp_customer_location
    WHERE CNTRY != 'Unknown'
    GROUP BY CNTRY
    ORDER BY customer_count DESC
""")
display(country_dist)

# COMMAND ----------

