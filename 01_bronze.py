# Databricks notebook source
# MAGIC %md
# MAGIC # NOTEBOOK 1 — BRONZE LAYER: Raw Ingestion
# MAGIC
# MAGIC - Purpose : Read all 6 source CSV files and write them as-is into Delta Tables under the 'bronze' schema. No transformations applied.
# MAGIC - Output  : 6 Delta Tables in catalog.bronze.*
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "bigdata_catalog"            
DATABASE = "bronze"
 
# Path where you uploaded the CSV files in DBFS
 
CRM_PATH   = "/Volumes/bigdata_catalog/bigdata_schema/bigdata_volume/CRM/"
ERP_PATH   = "/Volumes/bigdata_catalog/bigdata_schema/bigdata_volume/ERP/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the bronze schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE DATABASE {DATABASE}")
 
print(f"Using catalog: {CATALOG}, database: {DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper function: read CSV and write to Delta Table

# COMMAND ----------

# DBTITLE 1,Cell 7
def ingest_csv_to_bronze(file_path, table_name, delimiter=",", add_cat_id=False):
    """
    Reads a CSV file with header and writes it as a Delta Table in bronze schema.
    Adds a metadata column _ingestion_ts for traceability.
    Applies basic data cleansing: trim whitespace from string columns.
    For products table, optionally extracts cat_id from prd_key pattern.
    """
    from pyspark.sql.functions import current_timestamp, col, trim, substring_index
    from pyspark.sql.types import StringType

    print(f"Ingesting: {file_path}  -->  {CATALOG}.{DATABASE}.{table_name}")

    df = (spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("sep", delimiter)
          .load(file_path))

    # Trim whitespace from all string columns
    for field in df.schema.fields:
        if field.dataType == StringType():
            df = df.withColumn(field.name, trim(col(field.name)))

    # Add cat_id for products by extracting first 2 segments from prd_key
    if add_cat_id and "prd_key" in df.columns:
        df = df.withColumn("cat_id", substring_index(col("prd_key"), "-", 2))
        print("  Added cat_id extraction from prd_key")

    # Add ingestion timestamp for auditability
    df = df.withColumn("_ingestion_ts", current_timestamp())

    row_count = df.count()
    print(f"  Rows read : {row_count}")
    print(f"  Columns   : {df.columns}")

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.{DATABASE}.{table_name}"))

    print(f"  Saved to  : {CATALOG}.{DATABASE}.{table_name}\n")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CRM tables

# COMMAND ----------

# DBTITLE 1,Cell 9
ingest_csv_to_bronze(f"{CRM_PATH}/customers.csv",  "crm_customers")
ingest_csv_to_bronze(f"{CRM_PATH}/products.csv",   "crm_products", add_cat_id=True)
ingest_csv_to_bronze(f"{CRM_PATH}/sales.csv",      "crm_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest ERP tables

# COMMAND ----------

ingest_csv_to_bronze(f"{ERP_PATH}/CUSTOMER_DEMOGRAPHICS.csv", "erp_customer_demographics")
ingest_csv_to_bronze(f"{ERP_PATH}/CUSTOMER_LOCATION.csv",     "erp_customer_location")
ingest_csv_to_bronze(f"{ERP_PATH}/PRODUCT_CATEGORY.csv",      "erp_product_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify all tables were created

# COMMAND ----------

print("=== Bronze tables created ===")
tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{DATABASE}").collect()
for t in tables:
    count = spark.table(f"{CATALOG}.{DATABASE}.{t.tableName}").count()
    print(f"  {t.tableName:<35} {count:>7} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick preview of each table

# COMMAND ----------

for table in [
    "crm_customers", "crm_products", "crm_sales",
    "erp_customer_demographics", "erp_customer_location", "erp_product_category"
]:
    print(f"\n--- {table} ---")
    spark.table(f"{CATALOG}.{DATABASE}.{table}").show(3, truncate=False)
 

# COMMAND ----------

# DBTITLE 1,Data Quality Summary
# Data Quality Summary
from pyspark.sql import functions as F

print("\n" + "="*80)
print("DATA QUALITY SUMMARY")
print("="*80)

# Check for key data quality issues
quality_checks = []

# 1. CRM Products - cat_id now extracted from prd_key
products_df = spark.table(f"{CATALOG}.{DATABASE}.crm_products")
total_products = products_df.count()
products_with_cat = products_df.filter(F.col("cat_id").isNotNull()).count()

if products_with_cat == total_products:
    quality_checks.append({
        "Table": "crm_products",
        "Check": "cat_id extraction",
        "Status": "✅ PASS",
        "Details": f"All {total_products} products have cat_id extracted from prd_key"
    })
else:
    quality_checks.append({
        "Table": "crm_products",
        "Check": "cat_id extraction",
        "Status": "⚠️ WARNING",
        "Details": f"{products_with_cat}/{total_products} products have cat_id"
    })

# 2. Sales - check for invalid dates (value = 0)
sales_df = spark.table(f"{CATALOG}.{DATABASE}.crm_sales")
total_sales = sales_df.count()
invalid_dates = sales_df.filter(F.col("sls_order_dt") == 0).count()

if invalid_dates > 0:
    pct = (invalid_dates / total_sales * 100)
    quality_checks.append({
        "Table": "crm_sales",
        "Check": "Date validity",
        "Status": "⚠️ DATA ISSUE",
        "Details": f"{invalid_dates} rows ({pct:.2f}%) with order_dt=0. These are SOURCE DATA ISSUES - exclude in downstream processing."
    })
else:
    quality_checks.append({
        "Table": "crm_sales",
        "Check": "Date validity",
        "Status": "✅ PASS",
        "Details": "All order dates are valid"
    })

# 3. Customer demographics - check for CRM/ERP alignment
customers_df = spark.table(f"{CATALOG}.{DATABASE}.crm_customers")
demographics_df = spark.table(f"{CATALOG}.{DATABASE}.erp_customer_demographics")
location_df = spark.table(f"{CATALOG}.{DATABASE}.erp_customer_location")

crm_count = customers_df.count()
erp_demo_count = demographics_df.count()
erp_loc_count = location_df.count()

# Count distinct keys
crm_keys = customers_df.select("cst_key").distinct().count()
erp_keys = demographics_df.select("CID").distinct().count()

# Check how many CRM customers are missing in ERP
crm_not_in_erp = (customers_df
    .select("cst_key")
    .distinct()
    .join(demographics_df.select("CID").distinct(), 
          customers_df.cst_key == demographics_df.CID, "left_anti")
    .count())

if crm_not_in_erp > 0:
    pct = (crm_not_in_erp / crm_keys * 100)
    quality_checks.append({
        "Table": "customers",
        "Check": "CRM/ERP alignment",
        "Status": "⚠️ DATA ISSUE",
        "Details": f"CRM: {crm_keys} unique customers, ERP: {erp_keys}. {crm_not_in_erp} CRM customers ({pct:.1f}%) have no ERP demographics. This is a SOURCE DATA ISSUE - use LEFT JOIN in downstream processing."
    })
else:
    quality_checks.append({
        "Table": "customers",
        "Check": "CRM/ERP alignment",
        "Status": "✅ PASS",
        "Details": f"All {crm_keys} CRM customers have ERP demographics"
    })

# 4. Check for nulls in key columns
null_customer_ids = sales_df.filter(F.col("sls_cust_id").isNull()).count()
null_product_keys = sales_df.filter(F.col("sls_prd_key").isNull()).count()

if null_customer_ids > 0 or null_product_keys > 0:
    quality_checks.append({
        "Table": "crm_sales",
        "Check": "Foreign key completeness",
        "Status": "❌ DATA ISSUE",
        "Details": f"Null customer_ids: {null_customer_ids}, null product_keys: {null_product_keys}"
    })
else:
    quality_checks.append({
        "Table": "crm_sales",
        "Check": "Foreign key completeness",
        "Status": "✅ PASS",
        "Details": "All foreign keys are populated"
    })

# Print summary
for check in quality_checks:
    print(f"\n{check['Status']} {check['Table']}: {check['Check']}")
    print(f"   {check['Details']}")

print("\n" + "="*80)
print(f"Bronze ingestion complete. Total checks: {len(quality_checks)}")
print("\nNote: Bronze layer ingests raw data as-is. Data quality issues are flagged")
print("but not fixed here. Silver layer applies transformations and cleansing.")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Visualize Table Row Counts
# Visualize bronze table sizes
from pyspark.sql import functions as F

table_stats = []
for table in ["crm_customers", "crm_products", "crm_sales", 
              "erp_customer_demographics", "erp_customer_location", "erp_product_category"]:
    count = spark.table(f"{CATALOG}.{DATABASE}.{table}").count()
    source = "CRM" if table.startswith("crm_") else "ERP"
    table_stats.append({"table": table.replace("crm_", "").replace("erp_", ""), 
                        "row_count": count, 
                        "source": source})

stats_df = spark.createDataFrame(table_stats)
display(stats_df.orderBy(F.desc("row_count")))