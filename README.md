![Databricks](https://img.shields.io/badge/DATABRICKS-Latest-gray?style=flat-square&logo=databricks&logoColor=white&color=FF3621)
![Apache Spark](https://img.shields.io/badge/APACHE%20SPARK-3.x-gray?style=flat-square&logo=apachespark&logoColor=white&color=E25A1C)
![Python](https://img.shields.io/badge/PYTHON-3.8+-gray?style=flat-square&logo=python&logoColor=white&color=3776AB)
![Delta Lake](https://img.shields.io/badge/DELTA%20LAKE-Latest-gray?style=flat-square&logo=delta&logoColor=white&color=00A3E0)
![Unity Catalog](https://img.shields.io/badge/UNITY%20CATALOG-Data%20Governance-gray?style=flat-square&logo=databricks&logoColor=white&color=1F77B4)
![SQL](https://img.shields.io/badge/SQL-Standard-gray?style=flat-square&logo=sql&logoColor=white&color=CC2927)

# Big Data Project on Databricks

## Overview

This project demonstrates big data processing and analytics using Databricks platform. It leverages Apache Spark, Delta Lake, and Unity Catalog to handle, analyze, and visualize large-scale datasets efficiently.

## Architecture

* **Compute**: Databricks Serverless clusters for scalable processing
* **Storage**: Unity Catalog with Delta Lake tables
* **Processing**: Apache Spark for distributed data processing
* **Orchestration**: Databricks Jobs for workflow automation

## Prerequisites

* Databricks workspace (AWS, Azure, or GCP)
* Appropriate permissions to create catalogs, schemas, and tables
* Python 3.8+
* Access to data sources

## Databricks Setup

### 1. Create Unity Catalog

Unity Catalog provides centralized data governance and access control.

```sql
-- Create a new catalog for the project
CREATE CATALOG IF NOT EXISTS big_data_catalog
COMMENT 'Catalog for big data processing project';

-- Set the catalog as default
USE CATALOG big_data_catalog;
```

### 2. Create Schemas

Organize your data into schemas based on data layers (bronze, silver, gold).

```sql
-- Bronze layer: Raw data ingestion
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data layer for initial ingestion';

-- Silver layer: Cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned and enriched data layer';

-- Gold layer: Business-level aggregates
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready aggregated data layer';
```

### 3. Create Unity Catalog Volume

Volumes provide managed storage for non-tabular data (files, models, etc.).

```sql
-- Create a volume for raw data files
CREATE VOLUME IF NOT EXISTS big_data_catalog.bronze.raw_files
COMMENT 'Storage for raw data files';

-- Create a volume for processed files
CREATE VOLUME IF NOT EXISTS big_data_catalog.silver.processed_files
COMMENT 'Storage for intermediate processed files';
```

### 4. Configure Compute Cluster

Create a cluster for data processing:

1. Navigate to **Compute** in the Databricks workspace
2. Click **Create Cluster**
3. Configure the following:
   * **Cluster name**: `big-data-processing-cluster`
   * **Cluster mode**: Standard
   * **Databricks Runtime**: 14.3 LTS or later
   * **Node type**: Choose based on workload (e.g., `m5.xlarge` for AWS)
   * **Autoscaling**: Enable with min 2 and max 8 workers
   * **Auto termination**: 30 minutes of inactivity

Alternatively, use **Serverless** compute for instant startup and automatic scaling.

### 5. Grant Permissions

Set up appropriate permissions for your catalog and schemas:

```sql
-- Grant usage on catalog
GRANT USE CATALOG ON CATALOG big_data_catalog TO `user@example.com`;

-- Grant all privileges on schemas
GRANT ALL PRIVILEGES ON SCHEMA big_data_catalog.bronze TO `user@example.com`;
GRANT ALL PRIVILEGES ON SCHEMA big_data_catalog.silver TO `user@example.com`;
GRANT ALL PRIVILEGES ON SCHEMA big_data_catalog.gold TO `user@example.com`;

-- Grant read/write on volumes
GRANT READ VOLUME, WRITE VOLUME ON VOLUME big_data_catalog.bronze.raw_files TO `user@example.com`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME big_data_catalog.silver.processed_files TO `user@example.com`;
```

## Project Structure

```
big_Data/
├── notebooks/
│   ├── 01_data_ingestion.py       # Raw data ingestion to bronze layer
│   ├── 02_data_cleaning.py        # Data cleaning and validation
│   ├── 03_data_transformation.py  # Transform data to silver/gold layers
│   └── 04_data_analysis.py        # Analytics and visualizations
├── sql/
│   ├── create_tables.sql          # Table creation scripts
│   └── data_quality_checks.sql    # Data quality validation queries
├── config/
│   └── pipeline_config.yaml       # Pipeline configuration
├── README.md
└── requirements.txt
```

## Getting Started

### Step 1: Clone the Repository

In your Databricks workspace:

1. Navigate to **Repos**
2. Click **Add Repo**
3. Enter the Git URL:
   ```
   https://github.com/Zahrat-ElOula/big_Data.git
   ```
4. Click **Create Repo**

Alternatively, use Git from a notebook:

```bash
%sh
cd /Workspace/Users/your.email@example.com/
git clone https://github.com/Zahrat-ElOula/big_Data.git
```

### Step 2: Set Up Environment

Install required Python packages:

```bash
%pip install pandas numpy matplotlib seaborn pyspark
```

### Step 3: Configure Catalog and Schema

Run the setup notebook or execute SQL commands:

```python
# Python notebook cell
spark.sql("CREATE CATALOG IF NOT EXISTS big_data_catalog")
spark.sql("USE CATALOG big_data_catalog")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
```

### Step 4: Data Ingestion

Upload your data to Unity Catalog volumes:

```python
# Example: Upload CSV file to volume
dbutils.fs.cp(
    "file:/local/path/to/data.csv",
    "/Volumes/big_data_catalog/bronze/raw_files/data.csv"
)
```

Or ingest directly into Delta tables:

```python
# Read from external source
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/path/to/source/data.csv")

# Write to bronze layer
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("big_data_catalog.bronze.raw_data")
```

### Step 5: Run Notebooks

Execute the notebooks in order:

1. **Data Ingestion**: Load raw data into bronze layer
2. **Data Cleaning**: Clean and validate data
3. **Data Transformation**: Create silver and gold layer tables
4. **Data Analysis**: Perform analytics and create visualizations

## Data Pipeline

### Bronze Layer (Raw Data)

```sql
-- Example: Create bronze table for raw customer data
CREATE TABLE IF NOT EXISTS big_data_catalog.bronze.customers_raw (
    customer_id STRING,
    name STRING,
    email STRING,
    signup_date TIMESTAMP,
    ingestion_timestamp TIMESTAMP
) USING DELTA
LOCATION '/Volumes/big_data_catalog/bronze/raw_files/customers';
```

### Silver Layer (Cleaned Data)

```sql
-- Example: Create silver table with cleaned customer data
CREATE TABLE IF NOT EXISTS big_data_catalog.silver.customers_clean (
    customer_id STRING NOT NULL,
    name STRING,
    email STRING,
    signup_date DATE,
    is_active BOOLEAN,
    updated_timestamp TIMESTAMP
) USING DELTA
LOCATION '/Volumes/big_data_catalog/silver/processed_files/customers';
```

### Gold Layer (Aggregated Data)

```sql
-- Example: Create gold table with customer metrics
CREATE TABLE IF NOT EXISTS big_data_catalog.gold.customer_metrics (
    date DATE,
    total_customers INT,
    new_customers INT,
    active_customers INT,
    churn_rate DOUBLE
) USING DELTA
PARTITIONED BY (date);
```

## Workflow Automation

### Create a Databricks Job

1. Navigate to **Workflows** → **Jobs**
2. Click **Create Job**
3. Configure the job:
   * **Task name**: `big_data_pipeline`
   * **Type**: Notebook
   * **Source**: Select your notebook
   * **Cluster**: Use existing cluster or create new
4. Add dependencies between tasks
5. Set schedule (e.g., daily at 2 AM)

### Using Databricks CLI

```bash
# Create job from JSON configuration
databricks jobs create --json-file job_config.json

# Run job manually
databricks jobs run-now --job-id <job-id>
```

## Best Practices

### Performance Optimization

* **Partitioning**: Partition large tables by date or high-cardinality columns
* **Z-Ordering**: Use Z-ORDER BY for frequently filtered columns
* **Caching**: Cache frequently accessed DataFrames
* **Broadcast joins**: Use broadcast for small dimension tables

```python
# Example: Optimize table with Z-ORDER
spark.sql("""
    OPTIMIZE big_data_catalog.silver.customers_clean
    ZORDER BY (signup_date, customer_id)
""")
```

### Data Quality

* Implement data quality checks at each layer
* Use Delta Lake constraints for data validation
* Set up alerts for data anomalies

```sql
-- Add constraints
ALTER TABLE big_data_catalog.silver.customers_clean
ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%');
```

### Security

* Use Unity Catalog for fine-grained access control
* Implement row-level and column-level security
* Enable audit logging
* Use secrets for sensitive credentials

```python
# Access secrets securely
api_key = dbutils.secrets.get(scope="my-scope", key="api-key")
```

## Monitoring

* Monitor cluster utilization in the Databricks UI
* Review query execution plans with `EXPLAIN`
* Set up job alerts for failures
* Track data lineage in Unity Catalog

## Troubleshooting

### Common Issues

**Issue**: Permission denied errors
* **Solution**: Verify Unity Catalog permissions with `SHOW GRANTS ON <object>`

**Issue**: Out of memory errors
* **Solution**: Increase cluster size or optimize query with partitioning

**Issue**: Slow queries
* **Solution**: Check query plan with `EXPLAIN`, add proper partitioning and Z-ordering

## Contributing

1. Create a feature branch
2. Make your changes
3. Test thoroughly on a development catalog
4. Submit a pull request

## Resources

* [Databricks Documentation](https://docs.databricks.com/)
* [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Delta Lake Documentation](https://docs.delta.io/)
* [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please open an issue on GitHub or contact the project maintainer.
