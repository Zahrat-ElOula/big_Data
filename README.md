![Databricks](https://img.shields.io/badge/DATABRICKS-Latest-gray?style=flat-square&logo=databricks&logoColor=white&color=FF3621)
![Apache Spark](https://img.shields.io/badge/APACHE%20SPARK-3.x-gray?style=flat-square&logo=apachespark&logoColor=white&color=E25A1C)
![Python](https://img.shields.io/badge/PYTHON-3.8+-gray?style=flat-square&logo=python&logoColor=white&color=3776AB)
![Delta Lake](https://img.shields.io/badge/DELTA%20LAKE-Latest-gray?style=flat-square&logo=delta&logoColor=white&color=00A3E0)
![Unity Catalog](https://img.shields.io/badge/UNITY%20CATALOG-Data%20Governance-gray?style=flat-square&logo=databricks&logoColor=white&color=1F77B4)
![SQL](https://img.shields.io/badge/SQL-Standard-gray?style=flat-square&logo=sql&logoColor=white&color=CC2927)

# Big Data Project on Databricks

## Overview

This project demonstrates big data processing and analytics using Databricks platform. It leverages Apache Spark, Delta Lake, and Unity Catalog to handle, analyze, and visualize large-scale datasets efficiently following the medallion architecture pattern (Bronze, Silver, Gold layers).

## Architecture

* **Compute**: Databricks Serverless clusters for scalable processing
* **Storage**: Unity Catalog with Delta Lake tables
* **Processing**: Apache Spark for distributed data processing
* **Orchestration**: Databricks Jobs for workflow automation
* **Data Layers**: Bronze (raw), Silver (cleaned), Gold (aggregated)

## Prerequisites

* Databricks workspace (AWS, Azure, or GCP)
* Appropriate permissions to create catalogs, schemas, and tables
* Python 3.8+
* Access to data sources

## Databricks Setup - Step by Step

### Step 1: Create a Unity Catalog

In your Databricks workspace:

1. Navigate to **Catalog** in the left sidebar
2. Click **Create Catalog** button
3. Enter the catalog name: `big_data_catalog`
4. Add a description: "Catalog for big data processing project"
5. Set storage location (if needed)
6. Click **Create**
7. Set this catalog as the default workspace catalog

### Step 2: Create Schemas (Data Layers)

1. Open the newly created `big_data_catalog`
2. Click **Create Schema** to create three schemas:
   
   **Bronze Schema (Raw Data Layer)**
   - Click **Create Schema**
   - Name: `bronze`
   - Description: "Raw data layer for initial ingestion"
   - Click **Create**
   
   **Silver Schema (Cleaned Data Layer)**
   - Click **Create Schema**
   - Name: `silver`
   - Description: "Cleaned and enriched data layer"
   - Click **Create**
   
   **Gold Schema (Aggregated Data Layer)**
   - Click **Create Schema**
   - Name: `gold`
   - Description: "Business-ready aggregated data layer"
   - Click **Create**

### Step 3: Create Unity Catalog Volumes

Volumes provide managed storage for non-tabular data (files, models, etc.):

1. In the `big_data_catalog` → `bronze` schema:
   - Click **Create Volume**
   - Name: `raw_files`
   - Description: "Storage for raw data files"
   - Click **Create**

2. In the `big_data_catalog` → `silver` schema:
   - Click **Create Volume**
   - Name: `processed_files`
   - Description: "Storage for intermediate processed files"
   - Click **Create**

### Step 4: Configure Compute Cluster

1. Navigate to **Compute** in the left sidebar
2. Click **Create Cluster** button
3. Configure the following settings:
   * **Cluster name**: Enter `big-data-processing-cluster`
   * **Cluster mode**: Select **Standard**
   * **Databricks Runtime**: Choose **14.3 LTS** or later
   * **Node type**: Select based on workload (e.g., `m5.xlarge` for AWS)
   * **Number of workers**: Set min 2 and max 8 (for autoscaling)
   * **Auto termination**: Enable and set to **30 minutes** of inactivity
   * **Enable autoscaling**: Toggle ON
4. Click **Create Cluster**
5. Wait for the cluster to start (Status: "Running")

**Alternative - Use Serverless SQL**:
- Navigate to **SQL Warehouses**
- Click **Create SQL Warehouse**
- Select an appropriate size (Small, Medium, or Large)
- Warehouse will auto-scale and pause when idle

### Step 5: Set Up Permissions

1. Navigate to the `big_data_catalog`
2. Click the **Permissions** tab
3. Grant appropriate access levels:
   * Click **Grant** button
   * Select user(s) or group(s)
   * Assign permission level:
     - **Can Attach To**: Run queries
     - **Can Manage Metadata**: Create tables
     - **Can Manage**: Full control
4. Repeat for each schema (bronze, silver, gold)
5. Repeat for each volume (raw_files, processed_files)

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

### Step 1: Import the Repository

In your Databricks workspace:

1. Navigate to **Workspace** in the left sidebar
2. Click your username or select a folder
3. Click **Create** → **Folder** (optional)
4. Click **Create** → **Notebook**
5. Create a new notebook for importing the repo
6. In the notebook, use the following command:
   - In the terminal: `git clone https://github.com/Zahrat-ElOula/big_Data.git`

Or use the Repos feature:
1. Navigate to **Repos** in the sidebar
2. Click **Add Repo**
3. Select **GitHub**
4. Enter the Git URL: `https://github.com/Zahrat-ElOula/big_Data.git`
5. Click **Create Repo**

### Step 2: Set Up the Environment

1. Open or create a new notebook
2. Install required packages by running a cell with:
   - `%pip install pandas numpy matplotlib seaborn`
3. Wait for installation to complete
4. Verify PySpark is available (comes pre-installed)

### Step 3: Create Tables in Catalog

1. Open a **SQL Notebook** or create a notebook cell with SQL
2. Create each schema table:
   - Bronze layer: Raw customer data table
   - Silver layer: Cleaned customer data table
   - Gold layer: Customer metrics aggregation

3. Verify table creation:
   - Navigate to **Catalog** → `big_data_catalog`
   - Check each schema for created tables
   - Expand table details to view schema

### Step 4: Upload Data to Volumes

1. Navigate to **Catalog** → `big_data_catalog` → `bronze` → `raw_files`
2. Click **Upload File** button
3. Select your CSV/Parquet data file
4. Wait for upload to complete
5. View file in the volume

### Step 5: Execute Notebooks in Order

Using Databricks Workflow or manual execution:

1. **Start with 01_data_ingestion.py**
   - This loads raw data from volumes into bronze layer tables
   - Monitor execution in the notebook output
   - Check **Data** tab to see ingested records

2. **Run 02_data_cleaning.py**
   - Performs data validation and cleaning
   - Removes duplicates and null values
   - Writes cleaned data to silver layer tables
   - Review data quality metrics in output

3. **Execute 03_data_transformation.py**
   - Transforms silver layer data
   - Creates business logic aggregations
   - Writes final results to gold layer tables
   - Check gold layer table content

4. **Run 04_data_analysis.py**
   - Performs analytics and generates visualizations
   - Creates dashboards with insights
   - Generates reports

## Data Pipeline - Medallion Architecture

### Bronze Layer (Raw Data)

Purpose: Store raw, unprocessed data as-is from source systems

Actions in Databricks:
1. Upload raw data files to `bronze.raw_files` volume
2. Create raw data tables in bronze schema
3. Register tables in Unity Catalog
4. Enable change data capture (CDC) if needed

### Silver Layer (Cleaned Data)

Purpose: Store cleaned, validated, and de-duplicated data

Actions in Databricks:
1. Read from bronze layer tables
2. Apply data quality rules
3. Remove duplicates and null values
4. Add technical columns (created_date, updated_date)
5. Write to silver layer tables
6. Create indexes on frequently filtered columns

### Gold Layer (Aggregated Data)

Purpose: Store business-ready, aggregated data for reporting

Actions in Databricks:
1. Read from silver layer tables
2. Perform business logic transformations
3. Create aggregations and metrics
4. Join with dimension tables
5. Write to gold layer tables
6. Partition by date or business key

## Workflow Automation - Create a Databricks Job

1. Navigate to **Workflows** in the left sidebar
2. Click **Create Job**
3. Configure the job:
   * **Job name**: `big_data_pipeline`
   * **Task name**: `ingest_data`
   * **Type**: Select **Notebook**
   * **Source**: Select notebook `01_data_ingestion.py`
   * **Cluster**: Select the cluster created earlier
4. Click **Add Task** to add dependent tasks:
   * Add `cleaning_data` task → depends on `ingest_data`
   * Add `transform_data` task → depends on `cleaning_data`
   * Add `analyze_data` task → depends on `transform_data`
5. Set the schedule:
   * Click **Trigger** section
   * Select **Schedule**
   * Set frequency (e.g., Daily at 2 AM)
   * Set timezone
6. Click **Create Job**
7. Monitor job runs in the **Runs** tab

## Best Practices

### Performance Optimization

* **Partitioning**: Partition large tables by date or high-cardinality columns for faster queries
* **Z-Ordering**: Organize data within partitions for efficient filtering
* **Caching**: Use Databricks Delta Cache for frequently accessed DataFrames
* **Broadcast joins**: Use for joining large tables with small dimension tables
* **Monitor query performance**: Use the **Query Profile** in the notebook to identify bottlenecks

### Data Quality

* Implement data quality checks at each layer in your notebooks
* Use Delta Lake constraints for data validation
* Set up alerts for data anomalies via Databricks SQL alerts
* Monitor data lineage in Unity Catalog
* Create data quality dashboards for monitoring

### Security

* Use Unity Catalog for fine-grained access control
* Implement row-level and column-level security using row filters and column masks
* Enable audit logging in Databricks settings
* Use secrets for sensitive credentials (API keys, passwords)
* Store secrets in Databricks Secrets API
* Restrict cluster access to authorized users

## Monitoring and Troubleshooting

### Monitoring in Databricks

1. **Cluster Utilization**:
   - Navigate to **Compute**
   - Select your cluster
   - View **Metrics** tab for CPU, memory, disk usage

2. **Query Performance**:
   - Navigate to **SQL** → **Query History**
   - Review query execution time
   - Click on query to see **Query Profile** and optimization suggestions

3. **Job Runs**:
   - Navigate to **Workflows** → **Jobs**
   - Click on your job
   - View **Runs** tab for run history and status
   - Click on failed runs to view error logs

4. **Data Lineage**:
   - Navigate to **Catalog** → select table
   - Click **Lineage** tab to see data flow

### Common Issues and Solutions

**Issue**: Permission denied errors when accessing tables
* **Solution**: Check permissions in **Catalog** → table → **Permissions** tab. Grant appropriate access levels to users/groups.

**Issue**: Out of memory errors during processing
* **Solution**: Increase cluster size by adding more workers, optimize queries with proper partitioning, or reduce batch size.

**Issue**: Slow query performance
* **Solution**: Review query execution plan in notebook, add Z-ordering for frequently filtered columns, enable partition pruning, use broadcast joins for small tables.

**Issue**: Data not appearing in gold tables
* **Solution**: Check notebook logs for errors, verify all upstream tables have data, review SQL for transformation logic errors.

## Contributing

1. Create a feature branch in the GitHub repository
2. Make your changes in a development catalog/schema
3. Test thoroughly before committing
4. Create a pull request with detailed description
5. Once approved, merge to main branch
6. Update production jobs to use latest version

## Resources

* [Databricks Documentation](https://docs.databricks.com/)
* [Unity Catalog Guide](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Delta Lake Documentation](https://docs.delta.io/)
* [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
* [Databricks Learning Academy](https://learn.databricks.com/)

## License

This project is licensed under the MIT License.

## Contact

For questions or support, please open an issue on GitHub or contact the project maintainer.
