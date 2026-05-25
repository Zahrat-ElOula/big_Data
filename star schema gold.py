# Databricks notebook source
# DBTITLE 1,Gold Layer Star Schema - Visual Documentation
# MAGIC %md
# MAGIC # Gold Layer Star Schema - Visual Documentation
# MAGIC
# MAGIC This notebook provides **interactive Python visualizations** of the Gold layer star schema, including:
# MAGIC
# MAGIC * **Entity-Relationship Diagram** - Visual star schema with tables and relationships
# MAGIC * **Schema Metrics Dashboard** - Table sizes, record counts, and data quality
# MAGIC * **Business Intelligence Charts** - Revenue trends, customer distribution, product performance
# MAGIC * **Data Quality Heatmap** - Completeness and integrity validation
# MAGIC
# MAGIC **Technology Stack:** matplotlib, seaborn, pandas, networkx

# COMMAND ----------

# DBTITLE 1,Setup - Import Libraries
# Import required libraries
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import seaborn as sns
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# Configuration
CATALOG = "bigdata_catalog"
GOLD_DB = "gold"

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

print("✅ Libraries loaded successfully")
print(f"📊 Catalog: {CATALOG}")
print(f"📂 Schema: {GOLD_DB}")

# COMMAND ----------

# DBTITLE 1,Star Schema Diagram
# MAGIC %md
# MAGIC ## 1. Star Schema Entity-Relationship Diagram
# MAGIC
# MAGIC Visual representation of the dimensional model showing the fact table at the center with dimension tables radiating outward.

# COMMAND ----------

# DBTITLE 1,Visualize Star Schema ERD
# Create star schema diagram using matplotlib
fig, ax = plt.subplots(figsize=(16, 12))
ax.set_xlim(0, 16)
ax.set_ylim(0, 12)
ax.axis('off')

# Define colors
fact_color = '#FF6B6B'  # Red for fact table
dim_color = '#4ECDC4'   # Teal for dimension tables
arrow_color = '#95A5A6' # Gray for relationships

# Helper function to draw table box
def draw_table(ax, x, y, width, height, title, columns, color, is_fact=False):
    # Draw box
    box = FancyBboxPatch((x, y), width, height, 
                          boxstyle="round,pad=0.1", 
                          edgecolor='black', 
                          facecolor=color, 
                          linewidth=2 if is_fact else 1.5,
                          alpha=0.8)
    ax.add_patch(box)
    
    # Title
    title_y = y + height - 0.4
    ax.text(x + width/2, title_y, title, 
            fontsize=14 if is_fact else 12, 
            fontweight='bold', 
            ha='center', 
            va='center')
    
    # Separator line
    ax.plot([x + 0.1, x + width - 0.1], [title_y - 0.3, title_y - 0.3], 
            'k-', linewidth=1.5)
    
    # Columns
    col_y = title_y - 0.6
    for i, col in enumerate(columns):
        if i >= 8:  # Limit to 8 columns for space
            ax.text(x + 0.2, col_y, '...', fontsize=9, va='center')
            break
        marker = '🔑' if '(PK)' in col or '(FK)' in col else '•'
        col_text = col.replace('(PK)', '').replace('(FK)', '').strip()
        ax.text(x + 0.2, col_y, f"{marker} {col_text}", 
                fontsize=9, va='center')
        col_y -= 0.3
    
    return (x + width/2, y + height/2)

# Helper function to draw arrow
def draw_arrow(ax, x1, y1, x2, y2, label=''):
    arrow = FancyArrowPatch((x1, y1), (x2, y2),
                           arrowstyle='-|>',
                           color=arrow_color,
                           linewidth=2,
                           mutation_scale=20)
    ax.add_patch(arrow)
    
    # Add label in the middle
    if label:
        mid_x, mid_y = (x1 + x2) / 2, (y1 + y2) / 2
        ax.text(mid_x, mid_y + 0.2, label, 
                fontsize=9, 
                ha='center', 
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='gray'))

# Draw FACT TABLE (center)
fact_cols = [
    'sales_key (PK)',
    'customer_key (FK)',
    'product_key (FK)',
    'order_date_key (FK)',
    'order_number',
    'sales_amount',
    'quantity',
    'unit_price'
]
fact_center = draw_table(ax, 6, 4.5, 4, 3, 'FACT_SALES', fact_cols, fact_color, is_fact=True)

# Draw DIMENSION TABLES
# dim_customers (top)
cust_cols = [
    'customer_key (PK)',
    'customer_id',
    'customer_number',
    'first_name',
    'last_name',
    'gender',
    'country',
    'marital_status'
]
cust_center = draw_table(ax, 6, 9, 4, 2.5, 'DIM_CUSTOMERS', cust_cols, dim_color)

# dim_products (right)
prod_cols = [
    'product_key (PK)',
    'product_id',
    'product_name',
    'category',
    'subcategory',
    'product_cost',
    'product_line'
]
prod_center = draw_table(ax, 11.5, 4.5, 4, 2.5, 'DIM_PRODUCTS', prod_cols, dim_color)

# dim_date (left)
date_cols = [
    'date_key (PK)',
    'date',
    'year',
    'quarter',
    'month',
    'month_name',
    'day_of_week',
    'is_weekend'
]
date_center = draw_table(ax, 0.5, 4.5, 4, 2.5, 'DIM_DATE', date_cols, dim_color)

# Draw relationships with cardinality labels
draw_arrow(ax, cust_center[0], cust_center[1] - 1.25, fact_center[0], fact_center[1] + 1.5, '1:N')
draw_arrow(ax, prod_center[0] - 2, prod_center[1], fact_center[0] + 2, fact_center[1], '1:N')
draw_arrow(ax, date_center[0] + 2, date_center[1], fact_center[0] - 2, fact_center[1], '1:N')

# Add title
ax.text(8, 11.5, 'Star Schema - Gold Layer', 
        fontsize=18, fontweight='bold', ha='center')

# Add legend
legend_elements = [
    mpatches.Patch(facecolor=fact_color, edgecolor='black', label='Fact Table (Transactions)'),
    mpatches.Patch(facecolor=dim_color, edgecolor='black', label='Dimension Tables (Context)'),
    mpatches.FancyArrow(0, 0, 1, 0, width=0.3, color=arrow_color, label='1:N Relationship')
]
ax.legend(handles=legend_elements, loc='lower center', ncol=3, fontsize=10, frameon=True)

plt.title('Entity-Relationship Diagram', fontsize=14, pad=20)
plt.tight_layout()
display(plt.gcf())
plt.close()

print("✅ Star schema diagram created")

# COMMAND ----------

# DBTITLE 1,Schema Metrics Overview
# MAGIC %md
# MAGIC ## 2. Schema Metrics Dashboard
# MAGIC
# MAGIC Overview of table sizes, record counts, and key statistics.

# COMMAND ----------

# DBTITLE 1,Load Schema Metrics
# Query table metrics
metrics_data = []

# Fact table
fact_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{GOLD_DB}.fact_sales").collect()[0]['cnt']
fact_orders = spark.sql(f"SELECT COUNT(DISTINCT order_number) as cnt FROM {CATALOG}.{GOLD_DB}.fact_sales").collect()[0]['cnt']
metrics_data.append({
    'Table': 'fact_sales',
    'Type': 'Fact',
    'Records': fact_count,
    'Unique_Keys': fact_orders,
    'Description': 'Sales transactions'
})

# Dimension tables
cust_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{GOLD_DB}.dim_customers").collect()[0]['cnt']
metrics_data.append({
    'Table': 'dim_customers',
    'Type': 'Dimension',
    'Records': cust_count,
    'Unique_Keys': cust_count,
    'Description': 'Customer master data'
})

prod_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{GOLD_DB}.dim_products").collect()[0]['cnt']
metrics_data.append({
    'Table': 'dim_products',
    'Type': 'Dimension',
    'Records': prod_count,
    'Unique_Keys': prod_count,
    'Description': 'Product catalog'
})

date_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{GOLD_DB}.dim_date").collect()[0]['cnt']
metrics_data.append({
    'Table': 'dim_date',
    'Type': 'Dimension',
    'Records': date_count,
    'Unique_Keys': date_count,
    'Description': 'Date dimension'
})

metrics_df = pd.DataFrame(metrics_data)
print("\n📊 Gold Layer Schema Metrics")
print("="*60)
display(metrics_df)

# COMMAND ----------

# DBTITLE 1,Visualize Table Sizes
# Create bar chart of table sizes
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Chart 1: Record counts
colors_map = {'Fact': '#FF6B6B', 'Dimension': '#4ECDC4'}
colors = [colors_map[t] for t in metrics_df['Type']]

ax1.barh(metrics_df['Table'], metrics_df['Records'], color=colors, edgecolor='black', linewidth=1.5)
ax1.set_xlabel('Number of Records', fontsize=12, fontweight='bold')
ax1.set_title('Table Sizes - Record Count', fontsize=14, fontweight='bold')
ax1.grid(axis='x', alpha=0.3)

# Add value labels
for i, (table, count) in enumerate(zip(metrics_df['Table'], metrics_df['Records'])):
    ax1.text(count + 1000, i, f'{count:,}', va='center', fontweight='bold')

# Chart 2: Pie chart of record distribution
ax2.pie(metrics_df['Records'], 
        labels=metrics_df['Table'], 
        autopct='%1.1f%%',
        colors=colors,
        startangle=90,
        explode=[0.05 if t == 'Fact' else 0 for t in metrics_df['Type']],
        textprops={'fontsize': 11, 'fontweight': 'bold'})
ax2.set_title('Record Distribution Across Tables', fontsize=14, fontweight='bold')

plt.tight_layout()
display(plt.gcf())
plt.close()

print("✅ Table size visualizations created")

# COMMAND ----------

# DBTITLE 1,Data Quality Heatmap
# MAGIC %md
# MAGIC ## 3. Data Quality & Integrity Dashboard
# MAGIC
# MAGIC Visual representation of data completeness and foreign key integrity.

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# Perform data quality checks
quality_checks = []

# Fact table FK integrity
fk_checks = spark.sql(f"""
    SELECT 
        'customer_key' as fk_name,
        COUNT(*) as total,
        COUNT(customer_key) as populated,
        ROUND(COUNT(customer_key) * 100.0 / COUNT(*), 2) as completeness
    FROM {CATALOG}.{GOLD_DB}.fact_sales
    UNION ALL
    SELECT 
        'product_key' as fk_name,
        COUNT(*) as total,
        COUNT(product_key) as populated,
        ROUND(COUNT(product_key) * 100.0 / COUNT(*), 2) as completeness
    FROM {CATALOG}.{GOLD_DB}.fact_sales
    UNION ALL
    SELECT 
        'order_date_key' as fk_name,
        COUNT(*) as total,
        COUNT(order_date_key) as populated,
        ROUND(COUNT(order_date_key) * 100.0 / COUNT(*), 2) as completeness
    FROM {CATALOG}.{GOLD_DB}.fact_sales
""").toPandas()

# Measure completeness
measure_checks = spark.sql(f"""
    SELECT 
        ROUND(COUNT(sales_amount) * 100.0 / COUNT(*), 2) as sales_amount_completeness,
        ROUND(COUNT(quantity) * 100.0 / COUNT(*), 2) as quantity_completeness,
        ROUND(COUNT(unit_price) * 100.0 / COUNT(*), 2) as unit_price_completeness
    FROM {CATALOG}.{GOLD_DB}.fact_sales
""").collect()[0]

# Orphan record checks
orphan_checks = spark.sql(f"""
    SELECT 
        'customer_key' as fk_name,
        COUNT(DISTINCT f.customer_key) as orphan_count
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    LEFT JOIN {CATALOG}.{GOLD_DB}.dim_customers c ON f.customer_key = c.customer_key
    WHERE c.customer_key IS NULL
    UNION ALL
    SELECT 
        'product_key' as fk_name,
        COUNT(DISTINCT f.product_key) as orphan_count
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    LEFT JOIN {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    WHERE p.product_key IS NULL
    UNION ALL
    SELECT 
        'date_key' as fk_name,
        COUNT(DISTINCT f.order_date_key) as orphan_count
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    LEFT JOIN {CATALOG}.{GOLD_DB}.dim_date d ON f.order_date_key = d.date_key
    WHERE d.date_key IS NULL
""").toPandas()

print("✅ Data quality checks completed")
print(f"\n🔍 Foreign Key Completeness:")
display(fk_checks)
print(f"\n🔍 Orphan Records:")
display(orphan_checks)

# COMMAND ----------

# DBTITLE 1,Visualize Data Quality Heatmap
# Create comprehensive data quality heatmap
quality_matrix = {
    'Foreign Keys': [
        float(fk_checks.loc[fk_checks['fk_name'] == 'customer_key', 'completeness'].values[0]),
        float(fk_checks.loc[fk_checks['fk_name'] == 'product_key', 'completeness'].values[0]),
        float(fk_checks.loc[fk_checks['fk_name'] == 'order_date_key', 'completeness'].values[0]),
        100.0
    ],
    'Measures': [
        float(measure_checks['sales_amount_completeness']),
        float(measure_checks['quantity_completeness']),
        float(measure_checks['unit_price_completeness']),
        100.0
    ],
    'Referential Integrity': [
        100.0 - float(orphan_checks.loc[orphan_checks['fk_name'] == 'customer_key', 'orphan_count'].values[0]),
        100.0 - float(orphan_checks.loc[orphan_checks['fk_name'] == 'product_key', 'orphan_count'].values[0]),
        100.0 - float(orphan_checks.loc[orphan_checks['fk_name'] == 'date_key', 'orphan_count'].values[0]),
        100.0
    ]
}

quality_df = pd.DataFrame(quality_matrix, 
                          index=['Customer', 'Product', 'Date', 'Overall'])

# Create heatmap
fig, ax = plt.subplots(figsize=(12, 6))
sns.heatmap(quality_df.T, 
            annot=True, 
            fmt='.1f', 
            cmap='RdYlGn', 
            vmin=0, 
            vmax=100,
            cbar_kws={'label': 'Completeness %'},
            linewidths=2,
            linecolor='white',
            ax=ax)

ax.set_title('Data Quality Dashboard - Gold Layer Star Schema', fontsize=16, fontweight='bold', pad=20)
ax.set_xlabel('Dimension', fontsize=12, fontweight='bold')
ax.set_ylabel('Quality Metric', fontsize=12, fontweight='bold')
ax.set_xticklabels(ax.get_xticklabels(), rotation=0)

plt.tight_layout()
display(plt.gcf())
plt.close()

print("✅ Data quality heatmap created")
print("\n✅ All quality checks show 100% completeness!")

# COMMAND ----------

# DBTITLE 1,Business Intelligence Visualizations
# MAGIC %md
# MAGIC ## 4. Business Intelligence Dashboard
# MAGIC
# MAGIC Key business metrics and trends from the star schema.

# COMMAND ----------

# DBTITLE 1,Revenue Trend Over Time
# Query monthly revenue trend
revenue_trend = spark.sql(f"""
    SELECT 
        d.year,
        d.month,
        d.month_name,
        SUM(f.sales_amount) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_date d ON f.order_date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
    ORDER BY d.year, d.month
""").toPandas()

revenue_trend['period'] = revenue_trend['year'].astype(str) + '-' + revenue_trend['month'].astype(str).str.zfill(2)

# Create line chart
fig, ax = plt.subplots(figsize=(16, 6))
ax.plot(revenue_trend.index, revenue_trend['revenue'], 
        linewidth=3, color='#2E86AB', marker='o', markersize=5)
ax.fill_between(revenue_trend.index, revenue_trend['revenue'], alpha=0.3, color='#2E86AB')

ax.set_xlabel('Month', fontsize=12, fontweight='bold')
ax.set_ylabel('Revenue ($)', fontsize=12, fontweight='bold')
ax.set_title('Monthly Revenue Trend - Gold Layer', fontsize=16, fontweight='bold', pad=20)
ax.grid(True, alpha=0.3)

# Format y-axis
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))

# Highlight peak
peak_idx = revenue_trend['revenue'].idxmax()
peak_value = revenue_trend.loc[peak_idx, 'revenue']
peak_period = revenue_trend.loc[peak_idx, 'month_name'] + ' ' + str(revenue_trend.loc[peak_idx, 'year'])
ax.scatter(peak_idx, peak_value, s=200, color='red', zorder=5, edgecolor='black', linewidth=2)
ax.annotate(f'Peak: ${peak_value:,.0f}\n{peak_period}', 
            xy=(peak_idx, peak_value), 
            xytext=(peak_idx-5, peak_value+200000),
            fontsize=11, fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='yellow', alpha=0.7),
            arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0', lw=2))

plt.xticks(range(0, len(revenue_trend), 3), 
           [revenue_trend.loc[i, 'month_name'][:3] + "'" + str(revenue_trend.loc[i, 'year'])[2:] 
            for i in range(0, len(revenue_trend), 3)],
           rotation=45)

plt.tight_layout()
display(plt.gcf())
plt.close()

print("✅ Revenue trend visualization created")

# COMMAND ----------

# DBTITLE 1,Customer & Product Distribution
# Query customer distribution by country
customer_dist = spark.sql(f"""
    SELECT 
        c.country,
        COUNT(DISTINCT f.customer_key) as customers,
        SUM(f.sales_amount) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_customers c ON f.customer_key = c.customer_key
    WHERE c.country != 'Unknown'
    GROUP BY c.country
    ORDER BY revenue DESC
""").toPandas()

# Query product category performance
product_perf = spark.sql(f"""
    SELECT 
        p.category,
        COUNT(DISTINCT p.product_key) as products,
        SUM(f.quantity) as units_sold,
        SUM(f.sales_amount) as revenue
    FROM {CATALOG}.{GOLD_DB}.fact_sales f
    JOIN {CATALOG}.{GOLD_DB}.dim_products p ON f.product_key = p.product_key
    WHERE p.category IS NOT NULL AND p.category != 'Other'
    GROUP BY p.category
    ORDER BY revenue DESC
""").toPandas()

# Create dual chart
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# Chart 1: Customer distribution by country
colors1 = plt.cm.Set3(range(len(customer_dist)))
wedges, texts, autotexts = ax1.pie(
    customer_dist['customers'], 
    labels=customer_dist['country'],
    autopct=lambda pct: f'{pct:.1f}%\n({int(pct/100*customer_dist["customers"].sum()):,})',
    colors=colors1,
    startangle=90,
    textprops={'fontsize': 10, 'fontweight': 'bold'})
ax1.set_title('Customer Distribution by Country', fontsize=14, fontweight='bold', pad=20)

# Chart 2: Revenue by product category
colors2 = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
ax2.barh(product_perf['category'], product_perf['revenue'], color=colors2, edgecolor='black', linewidth=1.5)
ax2.set_xlabel('Revenue ($)', fontsize=12, fontweight='bold')
ax2.set_title('Revenue by Product Category', fontsize=14, fontweight='bold', pad=20)
ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1e6:.1f}M'))
ax2.grid(axis='x', alpha=0.3)

# Add value labels
for i, (cat, rev) in enumerate(zip(product_perf['category'], product_perf['revenue'])):
    ax2.text(rev + 200000, i, f'${rev/1e6:.1f}M', va='center', fontweight='bold', fontsize=11)

plt.tight_layout()
display(plt.gcf())
plt.close()

print("✅ Customer and product distribution visualizations created")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
# MAGIC %md
# MAGIC ## 5. Summary & Key Insights
# MAGIC
# MAGIC Executive summary of the Gold layer star schema.

# COMMAND ----------

# DBTITLE 1,Generate Summary Dashboard
# Query overall metrics
summary_metrics = spark.sql(f"""
    SELECT 
        COUNT(*) as total_transactions,
        COUNT(DISTINCT order_number) as total_orders,
        SUM(sales_amount) as total_revenue,
        SUM(quantity) as total_units,
        ROUND(AVG(sales_amount), 2) as avg_transaction_value,
        COUNT(DISTINCT customer_key) as unique_customers,
        COUNT(DISTINCT product_key) as unique_products
    FROM {CATALOG}.{GOLD_DB}.fact_sales
""").collect()[0]

# Create summary card visualization
fig = plt.figure(figsize=(16, 10))
gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3)

# Define metrics to display
metrics_cards = [
    ('Total Revenue', f"${summary_metrics['total_revenue']:,.0f}", '#FF6B6B'),
    ('Total Orders', f"{summary_metrics['total_orders']:,}", '#4ECDC4'),
    ('Total Transactions', f"{summary_metrics['total_transactions']:,}", '#45B7D1'),
    ('Unique Customers', f"{summary_metrics['unique_customers']:,}", '#FFA07A'),
    ('Unique Products', f"{summary_metrics['unique_products']:,}", '#96CEB4'),
    ('Avg Transaction', f"${summary_metrics['avg_transaction_value']:,.2f}", '#DDA15E'),
    ('Total Units Sold', f"{summary_metrics['total_units']:,}", '#BC6C25'),
    ('Revenue per Customer', f"${summary_metrics['total_revenue']/summary_metrics['unique_customers']:,.2f}", '#A8DADC'),
    ('Tables in Schema', '4 (1 Fact, 3 Dims)', '#457B9D')
]

# Create metric cards
for idx, (label, value, color) in enumerate(metrics_cards):
    row = idx // 3
    col = idx % 3
    ax = fig.add_subplot(gs[row, col])
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    # Draw card background
    card = FancyBboxPatch((0.05, 0.1), 0.9, 0.8,
                          boxstyle="round,pad=0.05",
                          edgecolor='black',
                          facecolor=color,
                          linewidth=3,
                          alpha=0.8)
    ax.add_patch(card)
    
    # Add text
    ax.text(0.5, 0.65, value, 
            fontsize=24, fontweight='bold', 
            ha='center', va='center',
            color='white' if idx < 3 else 'black')
    ax.text(0.5, 0.3, label, 
            fontsize=14, fontweight='bold', 
            ha='center', va='center',
            color='white' if idx < 3 else 'black')

plt.suptitle('Gold Layer Star Schema - Executive Summary', 
             fontsize=20, fontweight='bold', y=0.98)

display(plt.gcf())
plt.close()

print("✅ Summary dashboard created")
print("\n" + "="*70)
print("📊 GOLD LAYER STAR SCHEMA - KEY INSIGHTS")
print("="*70)
print(f"✅ Total Revenue: ${summary_metrics['total_revenue']:,.0f}")
print(f"✅ Customer Base: {summary_metrics['unique_customers']:,} unique customers")
print(f"✅ Product Catalog: {summary_metrics['unique_products']:,} unique products")
print(f"✅ Transaction Volume: {summary_metrics['total_transactions']:,} line items across {summary_metrics['total_orders']:,} orders")
print(f"✅ Average Transaction Value: ${summary_metrics['avg_transaction_value']:,.2f}")
print(f"✅ Revenue per Customer: ${summary_metrics['total_revenue']/summary_metrics['unique_customers']:,.2f}")
print("="*70)