E-Commerce Lakehouse on Databricks

Customer 360 & Sales Analytics Using Medallion Architecture

An end-to-end data engineering project built using Databricks, PySpark, Spark SQL, Delta Lake, and Unity Catalog to transform raw e-commerce data into analytics-ready datasets.

The project implements a Bronze → Silver → Gold architecture, incremental data processing, data quality validation, dimensional modeling, access governance, and business intelligence dashboards.

⸻

Project Overview

E-commerce platforms generate data across customers, orders, payments, products, sellers, inventory, and customer interactions. When these datasets are stored separately and contain inconsistent or duplicate records, generating reliable business insights becomes difficult.

This project addresses these challenges by building a Lakehouse pipeline that ingests raw data, cleans and validates records, maintains historical changes, and produces curated datasets for customer and sales analytics.

Project Highlights

* 10 source datasets covering core e-commerce operations.
* Approximately 2.04 million initial source records.
* Medallion Architecture with Bronze, Silver, and Gold layers.
* Incremental ingestion using Databricks Auto Loader.
* Delta Lake tables with Change Data Feed (CDF).
* Slowly Changing Dimensions (SCD) Type 1 and Type 2.
* A Gold-layer star schema with one fact table and five dimensions.
* Five analytical data marts, including Customer 360.
* Unity Catalog governance using RBAC, row filters, and column masks.
* Lakeflow Jobs for pipeline orchestration.
* Four AI/BI dashboards with Genie integration.

⸻

Technology Stack

Technology	Purpose
Databricks	Data engineering and analytics platform
PySpark	Data ingestion, transformation, and processing
Spark SQL	Data validation, SQL transformations, and analytics
Delta Lake	ACID transactions, MERGE operations, and version history
Auto Loader	Incremental file ingestion
Unity Catalog	Data organization, permissions, and governance
Change Data Feed	Processing inserts, updates, and deletes incrementally
SCD Type 1 & Type 2	Managing current values and historical changes
Lakeflow Jobs	Pipeline orchestration and task dependencies
Databricks SQL	Querying curated analytical tables
AI/BI Dashboards and Genie	Business reporting and natural-language analytics

⸻

Architecture

The pipeline follows the Medallion Architecture.

1. Bronze Layer — Raw Data Ingestion

The Bronze layer ingests the source CSV files into Delta tables using Databricks Auto Loader.

Key features:

* CSV ingestion using the cloudFiles source.
* Schema inference and schema tracking.
* Checkpoints to track ingestion progress.
* Lineage columns such as _ingest_time and _source_file.
* Change Data Feed enabled on Bronze tables.
* Preservation of raw records for downstream processing.

Output: 10 Bronze Delta tables.

2. Silver Layer — Cleaning and Validation

The Silver layer transforms raw records into cleaned and conformed datasets.

Key operations:

* Standardization of column values, data types, and timestamps.
* Duplicate detection and removal.
* Validation of required fields and business rules.
* Handling of invalid dates, ratings, prices, quantities, and payment values.
* Quarantine of records that fail critical validation rules.
* Deduplication based on business keys and update timestamps.
* Incremental processing using Bronze Change Data Feed.
* MERGE operations using SCD Type 1, SCD Type 2, and insert-only strategies.

Output: 10 Silver business tables, together with supporting quarantine and audit tables.

3. Gold Layer — Analytics and Dimensional Modeling

The Gold layer transforms Silver data into business-oriented tables for reporting.

The project uses a star schema consisting of one fact table and five dimension tables.

Fact table:

* gold_fact_sales

Dimension tables:

* gold_dim_date
* gold_dim_customer
* gold_dim_product
* gold_dim_seller
* gold_dim_campaign

The sales fact table has one row per order line and includes measures such as GMV, net sales, units sold, cost, gross margin, commission, returned value, and delivery performance.

Surrogate keys are used for dimensional relationships, and an Unknown member supports records with missing dimension matches.

Business Data Marts

The Gold layer includes five analytical marts:

Data Mart	Purpose
gold_customer_360	Customer lifetime value, spending, order behaviour, segmentation, and churn risk
gold_product_performance	Product sales, revenue, margins, ratings, conversion, and returns
gold_seller_performance	Seller revenue, commission, ratings, delivery performance, and cancellations
gold_inventory_risk	Stock availability, demand, reorder recommendations, and inventory risk
gold_daily_sales_summary	Daily sales, orders, customers, discounts, returns, and margins

⸻

Incremental Processing and Historical Data

The pipeline uses Delta Lake Change Data Feed and MERGE operations to process changes without unnecessarily reprocessing all source data.

SCD Type 1

SCD Type 1 overwrites existing attribute values when newer records arrive.

It is used for entities where the latest state is sufficient, including orders, order items, payments, reviews, and campaigns.

SCD Type 2

SCD Type 2 preserves historical versions of records.

It is implemented for customers, products, and sellers.

Historical records are managed using fields such as:

* valid_from
* valid_to
* is_current

When a tracked attribute changes, the previous version is closed and a new current version is inserted.

Change Data Feed

CDF supports incremental processing between layers:

1. Bronze records source changes.
2. Silver reads new changes using saved processing bookmarks.
3. Silver transformations apply validation and the appropriate update strategy.
4. Gold processes changes from Silver to update dimensions, facts, and analytical marts.

Processing bookmarks advance after successful writes to help support reliable incremental execution.

⸻

Data Quality Framework

The Silver layer addresses common data quality issues found in operational datasets.

Dataset	Example issue	Handling
Customers	Missing identifiers and inconsistent contact values	Quarantine, standardization, and validation
Sellers	Invalid GSTIN values and commission rates	Validation and audit records
Products	Missing or invalid prices	Validation and quarantine
Orders	Duplicate records and invalid timestamps	Deduplication and quarantine
Order items	Zero or negative quantities	Quarantine and validation
Payments	Currency-formatted amounts and duplicate transactions	Standardization and deduplication
Reviews	Ratings outside the 1–5 range	Validation
Inventory	Negative or over-reserved stock	Quarantine and inventory checks
Clickstream	Duplicate events and missing sessions	Deduplication, validation, and quarantine

The framework uses a combination of correction, standardization, flagging, deduplication, and quarantine rather than treating every data issue in the same way.

⸻

Governance and Security

Unity Catalog provides centralized governance for the lakehouse.

Role-Based Access Control (RBAC)

Access is organized around roles such as:

* platform_admins
* data_engineers
* analysts
* regional_managers

Permissions determine which layers and objects each role can access.

Attribute-Based Access Control (ABAC)

The project implements:

* Region-based row filters.
* Column masks for sensitive information.
* Sensitivity tags and governance attributes.
* Restricted access to personal and financial information.

Regional managers are intended to access data for their authorized regions, while analysts receive governed access to Gold-layer data.

Note: Governance policies and group memberships must be configured in the target Databricks environment. They do not automatically transfer with notebook files.

⸻

Performance and Reliability

The project demonstrates several Delta Lake optimization and recovery features:

* OPTIMIZE for file compaction.
* Liquid Clustering for frequently queried Gold data.
* Z-ORDER for selected query patterns.
* VACUUM for removing eligible unreferenced files.
* Delta Lake Time Travel for historical queries.
* RESTORE for recovering previous table versions.

These operations support efficient data access, storage maintenance, and recovery from unintended changes.

⸻

Pipeline Orchestration

The pipeline is orchestrated using Databricks Lakeflow Jobs.

The workflow includes tasks for:

1. Bronze ingestion.
2. Silver transformations and data quality checks.
3. Gold dimension processing.
4. Gold fact table processing.
5. Analytical mart generation.
6. Governance setup.
7. Data quality validation.
8. Delta Lake maintenance.
9. Dashboard refreshes.

The capstone implementation contains a 29-task job workflow.

Task dependencies ensure that upstream datasets are processed before downstream transformations that rely on them.

⸻

Dashboards and Business Analytics

The project includes four AI/BI dashboards.

Dashboard	Business questions
Executive Dashboard	How are GMV, net sales, orders, margins, and regional performance changing?
Customer 360 Dashboard	Which customers generate the most value, and which customers are at risk of churn?
Product Performance Dashboard	Which products and categories drive sales, margins, and conversions?
Seller Performance Dashboard	Which sellers contribute the most revenue, and how do their delivery and return metrics compare?

The dashboards support filtering and analysis across relevant dimensions, including region, sales channel, product category, customer segment, and seller tier.

Genie provides a natural-language interface for asking questions about the available analytical data.

⸻

Setup and Execution Guide

Prerequisites

Before running the notebooks, you need:

* Access to a Databricks workspace.
* A Unity Catalog-enabled environment with the required permissions.
* Access to the source CSV datasets used by the project.
* Permission to create catalogs, schemas, volumes, tables, and jobs where required.
* A compatible compute environment or serverless resources supported by your workspace.

Some features may require permissions or workspace capabilities that are not available in every Databricks edition.

Step 1: Import the Source Dataset Files

Important: The source datasets must be available in your own Databricks environment before you run the ingestion pipeline.

The project expects 10 CSV datasets:

1. customers
2. sellers
3. products
4. campaigns
5. orders
6. order_items
7. payments
8. reviews
9. inventory
10. clickstream

Upload the corresponding CSV files to your Databricks workspace and place them in the appropriate Unity Catalog Volume folders.

The Bronze ingestion notebooks expect the landing paths to follow this pattern:

/Volumes/e_commerce/bronze/raw_data/<table>/

For example, the customers dataset should be placed under the corresponding customers landing folder.

The exact Volume path is determined by the catalog, schema, volume, and table-folder naming conventions used by the notebooks.

Important: If your catalog or Volume has a different name, update the configuration and paths in the notebooks accordingly.

The source files should use CSV format with a header row and the expected column names and data structure.

Step 2: Create or Configure the Unity Catalog Objects

The original project uses the catalog:

e_commerce

The architecture organizes data into Bronze, Silver, Gold, and supporting governance objects.

Before execution, ensure that the catalog, required schemas, and landing and checkpoint Volumes exist.

If you are using a different catalog name, update the project configuration and all dependent references consistently.

You must also configure the required permissions for reading source files and creating or modifying Delta tables.

Step 3: Import the Notebooks

Import the project notebooks into your Databricks workspace.

Keep them organized by processing layer:

* bronze/ — ingestion notebooks.
* silver/ — cleaning and incremental transformation notebooks.
* gold/ — dimensions, fact table, and analytical marts.
* governance/ — access controls, masks, and row filters.
* maintenance/ — optimization and recovery operations.

Importing notebooks does not automatically recreate the catalog, Volumes, tables, permissions, jobs, dashboards, or source data.

Step 4: Configure Paths and Dependencies

Review the configuration notebook and shared helper functions before execution.

Verify:

* Catalog and schema names.
* Source landing paths.
* Checkpoint and schema-tracking paths.
* Table names and dependencies.
* Required widgets and parameters.
* Permissions for Change Data Feed and MERGE operations.
* Governance group names and access policies.

Make sure the configuration matches your own workspace.

Step 5: Run the Pipeline in Order

For an initial execution, follow this general sequence:

1. Upload the source CSV files to the expected landing folders.
2. Run Bronze ingestion to create the raw Delta tables.
3. Run the Silver cleaning and validation notebooks.
4. Build the Gold dimension tables.
5. Build the Gold sales fact table.
6. Generate the five analytical marts.
7. Configure and validate governance policies.
8. Run the data quality validation tasks.
9. Execute maintenance tasks where appropriate.
10. Configure the Lakeflow Job and dashboard refresh tasks.

Run upstream dependencies before downstream notebooks.

For subsequent source updates, place new CSV batches in the expected landing folders and execute the configured incremental workflow.

Note: This repository documents the expected execution flow. Exact notebook names, task parameters, and execution dependencies should be verified against the imported project files.

Step 6: Validate the Results

After execution, confirm that:

* All 10 Bronze tables have been created.
* Silver tables contain validated and transformed data.
* Quarantine tables capture rejected records.
* SCD Type 2 tables maintain the expected history.
* Gold fact and dimension tables have the expected relationships.
* Analytical marts contain the required measures.
* Governance filters and masks behave as expected.
* Dashboards can query the Gold-layer tables.

⸻

Repository Structure

A recommended organization for this project is:

ecommerce-databricks-lakehouse/
│
├── README.md
│
├── notebooks/
│   ├── bronze/
│   │   └── bronze_ingestion
│   │
│   ├── silver/
│   │   ├── customers_clean
│   │   ├── sellers_clean
│   │   ├── products_clean
│   │   ├── campaign_clean
│   │   ├── orders_clean
│   │   ├── order_items_clean
│   │   ├── payments_clean
│   │   ├── reviews_clean
│   │   ├── inventory_clean
│   │   └── clickstream_clean
│   │
│   ├── gold/
│   │   ├── gold_config
│   │   ├── gold_dim_date
│   │   ├── gold_dim_customer
│   │   ├── gold_dim_product
│   │   ├── gold_dim_seller
│   │   ├── gold_dim_campaign
│   │   ├── gold_fact_sales
│   │   └── analytical_marts/
│   │
│   ├── governance/
│   │   └── governance_setup
│   │
│   └── maintenance/
│       └── delta_maintenance
│
├── docs/
│   └── architecture.png
│
└── .gitignore

This is a suggested repository structure. Adjust the filenames and folders to match the notebooks actually included in your repository.

⸻

Expected Outcomes

The completed project demonstrates how to:

* Build a multi-layer lakehouse using Delta Lake.
* Ingest files incrementally with Auto Loader.
* Implement reusable data quality and quarantine logic.
* Apply CDF-based incremental processing.
* Maintain historical records with SCD Type 2.
* Model business data using a star schema.
* Create analytical data marts for multiple business functions.
* Apply data governance using Unity Catalog.
* Orchestrate data pipelines using Lakeflow Jobs.
* Develop dashboards for business decision-making.

⸻

Limitations and Reproducibility

The notebooks alone may not be sufficient to reproduce the entire environment.

Execution depends on access to the required source datasets, a compatible Databricks workspace, Unity Catalog objects, compute resources, permissions, and any additional project assets.

The original catalog and Volume names may need to be adapted to your environment.

Dashboards, Genie configuration, job definitions, permissions, and other workspace resources may require separate setup.

Do not include credentials, access tokens, restricted datasets, or other confidential information in the repository.

⸻

Conclusion

This project demonstrates an end-to-end data engineering workflow that transforms raw e-commerce data into governed, analytics-ready datasets using Databricks.

By combining the Medallion Architecture, Delta Lake, incremental processing, dimensional modeling, data quality controls, and business intelligence, the lakehouse provides a foundation for customer, product, seller, inventory, and sales analytics.
