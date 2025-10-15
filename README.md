# Data Retention Offloader

A complete archival pipeline to offload aged tables from relational databases to an Iceberg-backed data lake using AWS Glue, Airflow (MWAA), and Lambda.

## Project Structure


## What It Does

- Discovers tables in source databases that match a retention policy.
- Offloads tables to Iceberg format using AWS Glue with full lineage.
- Automates orchestration via Apache Airflow running on MWAA.
- Supports onboarding of new databases via simple YAML config.
- Deployable via a single script (`scripts/deploy.sh`).

## Components

### 1. Glue Job

- Script: `glue_jobs/archive_table.py`
- Copies tables to Iceberg format in S3.
- Uses JDBC connection and Secrets Manager for authentication.
- Automatically creates Glue metadata (catalog, database, tables).

### 2. Airflow DAG

- File: `airflow/dags/archive_tables.py`
- Discovers eligible tables and launches per-table offload Glue jobs.

### 3. Lambda Functions

- `discover_tables_lambda`: Finds archival candidates and triggers DAG runs.
- `s3_yaml_to_mwaa`: Triggers DAGs on YAML upload to S3.
- Both share an IAM role and are packaged using `scripts/deploy_lambda.sh`.

## Deployment

Run the main deployment script:

```bash
./scripts/deploy.sh
```

This will:

- Upload Airflow DAGs to S3
- Deploy required Lambda functions
- Set up and upload the Glue job
- Optionally deploy MWAA infrastructure (can be uncommented in deploy.sh)

## Onboard a New Database
Use the following script to register a new JDBC database:

```bash
./scripts/onboard_database.sh \
  --connection-name nucleus_dev \
  --jdbc-url jdbc:postgresql://host:port/db \
  --subnet-ids "subnet-abc,subnet-def" \
  --security-group-id sg-xyz
```
This will:

- Create a secret in AWS Secrets Manager for DB credentials
- Create or update a Glue connection with VPC and networking details

## Lake Formation Governance
Run `scripts/manage_lakeformation_governance.py` to register archive storage with Lake Formation, tag the Glue catalog entries, and grant consumer access:

```bash
./scripts/manage_lakeformation_governance.py \
  --bucket my-archive-bucket \
  --prefix warehouse/prod \
  --glue-database archive_orders \
  --consumer-role-arn arn:aws:iam::111122223333:role/data-analytics-readonly
```
