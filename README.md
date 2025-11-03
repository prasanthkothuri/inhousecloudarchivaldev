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

Lake Formation policies keep archived data controlled while allowing downstream read-only access.

### 1. One-time resource registration

Register the archive prefix (or the whole bucket) with Lake Formation so data lake admins can manage anything stored beneath it:

```bash
aws lakeformation register-resource \
  --region eu-west-1 \
  --use-service-linked-role \
  --resource-arn arn:aws:s3:::natwest-data-archive-vault/iceberg
```

### 2. Per archive database

For every Glue database produced by an archival run (for example `archive_onprem_orcl_conn`, `archive_bizops_orcl_conn`):

```bash
# Ensure OpsAdmin stays a LF admin and grant DESCRIBE on the catalog
python3 ./scripts/manage_lakeformation_governance.py \
  --bucket natwest-data-archive-vault \
  --prefix iceberg \
  --glue-database <archive_db_name> \
  --consumer-role-arn arn:aws:iam::934336705194:role/ADFS-DOC-DOGB333 \
  --permissions DESCRIBE \
  --data-lake-admin-arns arn:aws:iam::934336705194:role/ADFS-DOC-OpsAdminRole

# Allow the consumer role to query every table in that Glue database
aws lakeformation grant-permissions \
  --region eu-west-1 \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::934336705194:role/ADFS-DOC-DOGB333 \
  --resource '{"Table":{"CatalogId":"934336705194","DatabaseName":"<archive_db_name>","TableWildcard":{}}}' \
  --permissions SELECT
```

### 3. Verification

Confirm the permissions after each run:

```bash
aws lakeformation list-permissions \
  --region eu-west-1 \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::934336705194:role/ADFS-DOC-DOGB333 \
  --resource '{"Database":{"CatalogId":"934336705194","Name":"<archive_db_name>"}}'

aws lakeformation list-permissions \
  --region eu-west-1 \
  --principal DataLakePrincipalIdentifier=arn:aws:iam::934336705194:role/ADFS-DOC-DOGB333 \
  --resource '{"Table":{"CatalogId":"934336705194","DatabaseName":"<archive_db_name>","TableWildcard":{}}}'
```

These steps keep `arn:aws:iam::934336705194:role/ADFS-DOC-OpsAdminRole` as the data lake administrator for the archive while limiting `arn:aws:iam::934336705194:role/ADFS-DOC-DOGB333` to read-only access on the target database.
