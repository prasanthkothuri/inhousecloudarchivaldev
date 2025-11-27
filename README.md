# Data Retention Offloader

A complete archival pipeline to offload aged tables from relational databases to an Iceberg-backed data lake using AWS Glue, Airflow (MWAA), and Lambda.

## Project Structure

```
.
├── airflow/
│   └── dags/
│       ├── natwest_data_archival_to_iceberg.py
│       └── natwest_data_purge_from_iceberg.py
├── conf/                    # Configuration files
├── glue_jobs/
│   ├── archive_table.py     # Archive tables to Iceberg
│   ├── purge_table.py       # Purge tables from Iceberg
│   └── validate_table.py    # Validate archived data
├── lambda/
│   ├── natwest_data_archive_discover_tables/
│   ├── natwest_data_archive_presigned_urls/
│   ├── natwest_data_archive_test_conn/
│   ├── natwest_data_archive_validation_report_generation/
│   └── s3_yaml_to_mwaa/
├── scripts/
│   ├── deploy.sh
│   ├── deploy_glue.sh
│   ├── deploy_lambda.sh
│   ├── deploy_mwaa.sh
│   ├── deploy_purge_glue.sh
│   ├── onboard_database.sh
│   └── manage_lakeformation_governance.py
├── utils/
│   └── delete_iceberg_db.sh
└── README.md
```


## What It Does

- Discovers tables in source databases that match a retention policy.
- Offloads tables to Iceberg format using AWS Glue with full lineage.
- Validates archived data integrity through comparison checks.
- Purges data from Iceberg archives when needed.
- Automates orchestration via Apache Airflow running on MWAA.
- Stores configuration in DynamoDB for dynamic workflow management.
- Supports onboarding of new databases via simple YAML config.
- Deployable via a single script (`scripts/deploy.sh`).

## Components

### 1. Glue Jobs

#### Archive Job
- Script: `glue_jobs/archive_table.py`
- Job name: `natwest-data-archive-table-to-iceberg`
- Copies tables to Iceberg format in S3
- Uses JDBC connection and Secrets Manager for authentication
- Automatically creates Glue metadata (catalog, database, tables)

#### Validation Job
- Script: `glue_jobs/validate_table.py`
- Job name: `natwest-archive-data-validation`
- Validates data integrity between source and archived tables
- Compares row counts and sample data checksums

#### Purge Job
- Script: `glue_jobs/purge_table.py`
- Job name: `natwest-data-purge-from-iceberg`
- Removes tables from Iceberg catalog and S3 storage
- Cleans up metadata and physical files

### 2. Airflow DAGs

#### Archival DAG
- File: `airflow/dags/natwest_data_archival_to_iceberg.py`
- Discovers eligible tables via Lambda
- Launches archive Glue jobs per table
- Runs validation after archival
- Generates validation reports

#### Purge DAG
- File: `airflow/dags/natwest_data_purge_from_iceberg.py`
- Orchestrates purging of archived tables from Iceberg
- Triggered via Airflow API or configuration

### 3. Lambda Functions

#### Discovery Lambda
- Function: `natwest_data_archive_discover_tables`
- Finds archival candidates based on retention policies
- Queries DynamoDB for configuration
- Triggers archival DAG runs

#### S3 YAML Trigger
- Function: `s3_yaml_to_mwaa`
- Triggers DAGs when YAML configs are uploaded to S3
- Enables configuration-driven workflows

#### Connection Tester
- Function: `natwest_data_archive_test_conn`
- Tests database connectivity before archival
- Validates JDBC connections and credentials

#### Presigned URL Generator
- Function: `natwest_data_archive_presigned_urls`
- Generates temporary S3 URLs for archived data access
- Provides secure time-limited access to archive files

#### Validation Report Generator
- Function: `natwest_data_archive_validation_report_generation`
- Aggregates validation results
- Creates comprehensive validation reports

### 4. DynamoDB Configuration

- Table: `natwest-archival-configs`
- Stores archival configurations dynamically
- Contains database connection details, retention policies, and scheduling information
- Enables runtime configuration without code changes

## Workflows

### Archival Workflow

1. **Discovery**: Lambda function queries DynamoDB and source databases for eligible tables
2. **Triggering**: Lambda triggers the archival DAG in MWAA
3. **Archival**: DAG spawns Glue jobs per table to copy data to Iceberg format
4. **Validation**: After archival, validation Glue job compares source and archive
5. **Reporting**: Validation report Lambda generates summary reports

### Purge Workflow

1. **Trigger**: Purge DAG is triggered manually or via configuration
2. **Execution**: Purge Glue job removes tables from Iceberg catalog
3. **Cleanup**: Physical files are deleted from S3 storage
4. **Verification**: Metadata is cleaned up from Glue catalog

## Deployment

Run the main deployment script:

```bash
./scripts/deploy.sh
```

This will:

- Upload Airflow DAGs to S3
- Deploy all Lambda functions
- Set up and upload all Glue jobs (archive, validate, purge)
- Optionally deploy MWAA infrastructure (can be uncommented in deploy.sh)

Individual deployment scripts:
- `scripts/deploy_glue.sh` - Deploy archive and validation Glue jobs
- `scripts/deploy_purge_glue.sh` - Deploy purge Glue job
- `scripts/deploy_lambda.sh` - Deploy all Lambda functions
- `scripts/deploy_mwaa.sh` - Deploy MWAA environment

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
- Configure the connection for use by Glue jobs

## Utilities

### Delete Iceberg Database
Remove an entire Iceberg database and its contents:

```bash
./utils/delete_iceberg_db.sh <database-name>
```

This utility:
- Drops the specified Glue database
- Cleans up associated S3 data
- Removes catalog metadata

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
