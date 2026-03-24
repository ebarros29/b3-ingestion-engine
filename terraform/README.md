# B3 Ingestion Engine - Terraform Infrastructure

Automated infrastructure for the B3 Stock Market Data Ingestion and Processing Engine. This Terraform configuration deploys a serverless ETL pipeline on AWS using S3, Lambda, and AWS Glue.

## Architecture Overview

This infrastructure implements a serverless ETL pipeline that automatically processes B3 stock market data.

![Data Pipeline](../docs/glue_data_pipe.png)

## AWS Resources

### S3 Buckets

**Raw Bucket** (`b3-ingestion-engine-raw-bucket`)
- Landing zone for raw B3 stock data files (Parquet format)
- Triggers Lambda function when files are uploaded to `/raw/` prefix
- Also stores Glue scripts, logs, and temporary files

**Refined Bucket** (`b3-ingestion-engine-refined-bucket`)
- Stores processed and curated data
- Partitioned by `Date` and `ticker_cod`
- Format: Snappy-compressed Parquet

### Lambda Function (`b3-ingestion-engine-trigger-glue`)

- **Runtime**: Python 3.14
- **Purpose**: Receives S3 events and triggers Glue job execution
- **Source**: `./src/app.py`

### AWS Glue Job (`b3-etl-job`)

Transforms raw B3 data into refined analytics-ready format.

**Processing Pipeline**:
1. Reads parquet files from raw bucket
2. Aggregates by ticker and date (sums closing prices)
3. Calculates historical min/max per ticker
4. Validates data quality
5. Writes partitioned Parquet to refined bucket
6. Updates Glue Data Catalog

**Configuration**:
- Glue Version: 4.0
- Worker Type: G.1X (4 vCPU, 16 GB memory)
- Number of Workers: 2
- Script: `./glue-scripts/b3_etl_script.py`

### Glue Data Catalog

**Database**: `b3-etl-job-database`
**Table**: `b3_stock_refined`
- Automatically updated with new partitions
- Queryable via Athena or Redshift Spectrum

## Data Flow

1. Upload raw B3 data (Parquet) to `s3://{raw_bucket}/raw/`
2. S3 event triggers Lambda function
3. Lambda starts Glue job execution
4. Glue job processes data with Spark transformations
5. Refined data written to `s3://{refined_bucket}/refined/`
6. Glue Data Catalog updated
7. Data ready for querying

## Prerequisites

- Terraform >= 1.7.5
- AWS CLI configured with credentials
- IAM permissions for S3, Lambda, Glue, IAM, and CloudWatch

## Provisioning Infrastructure

### 1. Initialize Terraform

```bash
cd terraform/
terraform init
```

### 2. Configure Variables

Update `terraform.tfvars` file (if you want to change):

```hcl
service_name   = "b3-ingestion-engine"
glue_job_name  = "b3-etl-job"
```

### 3. Preview Changes

```bash
terraform plan -var-file=terraform.tfvars
```

### 4. Deploy

```bash
terraform apply -var-file=terraform.tfvars
```

Type `yes` when prompted.

### 5. Verify

```bash
# Check outputs
terraform output

# Test the pipeline
aws s3 cp your-data.parquet s3://$(terraform output -raw raw_bucket_name)/raw/

# Monitor Glue job
aws glue get-job-runs --job-name $(terraform output -raw glue_job)
```

## Outputs

| Output                     | Description                              |
|----------------------------|------------------------------------------|
| raw_bucket_name            | Raw data S3 bucket name                  |
| refined_bucket_name        | Refined data S3 bucket name              |
| trigger_glue_function_name | Lambda function name                     |
| glue_job                   | Glue job name                            |
| glue_database              | Glue catalog database name               |

## Querying Data with Athena

Once the pipeline processes data, you can query it using AWS Athena.

### Example Queries

**View all data for a specific date:**
```sql
SELECT *
FROM "b3-etl-job-database"."b3_stock_refined"
WHERE date = '2026-03-23'
LIMIT 10;
```

**Get latest closing prices by ticker:**
```sql
SELECT ticker_cod, date, close, maxima_historica, minima_historica
FROM "b3-etl-job-database"."b3_stock_refined"
WHERE date = (SELECT MAX(date) FROM "b3-etl-job-database"."b3_stock_refined")
ORDER BY ticker_cod;
```

**Find tickers near historical highs (within 5%):**
```sql
SELECT ticker_cod, date, close, maxima_historica,
       ROUND((close / maxima_historica) * 100, 2) as pct_of_max
FROM "b3-etl-job-database"."b3_stock_refined"
WHERE date = (SELECT MAX(date) FROM "b3-etl-job-database"."b3_stock_refined")
  AND (close / maxima_historica) >= 0.95
ORDER BY pct_of_max DESC;
```

**Analyze price ranges for specific ticker:**
```sql
SELECT ticker_cod, date, close, maxima_historica, minima_historica,
       ROUND(maxima_historica - minima_historica, 2) as range_historico
FROM "b3-etl-job-database"."b3_stock_refined"
WHERE ticker_cod = 'PETR4'
ORDER BY date DESC
LIMIT 30;
```

**Compare current price to historical average:**
```sql
SELECT ticker_cod, date, close,
       ROUND((maxima_historica + minima_historica) / 2, 2) as media_historica,
       ROUND(close - (maxima_historica + minima_historica) / 2, 2) as diff_media
FROM "b3-etl-job-database"."b3_stock_refined"
WHERE date = (SELECT MAX(date) FROM "b3-etl-job-database"."b3_stock_refined")
ORDER BY ticker_cod;
```

## Cleanup

```bash
terraform destroy
```

## Configuration

### Variables

| Variable       | Description                              | Required |
|---------------|------------------------------------------|----------|
| service_name  | Base name for all resources              | Yes      |
| glue_job_name | Name of the AWS Glue ETL job             | Yes      |

### Customization Examples

**Change AWS Region** - Edit `providers.tf`:
```hcl
provider "aws" {
  region = "us-west-2"
}
```

**Adjust Glue Performance** - Edit `glue.tf`:
```hcl
worker_type       = "G.2X"  # Options: G.1X, G.2X, G.4X, G.8X
number_of_workers = 5        # Scale based on data volume
```

**Update Glue Script**:
```bash
vim glue-scripts/b3_etl_script.py
terraform apply
```
