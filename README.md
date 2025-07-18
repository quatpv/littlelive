# Ageing Report Pipeline

A PySpark-based data pipeline for generating daily ageing reports from financial documents.

## Data Flow & Processing Logic
### Step 1: Data Ingestion
```
Input Sources:
├── invoices.csv (id, centre_id, class_id, student_id, invoice_date, total_amount)
├── credit_notes.csv (id, centre_id, class_id, student_id, credit_note_date, total_amount)
└── payments.csv (id, document_id, document_type, amount_paid, payment_date)
```

### Step 2: Data Transformation Pipeline

1. Union Documents (```union_documents.sql```)
- Combines invoices and credit notes into a unified format
- Standardizes column names (invoice_date → document_date)
- Adds document_type identifier

2. Calculate Outstanding Balances (```calculate_outstanding.sql```)
- Joins documents with payments
- Calculates outstanding amount per document
- Filters out fully paid documents

3. Apply Ageing Buckets (```apply_ageing_buckets.sql```)
- Calculates days between document_date and as_at_date
- Assigns outstanding amounts to appropriate age buckets
- Ensures mutual exclusivity (one bucket per document)

## Features
- Age Bucketing: Categorizes outstanding amounts into predefined age ranges.
- Data Validation: Ensures data quality for input tables and output table.
- Logging: Simple logging for debugging and monitoring.
- Unit Testing: Test coverage for all major components.
- Configuration: YAML configurations for pipeline, logger, ...

## Installation
```bash
# Clone the repository
git clone https://github.com/quatpv/littlelive.git
cd litlelive

# Install dependencies
pip install -r requirements.txt
```

## Project Structure
```shell
litlelive/
├── README.md                       # This documentation
├── pipeline.py                     # Main pipeline execution script
├── pipeline.ipynb                  # Jupyter notebook for development
├── setup.py                        # Package installation configuration
├── pyproject.toml                  # Configs for pytest
├── requirements.txt                # Python dependencies
├── config/                         # Environment configurations
│   ├── config_dev.yaml             # Development environment settings
│   └── config_prod.yaml            # Production environment settings
├── data/                           # Data storage (environment-specific)
│   ├── dev/                        # Development data
│   │   ├── input/                  # Input CSV files for development
│   │   │   ├── invoices.csv
│   │   │   ├── credit_notes.csv
│   │   │   └── payments.csv
│   │   └── output/                 # Generated reports for development
│   │       └── ageing_report_{as_at_date}.csv
│   └── prod/                       # Production data
│       ├── input/                  # Input CSV files for production
│       │   ├── invoices.csv
│       │   ├── credit_notes.csv
│       │   └── payments.csv
│       └── output/                 # Generated reports for production
│           └── ageing_report_{as_at_date}.csv
├── log/                            # Pipeline logs
│   └── ageing_pipeline.log
├── src/                            # Source code
│   ├── config/                     # Configuration models
│   │   └── models.py               # Pydantic models for configuration
│   ├── pipeline/                   # Core pipeline components
│   │   ├── __init__.py
│   │   ├── ageing_report.py        # Main ageing report logic
│   │   └── data_validator.py       # Data validation utilities
│   ├── sql/                        # SQL query templates
│   │   ├── union_documents.sql        # Combine invoices and credit notes
│   │   ├── calculate_outstanding.sql  # Calculate outstanding balances
│   │   └── apply_ageing_buckets.sql   # Apply age bucket logic
│   └── utils/                         # Utility modules
│       ├── __init__.py
│       ├── logger.py               # Logging configuration
│       └── spark_utils.py          # Spark session management
└── tests/                          # Test
    ├── __init__.py
    └── test_ageing_report.py       # Unit tests for ageing report
```


## How to Run
### 1. Prepare Input Data
Place your CSV files in the data/dev/input/ directory:
- invoices.csv
- credit_notes.csv
- payments.csv

### 2. Configure Pipeline
Edit ```config/config_dev.yaml``` to set inputs, outputs for pipeline:
```yaml
spark:
  app_name: "AgeingReportPipeline"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.execution.arrow.pyspark.enabled: "true"

pipeline:
  inputs:
    invoices:
      format: csv
      path: data/dev/input/invoices.csv
    credit_notes:
      format: csv
      path: data/dev/input/credit_notes.csv
    payments:
      format: csv
      path: data/dev/input/payments.csv
  
  outputs:
    format: csv
    path: data/dev/output/ageing_report

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

To configure the production environment, edit the file ```config/config_prod.yaml```

### 3. Run the Pipeline
There are 2 options to run pipeline:

- Use python script.
```shell
# Run ageing report pipeline for environment dev with as-at-date=2025-07-07
python pipeline.py --as-at-date 2025-07-07 --env dev
```

- Use python notebook in file: ```pipeline.ipynb```

### 4. View Results
The pipeline generates:
- Fact Table: data/dev/output/ageing_report_{as_at_date}.csv
- Log Files: logs/ageing_pipeline.log


## Testing
```shell
# Run all tests
pytest -v -s .
```

## Explanation
> 1. I chose to separate business logic into dedicated SQL files for several key reasons:

- Readability: SQL syntax is more intuitive for complex data transformations.
- Maintainability: Can easily review and modify transformation logic.
- Tooling Support: IDEs provide SQL formatting, syntax highlighting, and validation


> 2. The ```data_validator.py``` module is designed for:

- Framework Integration: Can easily integrate with enterprise data quality frameworks like Great Expectations, SparkDQ.
- Custom Rules: Supports both built-in and custom validation rules.

> 3. The current output format ```ageing_report_{as_at_date}.csv``` is designed for simplicity and demonstration purposes:

- For production environments, I recommend:
```python
# Partitioned Parquet approach
df.write \
  .mode("overwrite") \
  .option("partitionOverwriteMode", "dynamic") \
  .partitionBy("as_at_date") \
  .parquet("output/dev/ageing_report/")
```
- Better Performance: Parquet's columnar format for faster queries
- Partition Pruning: Efficient date-based filtering

## Future Enhancements
- Enhanced configuration management.
- Multi format support.
