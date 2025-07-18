import os
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config.models import AppConfig


def create_spark_session(config: AppConfig) -> SparkSession:
    """Create Spark session with configuration"""

    builder = SparkSession.builder.appName(config.spark.app_name)

    # Apply Spark configurations
    builder = builder.config(map=config.spark.config)

    return builder.getOrCreate()


def load_config(config_path: str = "config/config_dev.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file"""

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def read_sql_file(file_path: str) -> str:
    """Read SQL file content"""

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, "r") as file:
        return file.read()


def read_input_file(
    spark: SparkSession, file_path: str, file_format: str, schema: StructType
) -> DataFrame:
    if file_format == "csv":
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(file_path)
        )
    else:
        raise ValueError(f"Input with format {file_format} doesn't support!")

    return df


def write_output_file(output: DataFrame, file_path: str, file_format: str):
    if file_format == "csv":
        output.toPandas().to_csv(file_path, header=True, index=False)
    else:
        raise ValueError(f"Output with format {file_format} doesn't support!")
