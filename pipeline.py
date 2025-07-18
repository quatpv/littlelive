import argparse
from datetime import datetime
from src.pipeline.ageing_report import AgeingReportPipeline
from src.utils.logger import setup_logging, get_logger
from src.utils.spark_utils import create_spark_session, load_config, read_input_file
from src.config.models import AppConfig
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    DoubleType,
)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run the Ageing Report Pipeline")
    parser.add_argument(
        "--as-at-date",
        type=str,
        required=True,
        help="As-at date for the ageing report in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--env",
        type=str,
        required=True,
        choices=["dev", "prod"],
        help="Environment to run the pipeline (dev, prod)",
    )
    return parser.parse_args()


def validate_date(date_string):
    """Validate and parse date string"""
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError(
            f"Invalid date format: {date_string}. Expected format: YYYY-MM-DD"
        )


def main():
    """Main function to run the aging report pipeline"""
    try:
        # Parse command line arguments
        args = parse_arguments()

        # Validate and parse as-at date
        as_at_date = validate_date(args.as_at_date)
        env = args.env

        # Load configuration
        config_path = f"config/config_{env}.yaml"
        config_dict = load_config(config_path)
        config = AppConfig(**config_dict)

        logger = setup_logging(
            level=config.logging.level, format_str=config.logging.format
        )
        logger = get_logger(__name__)
        logger.info(f"Starting Ageing Report Pipeline for environment: {env}")
        logger.info(f"As-at date: {as_at_date}")

        # Create Spark session
        spark = create_spark_session(config)

        # Initialize pipeline
        pipeline = AgeingReportPipeline(spark, config)

        # Run the pipeline
        pipeline.generate_ageing_report(as_at_date)

        ageing_report_schema = StructType(
            [
                StructField("centre_id", StringType(), True),
                StructField("class_id", StringType(), True),
                StructField("document_id", StringType(), True),
                StructField("document_date", DateType(), True),
                StructField("student_id", StringType(), True),
                StructField("day_30", DoubleType(), True),
                StructField("day_60", DoubleType(), True),
                StructField("day_90", DoubleType(), True),
                StructField("day_120", DoubleType(), True),
                StructField("day_150", DoubleType(), True),
                StructField("day_180", DoubleType(), True),
                StructField("day_180_and_above", DoubleType(), True),
                StructField("document_type", StringType(), False),
                StructField("as_at_date", StringType(), False),
            ]
        )

        ageing_report = read_input_file(
            spark=spark,
            file_format=config.pipeline.outputs.format,
            file_path=f"{config.pipeline.outputs.path}_{as_at_date.strftime('%Y-%m-%d')}.{config.pipeline.outputs.format}",
            schema=ageing_report_schema,
        )

        # Show results
        logger.info("Pipeline completed successfully")
        logger.info(
            f"Generated aging report with {ageing_report.count()} records and saved to file: {config.pipeline.outputs.path}"
        )

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        # Clean up Spark session
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
