import logging
from typing import List

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates input/output dataframes for the ageing report pipeline"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def validate_dataframe(
        self, df: DataFrame, df_name: str, required_columns: List[str]
    ) -> bool:
        """Validate a single dataframe"""

        try:
            logger.info(f"Validating {df_name} dataframe...")

            # Check if dataframe exists and is not empty
            if df is None:
                raise ValueError(f"{df_name} dataframe is None")

            row_count = df.count()
            if row_count == 0:
                raise ValueError(f"{df_name} dataframe is empty")

            logger.info(f"{df_name} row count: {row_count}")

            # Check required columns exist
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                raise ValueError(
                    f"{df_name} missing required columns: {missing_columns}"
                )

            # Check for null values in critical columns
            for col_name in required_columns:
                null_count = df.filter(df[col_name].isNull()).count()
                if null_count > 0:
                    logger.warning(
                        f"{df_name} has {null_count} null values in column '{col_name}'"
                    )

            # Data type validation
            self._validate_data_types(df, df_name)

            logger.info(f"{df_name} validation completed successfully")
            return True

        except Exception as e:
            logger.error(f"Validation failed for {df_name}: {str(e)}")
            raise

    def _validate_data_types(self, df: DataFrame, df_name: str):
        """Validate data types for specific columns"""

        schema = df.schema

        for field in schema.fields:
            if "amount" in field.name.lower():
                if field.dataType.typeName() not in ["decimal", "double", "float"]:
                    logger.warning(
                        f"{df_name}.{field.name} should be numeric type, found: {field.dataType}"
                    )

            if "date" in field.name.lower():
                if field.dataType.typeName() != "date":
                    logger.warning(
                        f"{df_name}.{field.name} should be date type, found: {field.dataType}"
                    )

    def validate_inputs(
        self, invoices: DataFrame, credit_notes: DataFrame, payments: DataFrame
    ) -> bool:
        """Validate all input dataframes"""

        try:
            logger.info("Starting comprehensive data validation...")

            # Define required columns for each dataframe
            invoices_columns = [
                "id",
                "centre_id",
                "class_id",
                "student_id",
                "invoice_date",
                "total_amount",
            ]
            credit_notes_columns = [
                "id",
                "centre_id",
                "class_id",
                "student_id",
                "credit_note_date",
                "total_amount",
            ]
            payments_columns = [
                "id",
                "document_id",
                "document_type",
                "amount_paid",
                "payment_date",
            ]

            # Validate each dataframe
            self.validate_dataframe(invoices, "invoices", invoices_columns)
            self.validate_dataframe(credit_notes, "credit_notes", credit_notes_columns)
            self.validate_dataframe(payments, "payments", payments_columns)

            logger.info("All input validations completed successfully")
            return True

        except Exception as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise

    def validate_outputs(self, ageing_report: DataFrame) -> bool:
        """Validate output dataframes"""

        try:
            logger.info("Starting data output validation...")

            ageing_report_columns = [
                "centre_id",
                "class_id",
                "document_id",
                "document_date",
                "student_id",
                "day_30",
                "day_60",
                "day_90",
                "day_120",
                "day_150",
                "day_180",
                "day_180_and_above",
                "document_type",
                "as_at_date",
            ]

            # Validate each dataframe
            self.validate_dataframe(
                ageing_report, "ageing_report", ageing_report_columns
            )

            logger.info("All output validations completed successfully")
            return True

        except Exception as e:
            logger.error(f"Output validation failed: {str(e)}")
            raise
