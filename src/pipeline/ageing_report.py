from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, FloatType, StringType, StructField, StructType

from src.config.models import AppConfig
from src.pipeline.data_validator import DataValidator
from src.utils.logger import get_logger
from src.utils.spark_utils import read_input_file, read_sql_file, write_output_file

logger = get_logger(__name__)


class AgeingReportPipeline:
    def __init__(self, spark_session, config: AppConfig):
        self.spark = spark_session
        self.config = config
        self.validator = DataValidator(spark_session)

    def extract(self):
        input_configs = self.config.pipeline.inputs
        invoices_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("centre_id", StringType(), True),
                StructField("class_id", StringType(), True),
                StructField("student_id", StringType(), True),
                StructField("invoice_date", DateType(), True),
                StructField("total_amount", FloatType(), True),
            ]
        )

        invoices = read_input_file(
            spark=self.spark,
            file_format=input_configs.invoices.format,
            file_path=input_configs.invoices.path,
            schema=invoices_schema,
        )

        credit_notes_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("centre_id", StringType(), True),
                StructField("class_id", StringType(), True),
                StructField("student_id", StringType(), True),
                StructField("credit_note_date", DateType(), True),
                StructField("total_amount", FloatType(), True),
            ]
        )
        credit_notes = read_input_file(
            spark=self.spark,
            file_format=input_configs.credit_notes.format,
            file_path=input_configs.credit_notes.path,
            schema=credit_notes_schema,
        )

        payments_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("document_id", StringType(), True),
                StructField("document_type", StringType(), True),
                StructField("amount_paid", FloatType(), True),
                StructField("payment_date", DateType(), True),
            ]
        )
        payments = read_input_file(
            spark=self.spark,
            file_format=input_configs.payments.format,
            file_path=input_configs.payments.path,
            schema=payments_schema,
        )

        # Validate input data
        self.validator.validate_inputs(invoices, credit_notes, payments)
        # Register temporary tables
        self.register_temp_tables(invoices, credit_notes, payments)

    def register_temp_tables(
        self, invoices: DataFrame, credit_notes: DataFrame, payments: DataFrame
    ):
        """Register DataFrames as temporary tables"""
        logger.info("Registering temporary tables...")

        invoices.createOrReplaceTempView("invoices")
        credit_notes.createOrReplaceTempView("credit_notes")
        payments.createOrReplaceTempView("payments")

        logger.info("Temporary tables registered successfully")

    def union_documents(self) -> DataFrame:
        """Union invoices and credit notes"""
        logger.info("Unioning documents...")

        sql_query = read_sql_file("src/sql/union_documents.sql")
        result_df = self.spark.sql(sql_query)

        # Register as temp table for next step
        result_df.createOrReplaceTempView("all_documents")

        logger.info("Documents union completed.")

        return result_df

    def calculate_outstanding_amounts(self) -> DataFrame:
        """Calculate outstanding amounts"""

        logger.info("Calculating outstanding amounts")

        sql_query = read_sql_file("src/sql/calculate_outstanding.sql")
        result_df = self.spark.sql(sql_query)

        # Register as temp table for next step
        result_df.createOrReplaceTempView("outstanding_documents")

        logger.info("Outstanding amounts calculated.")

        return result_df

    def apply_ageing_buckets(self, as_at_date: date) -> DataFrame:
        """Apply ageing buckets"""
        logger.info(f"Applying ageing buckets for as_at_date: {as_at_date}")

        sql_query = read_sql_file("src/sql/apply_ageing_buckets.sql")
        # Replace placeholder with actual as_at_date
        sql_query = sql_query.format(as_at_date=as_at_date)

        result_df = self.spark.sql(sql_query)

        logger.info("Ageing buckets applied")

        return result_df

    def transform(
        self,
        as_at_date: date,
    ) -> DataFrame:
        self.union_documents()
        self.calculate_outstanding_amounts()
        self.ageing_report = self.apply_ageing_buckets(as_at_date)

    def load(self, as_at_date: date):
        self.validator.validate_outputs(self.ageing_report)
        output_configs = self.config.pipeline.outputs
        output_file_path = f"{output_configs.path}_{as_at_date.strftime('%Y-%m-%d')}.{output_configs.format}"
        logger.info(f"Write ageing_report to file: {output_file_path}")

        write_output_file(
            output=self.ageing_report,
            file_format=output_configs.format,
            file_path=output_file_path,
        )

    def generate_ageing_report(
        self,
        as_at_date: date,
    ) -> DataFrame:
        self.extract()
        self.transform(as_at_date=as_at_date)
        self.load(as_at_date=as_at_date)
