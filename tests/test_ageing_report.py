import unittest
from datetime import date

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual

from src.config.models import AppConfig
from src.pipeline.ageing_report import AgeingReportPipeline
from src.utils.spark_utils import load_config, read_input_file


class TestAgeingReportPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Creates a spark session that is optimized for unit tests."""
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("local-tests")
            .config("spark.executor.cores", "1")
            .config("spark.executor.instances", "1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
            .getOrCreate()
        )

        config_path = "config/config_dev.yaml"
        config_dict = load_config(config_path)

        cls.config = AppConfig(**config_dict)
        cls.pipeline = AgeingReportPipeline(cls.spark, cls.config)
        cls.invoices_df, cls.credit_notes_df, cls.payments_df = cls.create_sample_data()

    @classmethod
    def create_sample_data(cls) -> tuple[DataFrame, DataFrame, DataFrame]:
        # Invoices data
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

        invoices_data = [
            ("inv_001", "c_01", "cls_01", "stu_001", date(2025, 5, 1), 300.00),
            ("inv_008", "c_01", "cls_02", "stu_008", date(2025, 6, 20), 100.00),
        ]

        # Credit notes data
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

        credit_notes_data = [
            ("cr_001", "c_01", "cls_01", "stu_001", date(2025, 5, 15), 100.00),
            ("cr_008", "c_01", "cls_02", "stu_008", date(2025, 4, 25), 90.00),
        ]

        # Payments data
        payments_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("document_id", StringType(), True),
                StructField("document_type", StringType(), True),
                StructField("amount_paid", FloatType(), True),
                StructField("payment_date", DateType(), True),
            ]
        )

        payments_data = [
            ("pay_001", "inv_001", "invoice", 150.00, date(2025, 5, 10)),
            ("pay_008", "inv_005", "invoice", 50.00, date(2025, 3, 1)),
        ]

        invoices_df = cls.spark.createDataFrame(invoices_data, invoices_schema)
        credit_notes_df = cls.spark.createDataFrame(
            credit_notes_data, credit_notes_schema
        )
        payments_df = cls.spark.createDataFrame(payments_data, payments_schema)

        return invoices_df, credit_notes_df, payments_df

    @classmethod
    def tearDownClass(cls):
        """Tear down test class"""
        cls.spark.stop()

    def test_union_documents(self):
        """Test document union"""
        # Act
        self.pipeline.register_temp_tables(
            self.invoices_df, self.credit_notes_df, self.payments_df
        )
        result_df = self.pipeline.union_documents()

        # Assert
        self.assertGreater(result_df.count(), 0)
        self.assertIn("document_type", result_df.columns)
        self.assertIn("document_id", result_df.columns)

        # Check document types
        doc_types = [
            row.document_type
            for row in result_df.select("document_type").distinct().collect()
        ]
        self.assertIn("invoice", doc_types)
        self.assertIn("credit_note", doc_types)

    def test_calculate_outstanding_amounts(self):
        """Test outstanding amount calculation"""
        # Act
        self.pipeline.register_temp_tables(
            self.invoices_df, self.credit_notes_df, self.payments_df
        )
        self.pipeline.union_documents()
        result_df = self.pipeline.calculate_outstanding_amounts()

        # Assert
        self.assertGreater(result_df.count(), 0)
        self.assertIn("outstanding_amount", result_df.columns)

        # All outstanding amounts should be > 0
        negative_amounts = result_df.filter(result_df.outstanding_amount <= 0).count()
        self.assertEqual(negative_amounts, 0)

    def test_apply_ageing_buckets(self):
        """Test ageing bucket application"""
        # Act
        self.pipeline.register_temp_tables(
            self.invoices_df, self.credit_notes_df, self.payments_df
        )

        self.pipeline.union_documents()
        self.pipeline.calculate_outstanding_amounts()

        as_at_date = date(2025, 4, 7)
        result_df = self.pipeline.apply_ageing_buckets(as_at_date)

        # Assert
        self.assertGreater(result_df.count(), 0)

        # Check bucket columns
        bucket_columns = [
            "day_30",
            "day_60",
            "day_90",
            "day_120",
            "day_150",
            "day_180",
            "day_180_and_above",
        ]
        for col_name in bucket_columns:
            self.assertIn(col_name, result_df.columns)

    def test_full_pipeline(self):
        """Test complete pipeline execution"""
        as_at_date = date(2025, 4, 7)
        self.pipeline.generate_ageing_report(as_at_date)

        result_schema = StructType(
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
                StructField("as_at_date", DateType(), False),
            ]
        )

        output_file_path = f"{self.config.pipeline.outputs.path}_{as_at_date.strftime('%Y-%m-%d')}.{self.config.pipeline.outputs.format}"
        result_df = read_input_file(
            spark=self.spark,
            file_format=self.config.pipeline.outputs.format,
            file_path=output_file_path,
            schema=result_schema,
        )

        expected_df = (
            self.spark.createDataFrame(
                [
                    {
                        "centre_id": "0_02",
                        "class_id": "cls_03",
                        "document_id": "cr_003",
                        "document_date": date(2024, 12, 1),
                        "student_id": "stu_003",
                        "day_30": 0.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 300.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "credit_note",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "0_03",
                        "class_id": "c15_03",
                        "document_id": "cr_006",
                        "document_date": date(2025, 2, 28),
                        "student_id": "stu_006",
                        "day_30": 0.0,
                        "day_60": 80.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "credit_note",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "c_01",
                        "class_id": "cls_01",
                        "document_id": "cr_001",
                        "document_date": date(2025, 5, 15),
                        "student_id": "5tu_001",
                        "day_30": 0.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "credit_note",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "c_01",
                        "class_id": "cls_01",
                        "document_id": "inv_001",
                        "document_date": date(2025, 5, 1),
                        "student_id": "stu_001",
                        "day_30": 0.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "invoice",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "c_03",
                        "class_id": "cls_01",
                        "document_id": "inv_007",
                        "document_date": date(2025, 3, 20),
                        "student_id": "stu_007",
                        "day_30": 300.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "invoice",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "c_03",
                        "class_id": "cls_02",
                        "document_id": "cr_005",
                        "document_date": date(2025, 6, 1),
                        "student_id": "5tu_005",
                        "day_30": 0.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 0.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "credit_note",
                        "as_at_date": date(2025, 4, 7),
                    },
                    {
                        "centre_id": "c_03",
                        "class_id": "cls_03",
                        "document_id": "inv_004",
                        "document_date": date(2024, 12, 15),
                        "student_id": "stu_004",
                        "day_30": 0.0,
                        "day_60": 0.0,
                        "day_90": 0.0,
                        "day_120": 300.0,
                        "day_150": 0.0,
                        "day_180": 0.0,
                        "day_180_and_above": 0.0,
                        "document_type": "invoice",
                        "as_at_date": date(2025, 4, 7),
                    },
                ]
            )
            .withColumn("day_30", F.col("day_30").cast(DoubleType()))
            .withColumn("day_60", F.col("day_60").cast(DoubleType()))
            .withColumn("day_90", F.col("day_90").cast(DoubleType()))
            .withColumn("day_120", F.col("day_120").cast(DoubleType()))
            .withColumn("day_150", F.col("day_150").cast(DoubleType()))
            .withColumn("day_180", F.col("day_180").cast(DoubleType()))
            .withColumn(
                "day_180_and_above", F.col("day_180_and_above").cast(DoubleType())
            )
        )

        column_names = expected_df.columns
        assertDataFrameEqual(
            result_df.select(*column_names), expected_df.select(column_names)
        )


if __name__ == "__main__":
    unittest.main()
