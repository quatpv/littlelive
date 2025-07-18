from pydantic import BaseModel, Field


class SparkConfig(BaseModel):
    """
    The 'spark' section of the YAML configuration.
    """

    app_name: str = Field(..., description="The name of the Spark application.")
    config: dict = Field(..., description="Spark session configurations.")


class InputConfig(BaseModel):
    """Defines the structure for an input data source."""

    format: str = Field(..., description="The format of the input data (e.g., 'csv').")
    path: str = Field(..., description="The file path to the input data.")


class InputsConfig(BaseModel):
    """Defines the specific input data sources."""

    invoices: InputConfig
    credit_notes: InputConfig
    payments: InputConfig


class OutputConfig(BaseModel):
    """Defines the structure for the output data destination."""

    format: str = Field(
        ..., description="The format for the output data (e.g., 'csv')."
    )
    path: str = Field(..., description="The destination path for the output data.")


class PipelineConfig(BaseModel):
    """Defines the input and output configurations for the pipeline."""

    inputs: InputsConfig = Field(
        ..., description="The configurations for input data sources."
    )
    outputs: OutputConfig = Field(
        ..., description="The configuration for the pipeline's output."
    )


class LoggingConfig(BaseModel):
    """
    Represents the 'logging' section of the configuration.
    """

    level: str = Field("INFO", description="The logging level (e.g., 'INFO', 'DEBUG').")
    format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="The log message format.",
    )


class AppConfig(BaseModel):
    spark: SparkConfig
    pipeline: PipelineConfig
    logging: LoggingConfig
