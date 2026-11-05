"""Ingest book recommendation CSV files into Bronze Delta tables."""

import argparse
import logging
from enum import Enum

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class BronzeTables(Enum):
    """Names of the supported Bronze Delta tables."""

    BOOKS = "books_raw"
    RATINGS = "ratings_raw"
    USERS = "users_raw"


BOOKS_SCHEMA = StructType([
    StructField("ISBN", StringType(), True),
    StructField("Book-Title", StringType(), True),
    StructField("Book-Author", StringType(), True),
    StructField("Year-Of-Publication", StringType(), True),
    StructField("Publisher", StringType(), True),
    StructField("Image-URL-S", StringType(), True),
    StructField("Image-URL-M", StringType(), True),
    StructField("Image-URL-L", StringType(), True)
])

RATINGS_SCHEMA = StructType([
    StructField("User-ID", StringType(), True),
    StructField("ISBN", StringType(), True),
    StructField("Book-Rating", StringType(), True)
])

USERS_SCHEMA = StructType([
    StructField("User-ID", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Age", StringType(), True)
])


SCHEMAS = {BronzeTables.BOOKS.value: BOOKS_SCHEMA,
           BronzeTables.RATINGS.value: RATINGS_SCHEMA,
           BronzeTables.USERS.value: USERS_SCHEMA}


def autoloader(spark: SparkSession, schema: StructType, dataset_dir: str, checkpoint_dir: str, schema_dir: str, full_table_reference: str):
    """Stream CSV data into a Delta table with Databricks Auto Loader.

    Args:
        spark: Active Spark session used for the streaming read and write.
        schema: Explicit schema for the input CSV files.
        dataset_dir: Input directory watched by Auto Loader.
        checkpoint_dir: Streaming checkpoint directory.
        schema_dir: Directory where Auto Loader stores schema metadata.
        full_table_reference: Fully qualified destination table name.
    """

    schema_hints = ", ".join(
        f"`{field.name}` {field.dataType.simpleString()}" for field in schema.fields)
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("header", "true")
              .option("sep", ",")
              .option("cloudFiles.schemaLocation", schema_dir)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("rescuedDataColumn", "_rescued_data")
              .option("cloudFiles.schemaHints", schema_hints)
              )
    df = (reader.load(dataset_dir)
          .select("*",
                  F.col("_metadata.file_path").alias("_source_file_path"),
                  F.col("_metadata.file_modification_time").alias(
                      "_source_file_modification_time"),
                  F.current_timestamp().alias("_ingestion_time")))
    writer = (df.writeStream.option("checkpointLocation", checkpoint_dir)
              .trigger(availableNow=True)
              .toTable(full_table_reference)
              )
    writer.awaitTermination()
    logger.info(f"Streaming to Delta table {full_table_reference} completed.")


def cli():
    """Parse command-line arguments and run the selected Bronze ingestion."""

    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-dir", type=str,
                        required=True, help="Path to the input data")
    parser.add_argument("--catalog", type=str, required=True,
                        help="Catalog name for the Delta table")
    parser.add_argument("--schema", type=str, required=True,
                        help="Schema for the Delta table")
    parser.add_argument("--table", type=str, required=True,
                        choices=[table.value for table in BronzeTables],
                        help="Name of the Delta table")
    parser.add_argument("--checkpoint-dir", type=str,
                        required=True, help="Path to the checkpoint directory")
    parser.add_argument("--schema-dir", type=str, required=True,
                        help="Path to the schema directory")
    args = parser.parse_args()

    if args.table == BronzeTables.BOOKS.value:
        schema = BOOKS_SCHEMA
    elif args.table == BronzeTables.RATINGS.value:
        schema = RATINGS_SCHEMA
    elif args.table == BronzeTables.USERS.value:
        schema = USERS_SCHEMA

    spark = SparkSession.builder.getOrCreate()
    autoloader(spark, schema, args.dataset_dir, args.checkpoint_dir,
               args.schema_dir, f"{args.catalog}.{args.schema}.{args.table}")


if __name__ == "__main__":
    cli()
