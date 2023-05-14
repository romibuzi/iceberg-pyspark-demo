import os

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def init_spark():
    conf = SparkConf() \
        .setAppName("Apache Iceberg with PySpark") \
        .setMaster("local[2]") \
        .setAll([
            ("spark.driver.memory", "1g"),
            ("spark.executor.memory", "2g"),
            ("spark.sql.shuffle.partitions", "40"),

            # Add Iceberg SQL extensions like UPDATE or DELETE in Spark
            ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),

            # Register `my_iceberg_catalog`
            ("spark.sql.catalog.my_iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),

            # Configure SQL connection to track tables inside `my_iceberg_catalog`
            ("spark.sql.catalog.my_iceberg_catalog.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog"),
            ("spark.sql.catalog.my_iceberg_catalog.uri", "jdbc:postgresql://postgres:5432/iceberg_db"),
            ("spark.sql.catalog.my_iceberg_catalog.jdbc.user", "postgres"),
            ("spark.sql.catalog.my_iceberg_catalog.jdbc.password", "postgres"),

            # Configure Warehouse on MinIO
            ("spark.sql.catalog.my_iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
            ("spark.sql.catalog.my_iceberg_catalog.s3.endpoint", "http://minio:9000"),
            ("spark.sql.catalog.my_iceberg_catalog.s3.path-style-access", "true"),
            ("spark.sql.catalog.my_iceberg_catalog.warehouse", "s3://warehouse"),
        ])
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark


def create_table(spark: SparkSession):
    spark.sql("""
      CREATE TABLE IF NOT EXISTS my_iceberg_catalog.db.vaccinations (
        location string,
        date date,
        vaccine string,
        source_url string,
        total_vaccinations bigint,
        people_vaccinated bigint,
        people_fully_vaccinated bigint,
        total_boosters bigint
      ) USING iceberg PARTITIONED BY (location, date)
    """)


def drop_table(spark: SparkSession):
    spark.sql("TRUNCATE TABLE my_iceberg_catalog.db.vaccinations;")
    spark.sql("DROP TABLE my_iceberg_catalog.db.vaccinations;")


def write_data(spark: SparkSession):
    current_dir = os.path.realpath(os.path.dirname(__file__))
    path = os.path.join(current_dir, "covid19-vaccinations-country-data", "Belgium.csv")

    vaccinations: DataFrame = spark.read \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .csv(path)

    vaccinations \
      .withColumn("date", F.to_date(F.col("date"))) \
      .writeTo("my_iceberg_catalog.db.vaccinations") \
      .append()


def read_data(spark: SparkSession):
    vaccinations = spark.table("my_iceberg_catalog.db.vaccinations")
    vaccinations.orderBy("date").show(3)


def add_column(spark: SparkSession):
    spark.sql("""
        ALTER TABLE my_iceberg_catalog.db.vaccinations
        ADD COLUMN people_fully_vaccinated_percentage double
      """
    )


def add_data_with_new_column(spark: SparkSession):
    current_dir = os.path.realpath(os.path.dirname(__file__))
    path = os.path.join(current_dir, "covid19-vaccinations-country-data", "Spain.csv")

    spain_vaccinations: DataFrame = spark.read \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .csv(path)

    spain_vaccinations \
      .withColumn("date", F.to_date(F.col("date"))) \
      .withColumn("people_fully_vaccinated_percentage", (F.col("people_fully_vaccinated") / F.col("people_vaccinated")) * 100) \
      .writeTo("my_iceberg_catalog.db.vaccinations") \
      .append()


def change_partitioning(spark: SparkSession):
    spark.sql("ALTER TABLE my_iceberg_catalog.db.vaccinations DROP PARTITION FIELD date")
    spark.sql("ALTER TABLE my_iceberg_catalog.db.vaccinations ADD PARTITION FIELD months(date)")
    # new data added will be partitioned by month


def app():
    spark = init_spark()

    """
     Comment or uncomment methods you want below
    """

    create_table(spark)

    # write_data(spark)

    # add_column(spark)
    # add_data_with_new_column(spark)
    # change_partitioning(spark)

    # read_data(spark)

    # drop_table(spark)


if __name__ == "__main__":
    app()
