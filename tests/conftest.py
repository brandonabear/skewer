import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    return (
        SparkSession.builder.master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def source(spark_session):
    return spark_session.read.csv("skewer/source.csv", header=True)


@pytest.fixture(scope="session")
def target(spark_session):
    return spark_session.read.csv("skewer/target.csv", header=True)
