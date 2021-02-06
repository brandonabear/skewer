import pytest
from unittest import mock
from pyspark.sql import SparkSession, DataFrame
from skewer.skew_frame import SkewFrame


@mock.patch("pyspark.sql.SparkSession")
def test__get_default_replication_factor(mock_spark):
    mock_spark.conf = {"spark.sql.shuffle.partitions": "10"}
    replication_factor = SkewFrame._get_default_replication_factor(mock_spark)
    assert replication_factor == 10


@mock.patch("pyspark.sql.DataFrame")
def test__calc_skewed_keys(mock_dataframe):
    key_name = "key"
    frequency = 0.5
    SkewFrame._calc_skewed_keys(key_name, frequency, mock_dataframe)
    assert mock_dataframe.freqItems.called
