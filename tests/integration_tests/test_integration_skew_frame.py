import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from skewer.skew_frame import SkewFrame


def test_skewframe_init(spark_session, source):
    sf = SkewFrame(spark_session, source, "source", "skid", 0.4)
    assert sf

    sf = SkewFrame(spark_session, source, "source", "skid", 0.4, 5)
    assert sf

    sf = SkewFrame(spark_session, source, "source", "skid", 0.4, 5, ["-1"])
    assert sf


def test_skewframe__calc_skewed_keys(source):
    key_name = "skid"
    skewed_keys = SkewFrame._calc_skewed_keys(key_name, 0.1, source)
    assert skewed_keys.columns == [f"{key_name}_freqItems"]
    assert skewed_keys.schema == StructType(
        [StructField(f"{key_name}_freqItems", ArrayType(StringType(), False))]
    )


def test_skewframe__append_skew_indicator(source):
    key_name = "skid"
    skewed_keys = SkewFrame._calc_skewed_keys(key_name, 0.1, source)
    assert skewed_keys
