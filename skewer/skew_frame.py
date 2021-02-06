from dataclasses import dataclass
from typing import Union, List
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F


@dataclass
class SkewFrame:
    spark: SparkSession
    df: DataFrame
    relation: str
    key_name: str
    frequency: float
    replication_factor: int = None
    skewed_keys: Union[DataFrame, List[str]] = None

    def __post_init__(self):
        self.replication_factor = (
            self._get_default_replication_factor(self.spark)
            if not self.replication_factor
            else self.replication_factor
        )

        if not self.skewed_keys:
            self.skewed_keys = self._calc_skewed_keys(
                self.key_name, self.frequency, self.df
            )
        elif isinstance(self.skewed_keys, List):
            data = [{f"{self.key_name}_freqItems": self.skewed_keys}]
            self.skewed_keys = self.spark.createDataFrame(Row(**d) for d in data)
        else:
            pass

        self.salted_df = self._append_skew_indicator(
            self.key_name, self.skewed_keys, self.df
        )

        if self.relation == "source":
            self.salted_df = self._salt_source(
                self.key_name, self.replication_factor, self.salted_df
            )
        elif self.relation == "target":
            self.salted_df = self._salt_target(
                self.spark, self.key_name, self.replication_factor, self.salted_df
            )
        else:
            raise Exception("no.")
        return self

    @staticmethod
    def _get_default_replication_factor(spark: SparkSession):
        shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
        return int(shuffle_partitions)

    @staticmethod
    def _calc_skewed_keys(key_name: str, frequency: float, df: DataFrame):
        return df.freqItems([key_name], frequency)

    def _append_skew_indicator(
        self, key: str, skewed_keys: DataFrame, df: DataFrame
    ) -> DataFrame:
        freq_items_alias = [c for c in skewed_keys.columns][0]
        return (
            df.crossJoin(F.broadcast(skewed_keys))
            .withColumn(
                "skewed_ind",
                F.when(
                    F.expr(f"array_contains({freq_items_alias}, {key})"), 1
                ).otherwise(0),
            )
            .drop(freq_items_alias)
        )

    @classmethod
    def _salt_source(cls, key: str, replication_factor: float, df: DataFrame):
        return df.withColumn(
            "salted_key",
            F.when(
                F.col("skewed_ind") == F.lit(1), cls._salt_key(key, replication_factor)
            ).otherwise(F.col(key)),
        ).drop("skewed_ind")

    @classmethod
    def _salt_key(cls, key: str, replication_factor: float):
        return F.concat(F.col(key), F.lit("_"), cls._rand_int(replication_factor))

    @staticmethod
    def _rand_int(replication_factor: float):
        return F.least(
            F.floor(F.rand() * replication_factor), F.lit(replication_factor - 1)
        )

    @classmethod
    def _salt_target(
        cls, spark: SparkSession, key: str, replication_factor: float, df: DataFrame
    ):
        skew_rows = (
            df.where(F.col("skewed_ind") == 1)
            .crossJoin(cls._replicate_keys(spark, key, replication_factor))
            .withColumn("salted_key", cls._concat_salt_col(key))
        )
        nskew_rows = df.where(F.col("skewed_ind") == 0).withColumn(
            "salted_key", F.col(key)
        )
        return skew_rows.union(nskew_rows).drop("skewed_ind")

    @staticmethod
    def _concat_salt_col(key: str):
        return F.concat(F.col(key), F.lit("_"), F.col("salted_key"))

    @staticmethod
    def _replicate_keys(spark: SparkSession, key: str, replication_factor: float):
        return spark.range(replication_factor).withColumnRenamed("id", "salted_key")
