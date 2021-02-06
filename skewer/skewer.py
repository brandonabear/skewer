from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from skewer.skew_frame import SkewFrame


class Skewer:
    def join(
        source: SkewFrame, target: SkewFrame, how: str = "left", broadcast: bool = False
    ) -> DataFrame:
        if broadcast:
            target.salted_df = F.broadcast(target.salted_df)

        return source.salted_df.join(target.salted_df, how=how, on=["salted_key"]).drop(
            "salted_key"
        )
