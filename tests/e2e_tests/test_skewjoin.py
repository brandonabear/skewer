import pytest
import hypothesis.strategies as st
from hypothesis import settings, given
from skewer.skewer import Skewer, SkewFrame


@settings(deadline=5000, max_examples=200)
@given(
    st_broadcast=st.booleans(),
    st_frequency=st.floats(min_value=0, max_value=1),
    st_replication_factor=st.integers(min_value=2, max_value=50),
)
def test_skewer(
    st_broadcast, st_frequency, st_replication_factor, spark_session, source, target
):
    skew_source = SkewFrame(
        spark=spark_session,
        df=source,
        relation="source",
        key_name="skid",
        frequency=st_frequency,
        replication_factor=st_replication_factor,
        skewed_keys=["-1"],
    )
    skew_target = SkewFrame(
        spark=spark_session,
        df=target,
        relation="target",
        key_name="tid",
        frequency=st_frequency,
        replication_factor=skew_source.replication_factor,
        skewed_keys=skew_source.skewed_keys,
    )
    sdf = Skewer.join(
        source=skew_source, target=skew_target, how="left", broadcast=st_broadcast
    )
    assert sdf.count() == source.count()


def test_skewframe_join_relation(spark_session, source):
    with pytest.raises(Exception):
        SkewFrame(spark_session, source, "not_supported", 0.1, 2)
