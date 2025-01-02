from datetime import datetime, date
from chispa.dataframe_comparer import *

from ..jobs.host_activity_job import do_host_activity_transformation
from collections import namedtuple

HostCumulated = namedtuple("HostCulumated", "host date host_activity_datelist")
Event = namedtuple("Event", "host event_time user_id")



def test_host_activity_cumulation(spark):
    ds = '2023-01-02'
    input_data_hc = [
        HostCumulated("host1", date(2023, 1, 1), ["2023-01-01"]),
        HostCumulated("host2", date(2023, 1, 1), ["2023-01-01"]),
        HostCumulated("host3", date(2023, 1, 1), ["2023-01-01"])

    ]
    input_data_events = [
        Event("host1", datetime(2023, 1, 2, 1, 0, 0), "user1"),
        Event("host3", datetime(2023, 1, 2, 1, 0, 0), "user2"),

    ]

    source_hc_df = spark.createDataFrame(input_data_hc)
    source_events_df = spark.createDataFrame(input_data_events)
    actual_df = do_host_activity_transformation(spark, source_hc_df, source_events_df, ds)


    expected_values = [
       HostCumulated("host1", date(2023, 1, 2), ["2023-01-01", "2023-01-02"]),
       HostCumulated("host2", date(2023, 1, 2), ["2023-01-01"]),
       HostCumulated("host3", date(2023, 1, 2), ["2023-01-01", "2023-01-02"])
    ]
    expected_df = spark.createDataFrame(expected_values)
    assert_df_equality(actual_df, expected_df)

