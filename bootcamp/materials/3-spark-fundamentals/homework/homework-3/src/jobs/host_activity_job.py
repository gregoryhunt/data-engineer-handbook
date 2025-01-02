from pyspark.sql import SparkSession


def do_host_activity_transformation(spark, dataframe_hc, dataframe_e, ds):
    query = f"""
      WITH yesterday AS (
          SELECT 
              *
          FROM hosts_cumulated
          WHERE date = date_sub(cast('{ds}' as date), 1)
      ),
      today AS (
          SELECT 
              e.host,
              cast(event_time as date) as date_active
          FROM events e
          WHERE cast(e.event_time as date) = cast('{ds}' as date)
          AND e.user_id is not null
          GROUP BY e.host, cast(event_time as date)
      )
      SELECT 
          coalesce(t.host, y.host) as host,
          coalesce(t.date_active, date_add(y.date, 1)) as date,
          CASE 
              WHEN y.host_activity_datelist IS NULL THEN array(t.date_active)
              WHEN t.date_active IS NULL THEN y.host_activity_datelist
              ELSE concat(y.host_activity_datelist, array(t.date_active))
          END as host_activity_datelist
      FROM today t
      FULL OUTER JOIN yesterday y
      ON t.host = y.host
    """
    dataframe_hc.createOrReplaceTempView("hosts_cumulated")
    dataframe_e.createOrReplaceTempView("events")
    return spark.sql(query)


def main():
    ds = '2023-01-01'
    spark = SparkSession.builder \
      .master("local") \
      .appName("hosts_scd") \
      .getOrCreate()
    output_df = do_host_activity_transformation(spark, spark.table("hosts_cumulated"), spark.table("events"), ds)
    output_df.write.mode("overwrite").insertInto("hosts_cumulated")