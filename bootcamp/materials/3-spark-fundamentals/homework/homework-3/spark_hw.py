from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lit, col, avg, count, desc, sum

spark = SparkSession.builder.appName("homework_3").getOrCreate()

match_details_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/match_details.csv") \
    .repartitionByRange(16, "match_id")

matches_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/matches.csv") \
    .repartitionByRange(16, "match_id")

medals_matches_players_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/medals_matches_players.csv") \
    .repartitionByRange(16, "match_id")

# Broadcast smaller tables
medals_df = broadcast(spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/medals.csv"))

maps_df = broadcast(spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/maps.csv"))

joined_df = match_details_df \
    .join(matches_df, "match_id") \
    .join(medals_matches_players_df, ["match_id", "player_gamertag"]) \
    .join(medals_df.select(
        col("medal_id"),
        col("name").alias("medal_name"),
        col("description").alias("medal_description")
    ), "medal_id") \
    .join(maps_df.select(
        col("mapid"),
        col("name").alias("map_name"),
        col("description").alias("map_description")
    ), matches_df.mapid == maps_df.mapid) \
    .drop(maps_df.mapid)

avg_kills = match_details_df \
    .groupBy("player_gamertag") \
    .agg(
        avg("player_total_kills").alias("avg_kills"),
        count("match_id").alias("total_matches")
    ) \
    .orderBy(desc("avg_kills"))
avg_kills.show(5)

playlist_counts = matches_df \
    .groupBy("playlist_id") \
    .count() \
    .orderBy(desc("count"))

playlist_counts.show(5)

map_counts = matches_df \
    .join(maps_df, "mapid") \
    .groupBy("name") \
    .count() \
    .orderBy(desc("count"))

map_counts.show(5)

killing_spree_by_map = joined_df \
    .filter(col("medal_name") == "Killing Spree") \
    .groupBy("map_name") \
    .agg(sum("count").alias("spree_count")) \
    .orderBy(desc("spree_count"))

killing_spree_by_map.show(5)

unsorted_df = joined_df.repartition(16, matches_df.mapid)
unsorted_df.write.mode("overwrite").saveAsTable("hw3.unsorted")

sorted_by_playlist = joined_df \
    .sortWithinPartitions(matches_df.playlist_id)
sorted_by_playlist.write.mode("overwrite").saveAsTable("hw3.sorted_by_playlist")

sorted_by_map = joined_df \
    .sortWithinPartitions(matches_df.mapid)
sorted_by_map.write.mode("overwrite").saveAsTable("hw3.sorted_by_map")

spark.sql("""
    SELECT SUM(file_size_in_bytes) as size, 
           COUNT(1) as num_files, 
           'sorted_by_playlist' as sort_type
    FROM hw3.sorted_by_playlist.files
    UNION ALL
    SELECT SUM(file_size_in_bytes) as size, 
           COUNT(1) as num_files, 
           'sorted_by_map' as sort_type
    FROM hw3.sorted_by_map.files
    UNION ALL
    SELECT SUM(file_size_in_bytes) as size, 
           COUNT(1) as num_files, 
           'unsorted' as sort_type
    FROM hw3.unsorted.files
""").show()