from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, avg, count, desc, sum

spark = SparkSession.builder.appName("homework_3").getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

matches_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/matches.csv")

match_details_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/match_details.csv")

medals_matches_players_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/medals_matches_players.csv")

# Broadcast smaller tables
medals_df = broadcast(spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/medals.csv"))

maps_df = broadcast(spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/home/iceberg/data/maps.csv"))

spark.sql("""DROP TABLE IF EXISTS homework_3.matches""")
matches_ddl = """
CREATE TABLE homework_3.matches (
    match_id STRING,
    mapid STRING,
    is_team_game BOOLEAN,
    playlist_id STRING,
    completion_date TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
"""
spark.sql(matches_ddl)
matches_df \
    .select("match_id", "mapid", "is_team_game", "playlist_id", "completion_date") \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("homework_3.matches")
    

spark.sql("""DROP TABLE IF EXISTS homework_3.match_details""")
match_details_ddl = """
CREATE TABLE IF NOT EXISTS homework_3.match_details (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(match_details_ddl)
match_details_df \
    .select("match_id", "player_gamertag", "player_total_kills", "player_total_deaths") \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("homework_3.match_details")
    


spark.sql("""DROP TABLE IF EXISTS homework_3.medal_matches_players""")
medal_matches_players_ddl = """
CREATE TABLE homework_3.medal_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id LONG,
    count INT
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))
"""
spark.sql(medal_matches_players_ddl)
medals_matches_players_df \
    .select("match_id", "player_gamertag", "medal_id", "count") \
    .write.mode("append") \
    .bucketBy(16, "match_id").saveAsTable("homework_3.medals_matches_players")

# Join tables
joined_match_df = spark.table("homework_3.matches").alias("m") \
    .join(spark.table("homework_3.match_details").alias("md"), col("m.match_id") == col("md.match_id"), "left") \
    .join(spark.table("homework_3.medals_matches_players").alias("mmp"), (col("md.match_id") == col("mmp.match_id")) & (col("md.player_gamertag") == col("mmp.player_gamertag")), "left") \
    .join(broadcast(maps_df).alias("maps"), col("m.mapid") == col("maps.mapid"), "left") \
    .join(broadcast(medals_df).alias("medals").select("medals.medal_id", "medals.name"), col("mmp.medal_id") == col("medals.medal_id"), "left")


avg_kills = joined_match_df \
    .select("m.match_id", "md.player_gamertag", "md.player_total_kills").distinct() \
    .groupBy("player_gamertag") \
    .agg(
        avg("player_total_kills").alias("avg_kills"),
        count("match_id").alias("total_matches")
    ) \
    .orderBy(desc("avg_kills"))
avg_kills.take(5)

playlist_counts = joined_match_df \
    .select("m.match_id", "m.playlist_id").distinct() \
    .groupBy("playlist_id") \
    .count() \
    .orderBy(desc("count"))

playlist_counts.take(5)

map_counts = joined_match_df \
    .select("m.match_id", "maps.name").distinct() \
    .groupBy("name") \
    .count() \
    .orderBy(desc("count"))

map_counts.take(5)

killing_spree_by_map = joined_match_df \
    .filter(col("medals.name") == "Killing Spree") \
    .groupBy("maps.name") \
    .agg(sum("mmp.count").alias("spree_count")) \
    .orderBy(desc("spree_count"))

killing_spree_by_map.take(5)

spark.sql("""
    CREATE TABLE IF NOT EXISTS hw3.agg_matches.unsorted (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        player_gamertag STRING,
        player_total_kills INT,
        player_total_deaths INT,
        medal_count INT,
        map_name STRING,
        description STRING,
        medal_id LONG,
        medal_name STRING
)
USING iceberg
PARTITIONED BY (completion_date);
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS hw3.agg_matches.date_sorted (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        player_gamertag STRING,
        player_total_kills INT,
        player_total_deaths INT,
        medal_count INT,
        map_name STRING,
        description STRING,
        medal_id LONG,
        medal_name STRING
)
USING iceberg
PARTITIONED BY (completion_date);
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS hw3.agg_matches.maps_sorted (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        player_gamertag STRING,
        player_total_kills INT,
        player_total_deaths INT,
        medal_count INT,
        map_name STRING,
        description STRING,
        medal_id LONG,
        medal_name STRING
)
USING iceberg
PARTITIONED BY (completion_date);
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS hw3.agg_matches.playlist_sorted (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP,
        player_gamertag STRING,
        player_total_kills INT,
        player_total_deaths INT,
        medal_count INT,
        map_name STRING,
        description STRING,
        medal_id LONG,
        medal_name STRING
)
USING iceberg
PARTITIONED BY (completion_date);
""")

df = joined_match_df \
    .withColumn("medal_count", col("mmp.count")) \
    .withColumn("map_name", col("maps.name")) \
    .withColumn("medal_name", col("medals.name")) \
    .select("m.match_id",
        "m.mapid",
        "m.is_team_game",
        "m.playlist_id",
        "m.completion_date",
        "md.player_gamertag",
        "md.player_total_kills",
        "md.player_total_deaths",
        "medal_count",
        "map_name",
        "maps.description",
        "medals.medal_id",
        "medal_name")

start_df = df.repartition(4, "completion_date")

first_sort_df = df.repartition(10, "completion_date") \
             .sortWithinPartitions(col("completion_date"))

maps_sort_df = df.repartition(10, "completion_date") \
             .sortWithinPartitions(col("map_name"))

playlist_sort_df = df.repartition(10, "completion_date") \
             .sortWithinPartitions(col("playlist_id"))

start_df.write.mode("overwrite").saveAsTable("hw3.agg_matches.unsorted")
first_sort_df.write.mode("overwrite").saveAsTable("hw3.agg_matches.date_sorted")
maps_sort_df.write.mode("overwrite").saveAsTable("hw3.agg_matches.maps_sorted")
playlist_sort_df.write.mode("overwrite").saveAsTable("hw3.agg_matches.playlist_sorted")

spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM hw3.agg_matches.unsorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'date_sorted' 
FROM hw3.agg_matches.date_sorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'date_sorted' 
FROM hw3.agg_matches.maps_sorted.files
UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'date_sorted' 
FROM hw3.agg_matches.playlist_sorted.files
""").show()