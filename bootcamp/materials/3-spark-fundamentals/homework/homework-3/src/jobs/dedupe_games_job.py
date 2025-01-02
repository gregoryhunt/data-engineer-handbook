from pyspark.sql import SparkSession

query = """
WITH dedupe AS (
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) as row_num 
	FROM game_details
)
SELECT game_id, team_id, player_id FROM dedupe WHERE row_num = 1;
"""


def do_dedupe_games_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("players_scd") \
        .getOrCreate()
    output_df = do_dedupe_games_transformation(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("deduped_details")