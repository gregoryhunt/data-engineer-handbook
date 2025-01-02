from chispa.dataframe_comparer import *

from ..jobs.dedupe_games_job import do_dedupe_games_transformation
from collections import namedtuple

Game_Detail = namedtuple("Game_Details", "game_id team_id player_id")


def test_dedupe_game_details_generation(spark):
    input_data = [
        Game_Detail(22200162,1610612737,1630249),
        Game_Detail(22200162,1610612737,1630249),
        Game_Detail(22200162,1610612737,1630248)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_dedupe_games_transformation (spark, input_dataframe)
    expected_output = [
        Game_Detail(22200162,1610612737,1630248),
        Game_Detail(22200162,1610612737,1630249)
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)