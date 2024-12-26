from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from ..jobs.actor_scd_job import do_actor_scd_transformation

Yesterday = namedtuple("Yesterday", "actor_name year films quality_class")
Today = namedtuple("Today", "actor year film votes rating filmid")
ActorScd = namedtuple("ActorScd", "actor_name year films quality_class is_active")

def test_new_actor_entry(spark):
    yesterday_data = []
    today_data = [
        Today("Tom Hanks", 1970, "Movie1", 1000, 8.5, "film1"),
        Today("Tom Hanks", 1970, "Movie2", 2000, 8.2, "film2")
    ]
    
    yesterday_df = spark.createDataFrame(yesterday_data)
    today_df = spark.createDataFrame(today_data)
    
    actual_df = do_actor_scd_transformation(spark, yesterday_df, today_df)
    
    expected_films = [(1970, "Movie1", 1000, 8.5, "film1"),
                      (1970, "Movie2", 2000, 8.2, "film2")]
    expected_data = [
        ActorScd("Tom Hanks", 1970, expected_films, "star", True)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df)

def test_existing_actor_quality_change(spark):
    yesterday_data = [
        Yesterday("Morgan Freeman", 1969, 
                 [(1969, "OldMovie", 500, 6.5, "film0")], 
                 "average")
    ]
    today_data = [
        Today("Morgan Freeman", 1970, "NewMovie1", 3000, 8.8, "film1"),
        Today("Morgan Freeman", 1970, "NewMovie2", 2500, 8.5, "film2")
    ]
    
    yesterday_df = spark.createDataFrame(yesterday_data)
    today_df = spark.createDataFrame(today_data)
    
    actual_df = do_actor_scd_transformation(spark, yesterday_df, today_df)
    
    expected_films = [(1969, "OldMovie", 500, 6.5, "film0"),
                      (1970, "NewMovie1", 3000, 8.8, "film1"),
                      (1970, "NewMovie2", 2500, 8.5, "film2")]
    expected_data = [
        ActorScd("Morgan Freeman", 1970, expected_films, "star", True)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df)

def test_inactive_actor(spark):
    yesterday_data = [
        Yesterday("Al Pacino", 1969,
                 [(1969, "OldMovie", 1500, 7.5, "film0")],
                 "good")
    ]
    today_data = []
    
    yesterday_df = spark.createDataFrame(yesterday_data)
    today_df = spark.createDataFrame(today_data)
    
    actual_df = do_actor_scd_transformation(spark, yesterday_df, today_df)
    
    expected_films = [(1969, "OldMovie", 1500, 7.5, "film0")]
    expected_data = [
        ActorScd("Al Pacino", 1970, expected_films, "good", False)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df)

def test_quality_class_thresholds(spark):
    test_cases = [
        (8.5, "star"),
        (7.5, "good"),
        (6.5, "average"),
        (5.5, "bad")
    ]
    
    for rating, expected_quality in test_cases:
        today_data = [
            Today("Test Actor", 1970, "TestMovie", 1000, rating, "film1")
        ]
        today_df = spark.createDataFrame(today_data)
        yesterday_df = spark.createDataFrame([])
        
        actual_df = do_actor_scd_transformation(spark, yesterday_df, today_df)
        
        expected_films = [(1970, "TestMovie", 1000, rating, "film1")]
        expected_data = [
            ActorScd("Test Actor", 1970, expected_films, expected_quality, True)
        ]
        expected_df = spark.createDataFrame(expected_data)
        
        assert_df_equality(actual_df, expected_df)