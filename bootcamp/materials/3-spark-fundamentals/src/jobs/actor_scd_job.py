from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

query = """
    WITH yesterday AS (
        SELECT * FROM actors
        WHERE year = 1969
    ),
    today AS (
        SELECT * FROM actor_films
        WHERE year = 1970
    ),
    aggregated_films AS (
        SELECT 
            COALESCE(t.actor, y.actor_name) as actor_name,
            COALESCE(t.year, y.year + 1) as year,
            CASE 
                WHEN y.films IS NULL THEN collect_list(
                    struct(t.year, t.film, t.votes, t.rating, t.filmid)
                )
                WHEN t.year IS NOT NULL THEN 
                    array_union(y.films, collect_list(
                        struct(t.year, t.film, t.votes, t.rating, t.filmid)
                    ))
                ELSE y.films
            END as films,
            AVG(t.rating) as avg_rating,
            y.quality_class as prev_quality,
            CASE WHEN t.year IS NOT NULL THEN TRUE ELSE FALSE END as is_active
        FROM today t
        FULL OUTER JOIN yesterday y ON t.actor = y.actor_name
        GROUP BY 
            COALESCE(t.actor, y.actor_name),
            y.actor_name,
            y.films,
            t.year,
            y.year,
            y.quality_class
    )
    SELECT 
        actor_name,
        year,
        films,
        CASE 
            WHEN avg_rating > 8.0 THEN 'star'
            WHEN avg_rating > 7.0 THEN 'good'
            WHEN avg_rating > 6.0 THEN 'average'
            WHEN avg_rating <= 6.0 THEN 'bad'
            ELSE prev_quality
        END as quality_class,
        is_active
    FROM aggregated_films
"""

def define_film_schema():
    return StructType([
        StructField("year", IntegerType(), True),
        StructField("film", StringType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("filmid", StringType(), True)
    ])

def do_actor_scd_transformation(spark, actors_df, actor_films_df):
    # Register temporary views
    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")
    
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actor_scd") \
        .getOrCreate()
    
    # Define schema for films array
    film_schema = define_film_schema()
    
    # Read input tables with schema
    actors_df = spark.table("actors")
    actor_films_df = spark.table("actor_films")
    
    # Process transformation
    output_df = do_actor_scd_transformation(spark, actors_df, actor_films_df)
    
    # Write output
    output_df.write.mode("overwrite").insertInto("actors_scd")