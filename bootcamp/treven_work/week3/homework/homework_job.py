from pyspark.sql import SparkSession

query_one = """
WITH 
    years as (
        select *
        from generate_series(1970, 2021) as year
    ), 

    actor AS (
        select
            actor,
            min(year) as first_film_year
        from actor_films
        group by actor
    ), 

    actors_and_years AS (
        select *
        from actor a
        inner join years y
            on a.first_film_year <= y.year
    ), 

    windowed as (
        select
            aay.actor,
            aay.year,
            array_remove(
                array_agg(
                    case
                        when af.year is not null
                            then row(
                                af.film,
                                af.year,
                                af.votes,
                                af.rating,
                                af.filmid
                            )::films
                    end)
                over (partition by aay.actor order by coalesce(aay.year, af.year)),
                null
            ) as films
        from actors_and_years aay
        left join actor_films af
            on aay.actor = af.actor
            and aay.year = af.year
        order by aay.actor, aay.year
    ),

    static as (
        select
            actor,
            max(actorid) as actorid
        from actor_films
        group by actor
    )

select distinct 
    w.actor,
    s.actorid,
    w.films,
    case
        when (films[cardinality(films)]::films).rating > 8 then 'star'
        when (films[cardinality(films)]::films).rating > 7 then 'good'
        when (films[cardinality(films)]::films).rating > 6 then 'average'
        else 'bad'
    end::quality_class as quality_class,
    w.year,
    w.year - (films[cardinality(films)]::films).year as years_since_last_active,    
    (films[cardinality(films)]::films).year = year as is_active
from windowed w
join static s
    on w.actor = s.actor

"""


query_two = """
with 
    row_nums as (
        select *, 
            row_number() over (partition by game_id, player_id) as rn 
        from game_details
    ),

    deduped as (
        select * 
        from row_nums 
        where rn = 1
    )

select * 
from deduped
"""


def do_actor_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actor_films")
    return spark.sql(query_one)

def do_games_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query_two)



def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("Week3 Testing Homework") \
      .getOrCreate()
    
    output_df_one = do_actor_transformation(spark, spark.table("actors"))
    output_df_one.write.mode("overwrite").insertInto("actor_films")

    output_df_two = do_games_transformation(spark, spark.table("games"))
    output_df_two.write.mode("overwrite").insertInto("game_details")

