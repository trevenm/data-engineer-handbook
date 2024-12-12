from chispa.dataframe_comparer import *
from jobs.homework_job import do_actor_transformation, do_games_transformation
from collections import namedtuple

ActorFilmRecord = namedtuple("ActorFilmRecord", "actor actorid film year votes rating filmid")
ActorRecord = namedtuple("ActorRecord", "actor actorid films quality_class year years_since_last_active is_active")

GameRecord = namedtuple("GameRecord", "game_id team_id team_abbreviation player_id player_name")


def test_actor_generation(spark):
    source_data = [
        ActorFilmRecord("50 Cent", "nm1265067", "Home of the Brave", 2006, 10500, 5.6, "tt0763840"),
        ActorFilmRecord("50 Cent", "nm1265067", "Vengeance", 2006, 133, 3.5, "tt0485920"),
        ActorFilmRecord("50 Cent", "nm1265067", "Get Rich or Die Tryin", 2005, 44370, 5.4, "tt0430308")
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_transformation(spark, source_df)
    expected_data = [
        ActorRecord("50 Cent", "nm1265067", '{"(\"Get Rich or Die Tryin\",2005,44370,5.4,tt0430308)"}', 'bad', 2005, 0, 'true'),
        ActorRecord("50 Cent", "nm1265067", '{"(\"Get Rich or Die Tryin\",2005,44370,5.4,tt0430308)","(Vengeance,2006,133,3.5,tt0485920)","(\"Home of the Brave\",2006,10500,5.6,tt0763840)"}', 'bad', 2006, 0, 'true')        
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)


def test_game_generation(spark):
    source_data = [
        GameRecord(22200162, 1610612737, 'ATL', 1630249, 'Vit Krejci'),
        GameRecord(22200162, 1610612737, 'ATL', 1630249, 'Vit Krejci'),
        GameRecord(22200162, 1610612737, 'ATL', 1630249, 'Vit Krejci') 
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_games_transformation(spark, source_df)
    expected_data = [
        GameRecord(22200162, 1610612737, 'ATL', 1630249, 'Vit Krejci'),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)