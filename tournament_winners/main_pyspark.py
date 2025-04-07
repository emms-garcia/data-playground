from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import rank, sum as spark_sum
from pyspark.testing.utils import assertDataFrameEqual


def run(spark: SparkSession):
    # 1) Read the matches and players dataframes
    matches_df = spark.read.csv("data/matches.csv", header=True, inferSchema=True)
    players_df = spark.read.csv("data/players.csv", header=True, inferSchema=True)
    # 2) Unpivot the first_player, first_score and second_player, second_score columns from the matches dataframe
    # into a clean player_id, score dataframe.
    scores_df = matches_df.select(
        matches_df.first_player.alias("player_id"),
        matches_df.first_score.alias("score"),
    ).union(matches_df.select(
        matches_df.second_player.alias("player_id"),
        matches_df.second_score.alias("score"),
    ))
    # 3) Sum the player scores across matches
    scores_df = scores_df.groupBy("player_id").agg(spark_sum("score").alias("score"))
    # 4) Join the scores dataframe with the players dataframe to get the group_id
    df = scores_df.join(players_df, "player_id")
    # 5) Add a rank column, partitioning by group_id and ordering by score
    window = Window.partitionBy(players_df.group_id).orderBy(scores_df.score.desc())
    df = df.withColumn("rank", rank().over(window))
    # 6) Keep only the top player by group
    df = df.filter(df.rank == 1)
    df = df.select("group_id", "player_id")

    expected_df = spark.read.csv("data/result.csv", header=True, inferSchema=True)
    assertDataFrameEqual(df, expected_df)
    print("Success!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TournamentWinners").getOrCreate()
    run(spark)
    spark.stop()
