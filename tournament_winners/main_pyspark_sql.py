from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual


def run(spark: SparkSession):
    spark.read.csv("data/matches.csv", header=True, inferSchema=True).createOrReplaceTempView("matches")
    spark.read.csv("data/players.csv", header=True, inferSchema=True).createOrReplaceTempView("players")
    actual_df = spark.sql("""
        -- Unpivot players and their scores from the matches table and aggregate them.
        WITH scores AS (
            SELECT
                player_id,
                SUM(score) AS score
            FROM matches
            UNPIVOT (
                (player_id, score) FOR column IN (
                    (first_player, first_score),
                    (second_player, second_score)
                )
            )
            GROUP BY player_id
        ),
        -- Rank the players per group, prioritizing by score (greatest) and player_id (lowest)
        group_ranks AS (
            SELECT
                group_id,
                player_id,
                RANK() OVER (PARTITION BY group_id ORDER BY score DESC, player_id ASC) AS rank
            FROM scores
            JOIN
                players USING (player_id)
        )
        -- Keep the top result per group
        SELECT
            group_id,
            player_id
        FROM group_ranks
        WHERE
            rank = 1
    """)

    expected_df = spark.read.csv("data/result.csv", header=True, inferSchema=True)
    assertDataFrameEqual(actual_df, expected_df)
    print("Success!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TournamentWinners").getOrCreate()
    run(spark)
    spark.stop()
