import pandas as pd


def unpivot_player_scores_df(df: pd.DataFrame, player_id_column: str, player_score_column: str) -> pd.DataFrame:
    return df[[player_id_column, player_score_column]].rename(columns={
        player_id_column: "player_id",
        player_score_column: "score",
    })


def run():
    # 1) Read the matches and players dataframes
    matches_df = pd.read_csv("data/matches.csv")
    players_df = pd.read_csv("data/players.csv")
    # 2) Unpivot the first_player, first_score and second_player, second_score columns from the matches dataframe
    # into a clean player_id, score dataframe.
    scores_df = pd.concat([
        unpivot_player_scores_df(matches_df, "first_player", "first_score"),
        unpivot_player_scores_df(matches_df, "second_player", "second_score"),
    ])
    # 3) Sum the player scores across matches
    scores_df = scores_df.groupby("player_id").agg({"score": "sum"})
    # 4) Merge the scores dataframe with the players dataframe to get the group_id
    df = scores_df.merge(players_df, on="player_id", how="inner")
    # 5) Add a rank column, partitioning by group_id and ordering by score
    df["rank"] = df.groupby("group_id")["score"].rank(method="dense", ascending=False)
    # 6) Keep only the top player by group
    df = df[df["rank"] == 1]
    df = df[["group_id", "player_id"]]

    expected_df = pd.read_csv("data/result.csv")
    pd.testing.assert_frame_equal(expected_df.reset_index(drop=True), df.reset_index(drop=True))
    print("Success!")


if __name__ == "__main__":
    run()
