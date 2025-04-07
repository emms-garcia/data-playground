Table `players`

```
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| player_id   | int     |
| group_id    | int     |
+-------------+---------+
```

- Each record in the table `players` represents a single player in the tournament.
- The column `player_id` contains the ID of each player.
- The column `group_id` contains the id of the group that each player belongs to.

Table `matches`

```
+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| match_id      | int     |
| first_player  | int     |
| second_player | int     |
| first_score   | int     |
| second_score  | int     |
+---------------+---------+
```

- Each record in the table `matches` represents a single match in the group stage.
- The column `first_player` contains the id of the first player in the match.
- The column `second_player` contains the id of the second player in the match.
- The column `first_score` contains the number of points scored by the first player in the match.
- The column `second_score` contains the number of points scored by the second player in the match.
- You may assume that, in each match, players belong to the same group.

You would like to compute the winner in each group. The winner in each group is the player who scored the maximum total number of points within the group. If there is more that one such player, the winner is the one with the lowest ID.

Write an SQL query that returns a table containing the winner of each group. Each record should contain the id of the group and the id of the winner of this group. Records should be ordered by increasing ID number of the group.

Assume that:
- Groups are numbered with consecutive integers beginning from 1
- Every player from table matches occurs in table players
- In each match players belong to the same group
- `score` is a value between 0 and 1000000
- There are at most 100 players
- There are at most 100 matches
