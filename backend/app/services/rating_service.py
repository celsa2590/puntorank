from fastapi import HTTPException


def get_match_weight(match_type: str) -> float:
    weights = {
        "standard": 1.0,
        "match": 1.0,
        "cuadrangular": 0.8,
        "hexagonal": 0.7,
        "americano_short": 0.5,
    }
    return weights.get(match_type, 1.0)


def expected_score(team_rating: float, opponent_rating: float) -> float:
    return 1 / (1 + 10 ** ((opponent_rating - team_rating) / 400))


def ensure_player_rating(cur, player_id: int):
    cur.execute(
        """
        INSERT INTO player_ratings (player_id, rating, matches_count)
        VALUES (%s, 1000, 0)
        ON CONFLICT (player_id) DO NOTHING;
        """,
        (player_id,),
    )

def update_ratings_for_match(cur, match_id: int):
    cur.execute(
        """
        SELECT 
            m.id,
            m.match_type,
            m.rating_processed,
            mr.winning_team
        FROM matches m
        JOIN match_results mr ON mr.match_id = m.id
        WHERE m.id = %s;
        """,
        (match_id,),
    )
    match = cur.fetchone()

    if not match or not match["winning_team"]:
        raise HTTPException(status_code=400, detail="El partido no tiene equipo ganador")

    if match["rating_processed"]:
        raise HTTPException(status_code=400, detail="El rating de este partido ya fue procesado")

    cur.execute(
        """
        SELECT player_id, team
        FROM match_players
        WHERE match_id = %s;
        """,
        (match_id,),
    )
    players = cur.fetchall()

    if len(players) != 4:
        raise HTTPException(status_code=400, detail="El partido debe tener 4 jugadores")

    for p in players:
        ensure_player_rating(cur, p["player_id"])

    team_a = [p["player_id"] for p in players if p["team"] == "A"]
    team_b = [p["player_id"] for p in players if p["team"] == "B"]
    cur.execute(
        """
        SELECT player_id, rating
        FROM player_ratings
        WHERE player_id = ANY(%s);
        """,
        (team_a + team_b,),
    )
    ratings = {r["player_id"]: float(r["rating"]) for r in cur.fetchall()}

    rating_a = sum(ratings[p] for p in team_a) / 2
    rating_b = sum(ratings[p] for p in team_b) / 2

    expected_a = expected_score(rating_a, rating_b)
    expected_b = expected_score(rating_b, rating_a)

    actual_a = 1 if match["winning_team"] == "A" else 0
    actual_b = 1 if match["winning_team"] == "B" else 0

    k = 32
    weight = get_match_weight(match["match_type"])

    delta_a = k * weight * (actual_a - expected_a)
    delta_b = k * weight * (actual_b - expected_b)

    for player_id in team_a:
        old_rating = ratings[player_id]
        new_rating = old_rating + delta_a

        cur.execute("""
            UPDATE player_ratings
            SET rating = %s,
                matches_count = matches_count + 1,
                updated_at = NOW()
            WHERE player_id = %s;
        """, (new_rating, player_id))

        cur.execute("""
            INSERT INTO rating_history (player_id, match_id, rating_before, rating_after, delta)
            VALUES (%s, %s, %s, %s, %s);
        """, (player_id, match_id, old_rating, new_rating, delta_a))

    for player_id in team_b:
        old_rating = ratings[player_id]
        new_rating = old_rating + delta_b

        cur.execute(
            """
            UPDATE player_ratings
            SET rating = %s,
                matches_count = matches_count + 1,
                updated_at = NOW()
            WHERE player_id = %s;
            """, (new_rating, player_id))

        cur.execute("""
            INSERT INTO rating_history (player_id, match_id, rating_before, rating_after, delta)
            VALUES (%s, %s, %s, %s, %s);
        """, (player_id, match_id, old_rating, new_rating, delta_b))
        

def update_rating_pair_vs_pair(
    cur,
    player_ids_team_a,
    player_ids_team_b,
    winner,
    match_id=None,
    source_type=None,
    source_id=None,
    multiplier=1.0,
):
    ratings = {}

    all_players = player_ids_team_a + player_ids_team_b

    for player_id in all_players:
        ensure_player_rating(cur, player_id)

        cur.execute(
            """
            SELECT rating
            FROM player_ratings
            WHERE player_id = %s;
            """,
            (player_id,),
        )

        ratings[player_id] = float(cur.fetchone()["rating"])

    rating_a = sum(ratings[p] for p in player_ids_team_a) / 2
    rating_b = sum(ratings[p] for p in player_ids_team_b) / 2

    expected_a = 1 / (1 + 10 ** ((rating_b - rating_a) / 400))
    expected_b = 1 - expected_a

    score_a = 1 if winner == "A" else 0
    score_b = 1 if winner == "B" else 0

    k = 32 * multiplier

    delta_a = k * (score_a - expected_a)
    delta_b = k * (score_b - expected_b)

    for player_id in player_ids_team_a:
        before = ratings[player_id]
        after = before + delta_a

        cur.execute(
            """
            UPDATE player_ratings
            SET rating = %s,
                matches_count = matches_count + 1,
                updated_at = NOW()
            WHERE player_id = %s;
            """,
            (after, player_id),
        )

        cur.execute(
            """
            INSERT INTO rating_history
                (
                    player_id,
                    match_id,
                    source_type,
                    source_id,
                    rating_before,
                    rating_after,
                    delta
                )
            VALUES
                (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                player_id,
                match_id,
                source_type,
                source_id,
                before,
                after,
                delta_a,
            ),
        )

    for player_id in player_ids_team_b:

        before = ratings[player_id]
        after = before + delta_b

        cur.execute(
            """
            UPDATE player_ratings
            SET rating = %s,
                matches_count = matches_count + 1,
                updated_at = NOW()
            WHERE player_id = %s;
            """,
            (after, player_id),
        )

        cur.execute(
            """
            INSERT INTO rating_history
                (
                    player_id,
                    match_id,
                    source_type,
                    source_id,
                    rating_before,
                    rating_after,
                    delta
                )
            VALUES
                (%s, %s, %s, %s, %s, %s, %s);
            """,
            (
                player_id,
                match_id,
                source_type,
                source_id,
                before,
                after,
                delta_b,
            ),
        )


def apply_rating_bonus(cur, player_id: int, bonus: float, source_type: str, source_id: int):
    ensure_player_rating(cur, player_id)

    cur.execute(
        """
        SELECT rating
        FROM player_ratings
        WHERE player_id = %s;
        """,
        (player_id,),
    )

    before = float(cur.fetchone()["rating"])
    after = before + bonus

    cur.execute(
        """
        UPDATE player_ratings
        SET rating = %s
        WHERE player_id = %s;
        """,
        (after, player_id),
    )

    cur.execute(
        """
        INSERT INTO rating_history
            (player_id, match_id, source_type, source_id, rating_before, rating_after, delta)
        VALUES
            (%s, NULL, %s, %s, %s, %s, %s);
        """,
        (player_id, source_type, source_id, before, after, bonus),
    )
