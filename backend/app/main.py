import os
import psycopg2
import random
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(title="PuntoRank API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "https://puntorank-frontend.onrender.com",
        "https://puntorank-backend.onrender.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL no está configurada")
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)

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
            SET rating = %s
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
            SET rating = %s
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


class PlayerCreate(BaseModel):
    name: str
    club_id: int | None = None
    is_registered: bool = False
    side: str | None = None

class PlayerRegister(BaseModel):
    name: str
    email: str
    club_id: int | None = None
    gender: str | None = None
    side: str | None = None
    category: str | None = None

class MatchPlayer(BaseModel):
    player_id: int | None = None
    name: str | None = None
    side: str | None = None
    team: str  # A o B


class MatchReport(BaseModel):
    club_id: int
    event_id: int | None = None
    match_type: str = "standard"
    created_by: int | None = None
    played_at: str | None = None
    score: str
    winning_team: str
    players: list[MatchPlayer]
    category: str | None = None

class PlayerRegister(BaseModel):
    name: str
    email: str
    club_id: int | None = None
    gender: str | None = None
    side: str | None = None
    category: str | None = None

class ClubLogin(BaseModel):
    username: str
    password: str

class AmericanoCreate(BaseModel):
    club_id: int
    name: str
    category: str
    gender: str
    courts: int
    duration_minutes: int


class AmericanoAddPlayer(BaseModel):
    player_id: int | None = None
    name: str | None = None
    email: str | None = None
    gender: str | None = None
    category: str | None = None
    side: str | None = None

class AmericanoPairCreate(BaseModel):
    player_1_id: int
    player_2_id: int
    pair_name: str | None = None

class AmericanoMatchResult(BaseModel):
    pair_a_games: int
    pair_b_games: int


def get_or_create_player(cur, player_data, club_id: int):
    if player_data.player_id is not None:
        ensure_player_rating(cur, player_data.player_id)
        return player_data.player_id

    if not player_data.name:
        raise HTTPException(
            status_code=400,
            detail="Si no envías player_id, debes enviar name",
        )

    cur.execute(
        """
        INSERT INTO players (name, club_id, is_registered, side)
        VALUES (%s, %s, FALSE, %s)
        RETURNING id;
        """,
        (player_data.name, club_id, player_data.side),
    )

    new_player = cur.fetchone()
    player_id = new_player["id"]

    cur.execute(
        """
        INSERT INTO player_clubs (player_id, club_id, is_home_club)
        VALUES (%s, %s, FALSE)
        ON CONFLICT (player_id, club_id) DO NOTHING;
        """,
        (player_id, club_id),
    )

    ensure_player_rating(cur, player_id)

    return player_id




@app.get("/")
def root():
    return {"message": "PuntoRank API funcionando"}


@app.get("/clubs")
def get_clubs():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM clubs ORDER BY name;")
            return cur.fetchall()



@app.post("/players")
def create_player(player: PlayerCreate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO players (name, club_id, is_registered, side)
                VALUES (%s, %s, %s, %s)
                RETURNING *;
                """,
                (player.name, player.club_id, player.is_registered, player.side),
            )

            new_player = cur.fetchone()

            if player.club_id is not None:
                cur.execute(
                    """
                    INSERT INTO player_clubs (player_id, club_id, is_home_club)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (player_id, club_id) DO NOTHING;
                    """,
                    (new_player["id"], player.club_id),
                )

            conn.commit()
            return new_player


@app.get("/players")
def get_players():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    p.id,
                    p.name,
                    p.is_registered,
                    p.created_at,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'club_id', c.id,
                                'club_name', c.name,
                                'is_home_club', pc.is_home_club
                            )
                        ) FILTER (WHERE c.id IS NOT NULL),
                        '[]'
                    ) AS clubs
                FROM players p
                LEFT JOIN player_clubs pc ON pc.player_id = p.id
                LEFT JOIN clubs c ON c.id = pc.club_id
                GROUP BY p.id, p.name, p.is_registered, p.created_at
                ORDER BY p.name;
                """
            )
            return cur.fetchall()

@app.post("/matches/report")
def report_match(match: MatchReport):
    if len(match.players) != 4:
        raise HTTPException(
            status_code=400,
            detail="Por ahora cada partido debe tener exactamente 4 jugadores",
        )

    sets = match.score.strip().split()

    if match.match_type == "standard" and len(sets) < 2:
        raise HTTPException(
            status_code=400,
            detail="Un amistoso debe tener al menos 2 sets. Ej: 6-4 7-5",
        )

    teams = [p.team for p in match.players]
    if teams.count("A") != 2 or teams.count("B") != 2:
        raise HTTPException(
            status_code=400,
            detail="Debe haber 2 jugadores en el equipo A y 2 en el equipo B",
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO matches 
                    (club_id, event_id, match_type, status, created_by, played_at, category)
                VALUES 
                    (%s, %s, %s, 'pending', %s, COALESCE(%s::timestamp, NOW()), %s)
                RETURNING *;
                """,
                (
                    match.club_id,
                    match.event_id,
                    match.match_type,
                    match.created_by,
                    match.played_at,
                    match.category
                ),
            )
            new_match = cur.fetchone()
            match_id = new_match["id"]

            for player in match.players:
                player_id = get_or_create_player(cur, player, match.club_id)


                if player_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail="No se pudo identificar o crear uno de los jugadores",
                    )

                cur.execute(
                    """
                    INSERT INTO match_players (match_id, player_id, team)
                    VALUES (%s, %s, %s);
                    """,
                    (match_id, player_id, player.team),
                )


            cur.execute(
                """
                INSERT INTO match_results (match_id, score, winning_team)
                VALUES (%s, %s, %s);
                """,
                (match_id, match.score, match.winning_team),
            )

            conn.commit()

            return {
                "message": "Partido registrado correctamente",
                "match": new_match,
            }



@app.get("/club/{club_id}/matches")
def get_club_matches(club_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.id,
                    m.status,
                    m.match_type,
                    m.played_at,
                    mr.score,
                    mr.winning_team,
                    ce.name AS event_name,

                    -- equipo A
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'A') AS team_a,

                    -- equipo B
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'B') AS team_b

                FROM matches m
                JOIN match_players mp ON mp.match_id = m.id
                JOIN players p ON p.id = mp.player_id
                LEFT JOIN match_results mr ON mr.match_id = m.id
                LEFT JOIN club_events ce ON ce.id = m.event_id

                WHERE m.club_id = %s
                  AND m.status IN ('pending', 'confirmed', 'disputed')

                GROUP BY m.id, m.status, m.match_type, m.played_at, mr.score, mr.winning_team, ce.name

                ORDER BY
                  CASE m.status
                    WHEN 'pending' THEN 1
                    WHEN 'disputed' THEN 2
                    WHEN 'confirmed' THEN 3
                    ELSE 4
                  END,
                  m.played_at DESC;
                """,
                (club_id,),
            )

            return cur.fetchall()


@app.post("/matches/{match_id}/confirm")
def confirm_match(match_id: int, player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            # Verificar que el jugador pertenece al match
            cur.execute("""
                SELECT * FROM match_players
                WHERE match_id = %s AND player_id = %s;
            """, (match_id, player_id))

            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="Jugador no pertenece al partido")

            # Insertar confirmación
            cur.execute("""
                INSERT INTO match_confirmations (match_id, player_id, confirmed)
                VALUES (%s, %s, TRUE)
                ON CONFLICT DO NOTHING;
            """, (match_id, player_id))

            # Contar confirmaciones por equipo
            cur.execute("""
                SELECT mp.team, COUNT(mc.id) as confirmations
                FROM match_players mp
                LEFT JOIN match_confirmations mc 
                    ON mp.player_id = mc.player_id AND mp.match_id = mc.match_id
                WHERE mp.match_id = %s
                GROUP BY mp.team;
            """, (match_id,))

            results = cur.fetchall()

            # Si ambos equipos tienen al menos 1 confirmación → confirmed
            teams_confirmed = [r for r in results if r["confirmations"] > 0]

            if len(teams_confirmed) == 2:
                cur.execute("""
                    UPDATE matches
                    SET status = 'confirmed'
                    WHERE id = %s;
                """, (match_id,))

            conn.commit()

            return {"message": "Confirmación registrada"}


@app.post("/matches/{match_id}/approve")
def approve_match(match_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE matches
                SET status = 'approved'
                WHERE id = %s
                RETURNING id;
                """,
                (match_id,),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Partido no encontrado")

            update_ratings_for_match(cur, match_id)

            cur.execute(
                """
                UPDATE matches
                SET rating_processed = TRUE
                WHERE id = %s;
                """,
                (match_id,),
            )

            conn.commit()

            return {"message": "Partido aprobado y rating actualizado"}


@app.post("/matches/{match_id}/reject")
def reject_match(match_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE matches
                SET status = 'rejected'
                WHERE id = %s;
            """, (match_id,))

            conn.commit()

            return {"message": "Partido rechazado"}


@app.post("/matches/{match_id}/dispute")
def dispute_match(match_id: int, player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE matches
                SET status = 'disputed'
                WHERE id = %s;
            """, (match_id,))

            conn.commit()

            return {"message": "Partido marcado como en disputa"}


@app.get("/ranking")
def get_ranking(
    club_id: int | None = None,
    gender: str | None = None,
    category: str | None = None
):
    query = """
        SELECT
            p.id AS player_id,
            p.name AS player_name,
            c.name AS club_name,
            ROUND(pr.rating, 2) AS rating,
            pr.matches_count,
            p.is_registered,
            p.gender,
            p.category
        FROM player_ratings pr
        JOIN players p ON p.id = pr.player_id
        LEFT JOIN player_clubs pc ON pc.player_id = p.id
        LEFT JOIN clubs c ON c.id = COALESCE(pc.club_id, p.club_id)
        WHERE (%s IS NULL OR COALESCE(pc.club_id, p.club_id) = %s)
          AND (%s IS NULL OR p.gender = %s)
          AND (%s IS NULL OR p.category = %s)
        ORDER BY pr.rating DESC, pr.matches_count DESC, p.name ASC;
    """

    params = [
        club_id, club_id,
        gender, gender,
        category, category,
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()





@app.get("/matches/{match_id}/rating-change")
def get_rating_change(match_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.name,
                    rh.delta,
                    mp.team
                FROM rating_history rh
                JOIN players p ON p.id = rh.player_id
                JOIN match_players mp 
                    ON mp.player_id = rh.player_id 
                    AND mp.match_id = rh.match_id
                WHERE rh.match_id = %s;
                """,
                (match_id,),
            )

            return cur.fetchall()

@app.get("/players/{player_id}/profile")
def get_player_profile(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.id,
                    p.name,
                    p.side,
                    p.is_registered,
                    ROUND(pr.rating, 2) AS rating,
                    pr.matches_count,
                    COALESCE(
                        JSON_AGG(
                            DISTINCT JSONB_BUILD_OBJECT(
                                'club_id', c.id,
                                'club_name', c.name
                            )
                        ) FILTER (WHERE c.id IS NOT NULL),
                        '[]'
                    ) AS clubs
                FROM players p
                LEFT JOIN player_ratings pr ON pr.player_id = p.id
                LEFT JOIN player_clubs pc ON pc.player_id = p.id
                LEFT JOIN clubs c ON c.id = pc.club_id
                WHERE p.id = %s
                GROUP BY p.id, p.name, p.side, p.is_registered, pr.rating, pr.matches_count;
                """,
                (player_id,),
            )

            player = cur.fetchone()

            if not player:
                raise HTTPException(status_code=404, detail="Jugador no encontrado")

            cur.execute(
                """
                SELECT
                    rh.match_id,
                    rh.source_type,
                    rh.source_id,
                    ROUND(rh.rating_before, 2) AS rating_before,
                    ROUND(rh.rating_after, 2) AS rating_after,
                    ROUND(rh.delta, 2) AS delta,
                    rh.created_at
                FROM rating_history rh
                WHERE rh.player_id = %s
                ORDER BY rh.created_at DESC;
                """,
                (player_id,),
            )

            history = cur.fetchall()

            return {
                "player": player,
                "history": history
            }

@app.get("/players/{player_id}/streak")
def get_player_streak(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT delta
                FROM rating_history
                WHERE player_id = %s
                ORDER BY created_at DESC;
                """,
                (player_id,),
            )

            rows = cur.fetchall()

            streak = 0
            for r in rows:
                if r["delta"] > 0:
                    streak += 1
                else:
                    break

            return {"streak": streak}

@app.get("/ranking/top-weekly")
def top_weekly():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.id,
                    p.name,
                    SUM(rh.delta) AS total_delta
                FROM rating_history rh
                JOIN players p ON p.id = rh.player_id
                WHERE rh.created_at >= NOW() - INTERVAL '7 days'
                GROUP BY p.id, p.name
                ORDER BY total_delta DESC
                LIMIT 1;
                """
            )

            result = cur.fetchone()
            return result

@app.post("/register")
def register_player(player: PlayerRegister):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO players (name, email, club_id, gender, side, category, is_registered)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                RETURNING *;
                """,
                (
                    player.name,
                    player.email,
                    player.club_id,
                    player.gender,
                    player.side,
                    player.category,
                ),
            )

            new_player = cur.fetchone()

            if player.club_id is not None:
                cur.execute(
                    """
                    INSERT INTO player_clubs (player_id, club_id, is_home_club)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (player_id, club_id) DO NOTHING;
                    """,
                    (new_player["id"], player.club_id),
                )

            ensure_player_rating(cur, new_player["id"])

            conn.commit()
            return new_player



@app.post("/club/login")
def club_login(login: ClubLogin):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name
                FROM clubs
                WHERE username = %s
                  AND password = %s;
                """,
                (login.username, login.password),
            )

            club = cur.fetchone()

            if not club:
                raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

            return club



@app.get("/club/{club_id}/history")
def get_club_history(club_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.id,
                    m.status,
                    m.match_type,
                    m.category,
                    m.played_at,
                    mr.score,
                    mr.winning_team,
                    ce.name AS event_name,

                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'A') AS team_a,
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'B') AS team_b

                FROM matches m
                JOIN match_players mp ON mp.match_id = m.id
                JOIN players p ON p.id = mp.player_id
                LEFT JOIN match_results mr ON mr.match_id = m.id
                LEFT JOIN club_events ce ON ce.id = m.event_id

                WHERE m.club_id = %s
                  AND m.status IN ('approved', 'rejected')

                GROUP BY m.id, m.status, m.match_type, m.category, m.played_at, mr.score, mr.winning_team, ce.name

                ORDER BY m.played_at DESC;
                """,
                (club_id,),
            )

            return cur.fetchall()

@app.post("/americanos")
def create_americano(data: AmericanoCreate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO americano_events
                    (club_id, name, category, gender, courts, duration_minutes, status)
                VALUES
                    (%s, %s, %s, %s, %s, %s, 'draft')
                RETURNING *;
                """,
                (
                    data.club_id,
                    data.name,
                    data.category,
                    data.gender,
                    data.courts,
                    data.duration_minutes,
                ),
            )

            americano = cur.fetchone()
            conn.commit()
            return americano

@app.post("/americanos/{americano_id}/players")
def add_player_to_americano(americano_id: int, data: AmericanoAddPlayer):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player_id = data.player_id

            if player_id is None:
                if not data.name:
                    raise HTTPException(status_code=400, detail="Debes enviar player_id o name")


                cur.execute(
                    """
                    SELECT club_id
                    FROM americano_events
                    WHERE id = %s;
                    """,
                    (americano_id,),
                )

                americano = cur.fetchone()

                if not americano:
                    raise HTTPException(status_code=404, detail="Americano no encontrado")

                club_id = americano["club_id"]

                cur.execute(
                    """
                    INSERT INTO players
                        (name, email, club_id, gender, category, side, is_registered)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, FALSE)
                    RETURNING id;
                    """,
                    (
                        data.name,
                        data.email,
                        club_id,
                        data.gender,
                        data.category,
                        data.side,
                    ),
                )

                player = cur.fetchone()
                player_id = player["id"]
                ensure_player_rating(cur, player_id)

                cur.execute(
                    """
                    INSERT INTO player_clubs (player_id, club_id, is_home_club)
                    VALUES (%s, %s, FALSE)
                    ON CONFLICT (player_id, club_id) DO NOTHING;
                    """,
                    (player_id, club_id),
                )

            cur.execute(
                """
                INSERT INTO americano_players (americano_id, player_id)
                VALUES (%s, %s)
                RETURNING *;
                """,
                (americano_id, player_id),
            )

            americano_player = cur.fetchone()
            conn.commit()
            return americano_player

@app.get("/americanos/{americano_id}/players")
def get_americano_players(americano_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ap.id,
                    ap.player_id,
                    p.name,
                    p.gender,
                    p.category,
                    p.side,
                    ap.paid
                FROM americano_players ap
                JOIN players p ON p.id = ap.player_id
                WHERE ap.americano_id = %s
                ORDER BY ap.id;
                """,
                (americano_id,),
            )

            return cur.fetchall()

@app.get("/club/{club_id}/americanos")
def get_americanos(club_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ae.id,
                    ae.name,
                    ae.category,
                    ae.gender,
                    ae.courts,
                    ae.duration_minutes,
                    ae.status,
                    ae.created_at,
                    COUNT(ap.id) AS players_count
                FROM americano_events ae
                LEFT JOIN americano_players ap
                    ON ap.americano_id = ae.id
                WHERE ae.club_id = %s
                GROUP BY ae.id
                ORDER BY ae.created_at DESC;
                """,
                (club_id,),
            )

            return cur.fetchall()

@app.get("/americanos/{americano_id}")
def get_americano_detail(americano_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT *
                FROM americano_events
                WHERE id = %s;
                """,
                (americano_id,),
            )

            americano = cur.fetchone()

            if not americano:
                raise HTTPException(status_code=404, detail="Americano no encontrado")

            return americano


@app.post("/americanos/players/{americano_player_id}/toggle-paid")
def toggle_americano_player_paid(americano_player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE americano_players
                SET paid = NOT paid
                WHERE id = %s
                RETURNING *;
                """,
                (americano_player_id,),
            )

            result = cur.fetchone()

            if not result:
                raise HTTPException(status_code=404, detail="Inscripción no encontrada")

            conn.commit()
            return result


@app.post("/americanos/{americano_id}/generate-rounds")
def generate_americano_rounds(americano_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT id, courts, duration_minutes
                FROM americano_events
                WHERE id = %s;
                """,
                (americano_id,),
            )

            americano = cur.fetchone()

            if not americano:
                raise HTTPException(
                    status_code=404,
                    detail="Americano no encontrado",
                )

            cur.execute(
                """
                SELECT id
                FROM americano_pairs
                WHERE americano_id = %s
                ORDER BY id;
                """,
                (americano_id,),
            )

            pairs = [r["id"] for r in cur.fetchall()]

            if len(pairs) < 4:
                raise HTTPException(
                    status_code=400,
                    detail="Se necesitan al menos 4 parejas",
                )

            if len(pairs) > 6:
                raise HTTPException(
                    status_code=400,
                    detail="Por ahora este generador soporta hasta 6 parejas",
                )

            # limpiar rondas anteriores
            cur.execute(
                """
                DELETE FROM americano_rounds
                WHERE americano_id = %s;
                """,
                (americano_id,),
            )

            # ROUND ROBIN REAL
            pairs_work = pairs.copy()

            # si es impar agregamos descanso
            if len(pairs_work) % 2 != 0:
                pairs_work.append(None)

            n = len(pairs_work)

            rounds_needed = n - 1
            courts = int(americano["courts"])
            matches_per_round = n // 2

            recommended_minutes = int(
                americano["duration_minutes"] / rounds_needed
            )

            rounds = []

            for round_number in range(1, rounds_needed + 1):

                round_matches = []

                for i in range(matches_per_round):

                    pair_a = pairs_work[i]
                    pair_b = pairs_work[n - 1 - i]

                    # evitar descanso
                    if pair_a is not None and pair_b is not None:
                        round_matches.append((pair_a, pair_b))

                rounds.append(round_matches)

                # rotación
                pairs_work = (
                    [pairs_work[0]]
                    + [pairs_work[-1]]
                    + pairs_work[1:-1]
                )

            matches_created = 0

            for round_index, round_matches in enumerate(rounds, start=1):

                for court_index, (pair_a, pair_b) in enumerate(
                    round_matches,
                    start=1,
                ):

                    if court_index > courts:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Necesitas al menos {matches_per_round} canchas",
                        )

                    assigned_court = ((court_index + round_index - 2) % courts) + 1

                    cur.execute(
                        """
                        INSERT INTO americano_rounds
                            (americano_id, round_number, court_number)
                        VALUES
                            (%s, %s, %s)
                        RETURNING id;
                        """,
                        (
                            americano_id,
                            round_index,
                            assigned_court,
                        ),
                    )

                    round_row = cur.fetchone()
                    round_id = round_row["id"]

                    cur.execute(
                        """
                        INSERT INTO americano_matches
                            (round_id, pair_a_id, pair_b_id)
                        VALUES
                            (%s, %s, %s);
                        """,
                        (
                            round_id,
                            pair_a,
                            pair_b,
                        ),
                    )

                    matches_created += 1

            cur.execute(
                """
                UPDATE americano_events
                SET status = 'scheduled'
                WHERE id = %s;
                """,
                (americano_id,),
            )

            conn.commit()

            return {
                "message": "Rondas generadas",
                "format": "round_robin",
                "pairs": len(pairs),
                "matches_created": matches_created,
                "rounds_created": rounds_needed,
                "recommended_minutes_per_match": recommended_minutes,
            }




@app.get("/americanos/{americano_id}/matches")
def get_americano_matches(americano_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ar.round_number,
                    ar.court_number,
                    am.id AS match_id,

                    pa.id AS pair_a_id,
                    COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,

                    pb.id AS pair_b_id,
                    COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name,

                    am.score,
                    am.winning_team,
                    am.pair_a_games,
                    am.pair_b_games
                FROM americano_matches am
                JOIN americano_rounds ar ON ar.id = am.round_id

                JOIN americano_pairs pa ON pa.id = am.pair_a_id
                JOIN players p1a ON p1a.id = pa.player_1_id
                JOIN players p2a ON p2a.id = pa.player_2_id

                JOIN americano_pairs pb ON pb.id = am.pair_b_id
                JOIN players p1b ON p1b.id = pb.player_1_id
                JOIN players p2b ON p2b.id = pb.player_2_id

                WHERE ar.americano_id = %s
                ORDER BY ar.round_number, ar.court_number;
                """,
                (americano_id,),
            )

            return cur.fetchall()


@app.post("/americanos/{americano_id}/pairs")
def create_americano_pair(americano_id: int, data: AmericanoPairCreate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO americano_pairs
                    (americano_id, player_1_id, player_2_id, pair_name)
                VALUES
                    (%s, %s, %s, %s)
                RETURNING *;
                """,
                (
                    americano_id,
                    data.player_1_id,
                    data.player_2_id,
                    data.pair_name,
                ),
            )

            pair = cur.fetchone()
            conn.commit()
            return pair

@app.get("/americanos/{americano_id}/pairs")
def get_americano_pairs(americano_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ap.id,
                    ap.pair_name,
                    p1.name AS player_1_name,
                    p2.name AS player_2_name,
                    ap.player_1_id,
                    ap.player_2_id
                FROM americano_pairs ap
                JOIN players p1 ON p1.id = ap.player_1_id
                JOIN players p2 ON p2.id = ap.player_2_id
                WHERE ap.americano_id = %s
                ORDER BY ap.id;
                """,
                (americano_id,),
            )

            return cur.fetchall()


@app.post("/americano-matches/{match_id}/result")
def save_americano_match_result(
    match_id: int,
    data: AmericanoMatchResult,
):

    with get_conn() as conn:
        with conn.cursor() as cur:

            winning_team = None

            if data.pair_a_games > data.pair_b_games:
                winning_team = "A"

            elif data.pair_b_games > data.pair_a_games:
                winning_team = "B"

            cur.execute(
                """
                UPDATE americano_matches
                SET
                    pair_a_games = %s,
                    pair_b_games = %s,
                    winning_team = %s,
                    score = %s
                WHERE id = %s
                RETURNING *;
                """,
                (
                    data.pair_a_games,
                    data.pair_b_games,
                    winning_team,
                    f"{data.pair_a_games}-{data.pair_b_games}",
                    match_id,
                ),
            )

            match = cur.fetchone()

            conn.commit()

            return match


@app.get("/americanos/{americano_id}/standings")
def get_americano_standings(americano_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    ap.id,
                    COALESCE(
                        ap.pair_name,
                        p1.name || ' / ' || p2.name
                    ) AS pair_name,

                    COUNT(am.id) FILTER (
                        WHERE (
                            am.pair_a_id = ap.id
                            AND am.winning_team = 'A'
                        )
                        OR (
                            am.pair_b_id = ap.id
                            AND am.winning_team = 'B'
                        )
                    ) AS wins,

                    COUNT(am.id) FILTER (
                        WHERE (
                            am.pair_a_id = ap.id
                            AND am.winning_team = 'B'
                        )
                        OR (
                            am.pair_b_id = ap.id
                            AND am.winning_team = 'A'
                        )
                    ) AS losses,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN am.pair_a_id = ap.id
                                THEN am.pair_a_games
                                WHEN am.pair_b_id = ap.id
                                THEN am.pair_b_games
                                ELSE 0
                            END
                        ),
                        0
                    ) AS games_won

                FROM americano_pairs ap

                LEFT JOIN americano_matches am
                    ON am.pair_a_id = ap.id
                    OR am.pair_b_id = ap.id

                JOIN players p1
                    ON p1.id = ap.player_1_id

                JOIN players p2
                    ON p2.id = ap.player_2_id

                WHERE ap.americano_id = %s

                GROUP BY
                    ap.id,
                    p1.name,
                    p2.name

                ORDER BY wins DESC, games_won DESC;
                """,
                (americano_id,),
            )

            return cur.fetchall()

@app.post("/americanos/{americano_id}/finish")
def finish_americano(americano_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Verificar americano
            cur.execute(
                """
                SELECT id, status, rating_processed
                FROM americano_events
                WHERE id = %s;
                """,
                (americano_id,),
            )

            americano = cur.fetchone()

            if not americano:
                raise HTTPException(status_code=404, detail="Americano no encontrado")

            if americano["rating_processed"]:
                raise HTTPException(status_code=400, detail="Este americano ya fue procesado")

            # Verificar partidos sin resultado
            cur.execute(
                """
                SELECT COUNT(*) AS pending_results
                FROM americano_matches am
                JOIN americano_rounds ar ON ar.id = am.round_id
                WHERE ar.americano_id = %s
                  AND am.winning_team IS NULL;
                """,
                (americano_id,),
            )

            pending = cur.fetchone()["pending_results"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} partidos por cargar resultado",
                )


# Procesar rating de partidos americano
            cur.execute(
                """
                SELECT
                    am.id,
                    am.winning_team,

                    pa.player_1_id AS a1,
                    pa.player_2_id AS a2,

                    pb.player_1_id AS b1,
                    pb.player_2_id AS b2

                FROM americano_matches am

                JOIN americano_rounds ar
                    ON ar.id = am.round_id

                JOIN americano_pairs pa
                    ON pa.id = am.pair_a_id

                JOIN americano_pairs pb
                    ON pb.id = am.pair_b_id

                WHERE ar.americano_id = %s;
                """,
                (americano_id,),
            )

            matches = cur.fetchall()

            for match in matches:

                update_rating_pair_vs_pair(
                    cur=cur,
                    player_ids_team_a=[
                        match["a1"],
                        match["a2"],
                    ],
                    player_ids_team_b=[
                        match["b1"],
                        match["b2"],
                    ],
                    winner=match["winning_team"],
                    match_id=None,
                    source_type="americano_match",
                    source_id=match["id"],
                    multiplier=1.2,
                )

# Bonus de podio del americano
            cur.execute(
                """
                SELECT
                    ap.id AS pair_id,
                    ap.player_1_id,
                    ap.player_2_id,

                    COUNT(am.id) FILTER (
                        WHERE (
                            am.pair_a_id = ap.id
                            AND am.winning_team = 'A'
                        )
                        OR (
                            am.pair_b_id = ap.id
                            AND am.winning_team = 'B'
                        )
                    ) AS wins,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN am.pair_a_id = ap.id THEN am.pair_a_games
                                WHEN am.pair_b_id = ap.id THEN am.pair_b_games
                                ELSE 0
                            END
                        ),
                        0
                    ) AS games_won

                FROM americano_pairs ap

                LEFT JOIN americano_matches am
                    ON am.pair_a_id = ap.id
                    OR am.pair_b_id = ap.id

                WHERE ap.americano_id = %s

                GROUP BY
                    ap.id,
                    ap.player_1_id,
                    ap.player_2_id

                ORDER BY wins DESC, games_won DESC;
                """,
                (americano_id,),
            )

            standings = cur.fetchall()

            podium_bonus = {
                0: 10,
                1: 5,
                2: 2,
            }

            for index, row in enumerate(standings[:3]):
                bonus = podium_bonus.get(index, 0)

                if bonus > 0:
                    apply_rating_bonus(
                        cur,
                        row["player_1_id"],
                        bonus,
                        "americano_podium",
                        americano_id,
                    )

                    apply_rating_bonus(
                        cur,
                        row["player_2_id"],
                        bonus,
                        "americano_podium",
                        americano_id,
                    )

            # Por ahora solo cerramos el americano.
            # En el siguiente paso conectamos impacto de rating.
            cur.execute(
                """
                UPDATE americano_events
                SET status = 'completed',
                    rating_processed = TRUE
                WHERE id = %s
                RETURNING *;
                """,
                (americano_id,),
            )

            result = cur.fetchone()
            conn.commit()

            return {
                "message": "Americano finalizado correctamente",
                "americano": result,
            }

@app.get("/players/{player_id}/matches-history")
def get_player_matches_history(player_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            # Amistosos
            cur.execute(
                """
                SELECT
                    'friendly_match' AS source_type,
                    m.id AS source_id,
                    m.played_at AS played_at,
                    m.category,
                    m.match_type,
                    c.name AS club_name,
                    mr.score,
                    mr.winning_team,
                    (
                        SELECT mp2.team
                        FROM match_players mp2
                        WHERE mp2.match_id = m.id
                          AND mp2.player_id = %s
                        LIMIT 1
                    ) AS player_team,
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'A') AS team_a,
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'B') AS team_b
                FROM matches m
                JOIN match_players mp ON mp.match_id = m.id
                JOIN players p ON p.id = mp.player_id
                LEFT JOIN match_results mr ON mr.match_id = m.id
                LEFT JOIN clubs c ON c.id = m.club_id
                WHERE m.id IN (
                    SELECT match_id
                    FROM match_players
                    WHERE player_id = %s
                )
                GROUP BY
                    m.id,
                    m.played_at,
                    m.category,
                    m.match_type,
                    c.name,
                    mr.score,
                    mr.winning_team
                ORDER BY m.played_at DESC;
                """,
                (player_id, player_id),
            )

            friendly = cur.fetchall()

            # Americanos
            cur.execute(
                """
                SELECT
                    'americano_match' AS source_type,
                    am.id AS source_id,
                    ae.created_at AS played_at,
                    ae.category,
                    'americano' AS match_type,
                    c.name AS club_name,
                    am.score,
                    am.winning_team,

                    COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,
                    COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name,

                    pa.player_1_id AS a1,
                    pa.player_2_id AS a2,
                    pb.player_1_id AS b1,
                    pb.player_2_id AS b2

                FROM americano_matches am
                JOIN americano_rounds ar ON ar.id = am.round_id
                JOIN americano_events ae ON ae.id = ar.americano_id
                LEFT JOIN clubs c ON c.id = ae.club_id

                JOIN americano_pairs pa ON pa.id = am.pair_a_id
                JOIN players p1a ON p1a.id = pa.player_1_id
                JOIN players p2a ON p2a.id = pa.player_2_id

                JOIN americano_pairs pb ON pb.id = am.pair_b_id
                JOIN players p1b ON p1b.id = pb.player_1_id
                JOIN players p2b ON p2b.id = pb.player_2_id

                WHERE %s IN (
                    pa.player_1_id,
                    pa.player_2_id,
                    pb.player_1_id,
                    pb.player_2_id
                )
                ORDER BY ae.created_at DESC, am.id DESC;
                """,
                (player_id,),
            )

            americanos = cur.fetchall()

            return {
                "friendly": friendly,
                "americanos": americanos,
            }
