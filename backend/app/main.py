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
def get_ranking(club_id: int | None = None, gender: str | None = None, category: str | None = None):
    if club_id is None:
        query = """
            SELECT
                p.id AS player_id,
                p.name AS player_name,
                c.name AS club_name,
                ROUND(pr.rating, 2) AS rating,
                pr.matches_count,
                p.is_registered
            FROM player_ratings pr
            JOIN players p ON p.id = pr.player_id
            LEFT JOIN clubs c ON c.id = p.club_id
            WHERE (%s IS NULL OR p.gender = %s)
            ORDER BY pr.rating DESC, pr.matches_count DESC, p.name ASC;
            AND (%s IS NULL OR EXISTS (
                SELECT 1
                FROM matches m
                JOIN match_players mp ON mp.match_id = m.id
                WHERE mp.player_id = p.id
                  AND m.category = %s
            ))
        """
        params = [gender, gender]
    else:
        query = """
            SELECT
                p.id AS player_id,
                p.name AS player_name,
                c.name AS club_name,
                ROUND(pr.rating, 2) AS rating,
                COUNT(DISTINCT m.id) AS matches_count,
                p.is_registered
            FROM players p
            JOIN player_ratings pr ON pr.player_id = p.id
            JOIN match_players mp ON mp.player_id = p.id
            JOIN matches m ON m.id = mp.match_id
            LEFT JOIN clubs c ON c.id = p.club_id
            WHERE m.club_id = %s
              AND (%s IS NULL OR p.gender = %s)
              AND m.status IN ('confirmed', 'approved')
              AND m.rating_processed = TRUE
            GROUP BY p.id, p.name, c.name, pr.rating, p.is_registered
            ORDER BY pr.rating DESC, matches_count DESC, p.name ASC;
        """
        params = [club_id, gender, gender]

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
                    INSERT INTO players
                        (name, email, gender, category, side, is_registered)
                    VALUES
                        (%s, %s, %s, %s, %s, FALSE)
                    RETURNING id;
                    """,
                    (
                        data.name,
                        data.email,
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
                raise HTTPException(status_code=404, detail="Americano no encontrado")

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
                    detail="Por ahora este generador soporta hasta 6 parejas. Luego agregaremos grupos + playoffs.",
                )

            # limpiar rondas anteriores
            cur.execute(
                """
                DELETE FROM americano_rounds
                WHERE americano_id = %s;
                """,
                (americano_id,),
            )



# round-robin real: una pareja juega solo una vez por ronda
pairs_work = pairs.copy()

# Si hubiera número impar de parejas, agregamos descanso
if len(pairs_work) % 2 != 0:
    pairs_work.append(None)

n = len(pairs_work)
rounds_needed = n - 1
courts = int(americano["courts"])
matches_per_round = n // 2
total_matches = (len(pairs) * (len(pairs) - 1)) // 2

recommended_minutes = int(americano["duration_minutes"] / rounds_needed)

rounds = []

for round_number in range(1, rounds_needed + 1):
    round_matches = []

    for i in range(matches_per_round):
        pair_a = pairs_work[i]
        pair_b = pairs_work[n - 1 - i]

        # Si hay descanso, no se crea match
        if pair_a is not None and pair_b is not None:
            round_matches.append((pair_a, pair_b))

    rounds.append(round_matches)

    # rotación round-robin manteniendo fijo el primero
    pairs_work = [pairs_work[0]] + [pairs_work[-1]] + pairs_work[1:-1]


matches_created = 0

for round_index, round_matches in enumerate(rounds, start=1):

    for court_index, (pair_a, pair_b) in enumerate(round_matches, start=1):

        if court_index > courts:
            raise HTTPException(
                status_code=400,
                detail=f"Necesitas al menos {matches_per_round} canchas para que nadie descanse."
            )

        cur.execute(
            """
            INSERT INTO americano_rounds
                (americano_id, round_number, court_number)
            VALUES
                (%s, %s, %s)
            RETURNING id;
            """,
            (americano_id, round_index, court_index),
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
            (round_id, pair_a, pair_b),
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
                    am.winning_team
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
