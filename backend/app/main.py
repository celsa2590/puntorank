import os
import psycopg2
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
        "http://127.0.0.1:8080"
        "https://puntorank-frontend.onrender.com"
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
                    (club_id, event_id, match_type, status, created_by, played_at)
                VALUES 
                    (%s, %s, %s, 'pending', %s, COALESCE(%s::timestamp, NOW()))
                RETURNING *;
                """,
                (
                    match.club_id,
                    match.event_id,
                    match.match_type,
                    match.created_by,
                    match.played_at,
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

                GROUP BY m.id, m.status, m.match_type, m.played_at, mr.score, mr.winning_team, ce.name

                ORDER BY m.played_at DESC;
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
def get_ranking(club_id: int | None = None):
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
            ORDER BY pr.rating DESC, pr.matches_count DESC, p.name ASC;
        """
        params = []
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
              AND m.status IN ('confirmed', 'approved')
              AND m.rating_processed = TRUE
            GROUP BY p.id, p.name, c.name, pr.rating, p.is_registered
            ORDER BY pr.rating DESC, matches_count DESC, p.name ASC;
        """
        params = [club_id]

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
