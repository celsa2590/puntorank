import os
import psycopg2
import random
import hashlib
import secrets
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from fastapi.middleware.cors import CORSMiddleware
from app.services.email_service import send_email
from app.database import get_conn
from app.schemas import *
from app.services.rating_service import (
    get_match_weight,
    expected_score,
    ensure_player_rating,
    update_ratings_for_match,
    update_rating_pair_vs_pair,
    apply_rating_bonus,
    get_rating_multiplier,
)
from app.services.auth_service import (
    hash_password,
    verify_password,
    hash_session_token,
    generate_token,
)
from app.config import FRONTEND_URL, PASSWORD_RESET_MINUTES
from app.services.notification_service import (
    notify_password_reset,
)
from app.services.notification_service import notify_password_reset
from app.services.match_service import (
    register_match_metadata,
    notify_friendly_match_players,
    requires_confirmation,
)
from app.services.auth_service import hash_session_token
from app.routers.matches import router as matches_router

load_dotenv()

app = FastAPI(title="PuntoRank API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "https://puntorank-frontend.onrender.com",
        "https://puntorank-backend.onrender.com",
        "https://puntorank.cl",
        "https://www.puntorank.cl",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(matches_router)

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

@app.get("/email/test")
def test_email(to: str):
    result = send_email(
        to_email=to,
        subject="Prueba PuntoRank",
        html="""
        <h1>PuntoRank</h1>
        <p>Correo de prueba funcionando ✅</p>
        """,
        text="Correo de prueba funcionando"
    )

    return {
        "message": "Correo enviado",
        "result": result
    }

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

            created_by_player_id = match.created_by

            register_match_metadata(
                cur=cur,
                match_id=match_id,
                created_by_player_id=created_by_player_id,
                match_source="friendly",
            )

            emails_sent = 0

            if requires_confirmation("friendly"):
                emails_sent = notify_friendly_match_players(
                    cur=cur,
                    match_id=match_id,
                    created_by_player_id=created_by_player_id,
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
                    p.category,
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
                GROUP BY p.id, p.name, p.side, p.category, p.is_registered, pr.rating, pr.matches_count;
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
                    multiplier=get_rating_multiplier("americano_match")
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
                    ROUND(rh.delta, 2) AS delta,
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
                LEFT JOIN rating_history rh ON rh.player_id = %s AND rh.match_id = m.id
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
                    mr.winning_team,
                    rh.delta
                ORDER BY m.played_at DESC;
                """,
                (player_id, player_id, player_id),
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
                    ROUND(rh.delta, 2) AS delta,

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

                LEFT JOIN rating_history rh
                    ON rh.player_id = %s
                   AND rh.source_type = 'americano_match'
                   AND rh.source_id = am.id

                WHERE %s IN (
                    pa.player_1_id,
                    pa.player_2_id,
                    pb.player_1_id,
                    pb.player_2_id
                )
                ORDER BY ae.created_at DESC, am.id DESC;
                """,
                (player_id, player_id),
            )

            americanos = cur.fetchall()

            # Ligas
            cur.execute(
                """
                SELECT
                    'league_match' AS source_type,
                    lm.id AS source_id,
                    COALESCE(lm.played_at, ls.created_at) AS played_at,
                    ls.category,
                    'liga' AS match_type,
                    c.name AS club_name,
                    lm.score,
                    lm.winner_pair_id,
                    ROUND(rh.delta, 2) AS delta,

                    COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,
                    COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name,

                    pa.id AS pair_a_id,
                    pb.id AS pair_b_id,

                    pa.player_1_id AS a1,
                    pa.player_2_id AS a2,
                    pb.player_1_id AS b1,
                    pb.player_2_id AS b2

                FROM league_matches lm
                JOIN league_seasons ls ON ls.id = lm.league_id
                LEFT JOIN clubs c ON c.id = ls.club_id

                JOIN league_pairs pa ON pa.id = lm.pair_a_id
                JOIN players p1a ON p1a.id = pa.player_1_id
                JOIN players p2a ON p2a.id = pa.player_2_id

                JOIN league_pairs pb ON pb.id = lm.pair_b_id
                JOIN players p1b ON p1b.id = pb.player_1_id
                JOIN players p2b ON p2b.id = pb.player_2_id

                LEFT JOIN rating_history rh
                    ON rh.player_id = %s
                   AND rh.source_type = 'league_match'
                   AND rh.source_id = lm.id

                WHERE %s IN (
                    pa.player_1_id,
                    pa.player_2_id,
                    pb.player_1_id,
                    pb.player_2_id
                )
                  AND lm.status = 'completed'
                ORDER BY COALESCE(lm.played_at, ls.created_at) DESC, lm.id DESC;
                """,
                (player_id, player_id),
            )

            leagues = cur.fetchall()

            return {
                "friendly": friendly,
                "americanos": americanos,
                "leagues": leagues,
            }

@app.post("/leagues")
def create_league(data: LeagueCreate):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO league_seasons
                    (
                        club_id,
                        name,
                        category,
                        gender,
                        format,
                        start_date,
                        end_date,
                        status
                    )
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, 'draft')
                RETURNING *;
                """,
                (
                    data.club_id,
                    data.name,
                    data.category,
                    data.gender,
                    data.format,
                    data.start_date,
                    data.end_date,
                ),
            )

            league = cur.fetchone()

            conn.commit()

            return league

@app.get("/club/{club_id}/leagues")
def get_club_leagues(club_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    ls.*,

                    COUNT(lp.id) AS pairs_count

                FROM league_seasons ls

                LEFT JOIN league_pairs lp
                    ON lp.league_id = ls.id

                WHERE ls.club_id = %s

                GROUP BY ls.id

                ORDER BY ls.created_at DESC;
                """,
                (club_id,),
            )

            return cur.fetchall()

@app.post("/leagues/{league_id}/generate-fixture")
def generate_league_fixture(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT id
                FROM league_pairs
                WHERE league_id = %s
                ORDER BY id;
                """,
                (league_id,),
            )

            pairs = [r["id"] for r in cur.fetchall()]

            if len(pairs) < 2:
                raise HTTPException(
                    status_code=400,
                    detail="Se necesitan al menos 2 parejas para generar fixture",
                )

            cur.execute(
                """
                DELETE FROM league_matches
                WHERE league_id = %s;
                """,
                (league_id,),
            )

            pairs_work = pairs.copy()

            if len(pairs_work) % 2 != 0:
                pairs_work.append(None)

            n = len(pairs_work)
            rounds_needed = n - 1
            matches_per_round = n // 2

            matches_created = 0

            for round_number in range(1, rounds_needed + 1):

                for i in range(matches_per_round):

                    pair_a = pairs_work[i]
                    pair_b = pairs_work[n - 1 - i]

                    if pair_a is not None and pair_b is not None:
                        cur.execute(
                            """
                            INSERT INTO league_matches
                                (league_id, round_number, pair_a_id, pair_b_id)
                            VALUES
                                (%s, %s, %s, %s);
                            """,
                            (
                                league_id,
                                round_number,
                                pair_a,
                                pair_b,
                            ),
                        )

                        matches_created += 1

                pairs_work = (
                    [pairs_work[0]]
                    + [pairs_work[-1]]
                    + pairs_work[1:-1]
                )

            cur.execute(
                """
                UPDATE league_seasons
                SET status = 'scheduled'
                WHERE id = %s;
                """,
                (league_id,),
            )

            conn.commit()

            return {
                "message": "Fixture generado",
                "rounds_created": rounds_needed,
                "matches_created": matches_created,
            }

@app.get("/leagues/{league_id}/matches")
def get_league_matches(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    lm.id,
                    lm.round_number,
                    lm.phase,
                    lm.cup,
                    lm.bracket_round,
                    lm.score,
                    lm.status,
                    lm.played_at,

                    pa.id AS pair_a_id,
                    COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,

                    pb.id AS pair_b_id,
                    COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name,

                    lm.winner_pair_id

                FROM league_matches lm

                JOIN league_pairs pa
                    ON pa.id = lm.pair_a_id

                JOIN players p1a
                    ON p1a.id = pa.player_1_id

                JOIN players p2a
                    ON p2a.id = pa.player_2_id

                JOIN league_pairs pb
                    ON pb.id = lm.pair_b_id

                JOIN players p1b
                    ON p1b.id = pb.player_1_id

                JOIN players p2b
                    ON p2b.id = pb.player_2_id

                WHERE lm.league_id = %s

                ORDER BY
                  CASE
                    WHEN lm.phase = 'regular' THEN 1
                    WHEN lm.phase = 'playoff' AND lm.bracket_round = 'semifinal' THEN 2
                    WHEN lm.phase = 'playoff' AND lm.bracket_round = 'final' THEN 3
                    ELSE 4
                  END,
                  lm.round_number,
                  lm.cup,
                  lm.id;
                """,
                (league_id,),
            )

            return cur.fetchall()

@app.post("/leagues/{league_id}/pairs")
def create_league_pair(league_id: int, data: LeaguePairCreate):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO league_pairs
                    (
                        league_id,
                        player_1_id,
                        player_2_id,
                        pair_name
                    )
                VALUES
                    (%s, %s, %s, %s)
                RETURNING *;
                """,
                (
                    league_id,
                    data.player_1_id,
                    data.player_2_id,
                    data.pair_name,
                ),
            )

            pair = cur.fetchone()

            conn.commit()

            return pair

@app.get("/leagues/{league_id}/pairs")
def get_league_pairs(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    lp.id,
                    lp.pair_name,

                    p1.name AS player_1_name,
                    p2.name AS player_2_name,

                    lp.player_1_id,
                    lp.player_2_id

                FROM league_pairs lp

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                WHERE lp.league_id = %s

                ORDER BY lp.id;
                """,
                (league_id,),
            )

            return cur.fetchall()


@app.post("/league-matches/{match_id}/result")
def save_league_match_result(match_id: int, data: LeagueMatchResult):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE league_matches
                SET
                    score = %s,
                    winner_pair_id = %s,
                    status = 'completed',
                    played_at = NOW()
                WHERE id = %s
                RETURNING *;
                """,
                (
                    data.score,
                    data.winner_pair_id,
                    match_id,
                ),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(status_code=404, detail="Partido de liga no encontrado")

            conn.commit()

            return match

@app.get("/leagues/{league_id}/standings")
def get_league_standings(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    lp.id AS pair_id,
                    COALESCE(lp.pair_name, p1.name || ' / ' || p2.name) AS pair_name,

                    COUNT(lm.id) FILTER (
                        WHERE lm.winner_pair_id = lp.id
                    ) AS wins,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id IS NOT NULL
                          AND lm.winner_pair_id <> lp.id
                    ) AS losses,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                    ) AS played,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.winner_pair_id = lp.id THEN 3
                                ELSE 0
                            END
                        ),
                        0
                    ) AS points

                FROM league_pairs lp

                LEFT JOIN league_matches lm
                    ON lm.pair_a_id = lp.id
                    OR lm.pair_b_id = lp.id

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                WHERE lp.league_id = %s

                GROUP BY
                    lp.id,
                    lp.pair_name,
                    p1.name,
                    p2.name

                ORDER BY points DESC, wins DESC, pair_name ASC;
                """,
                (league_id,),
            )

            return cur.fetchall()

@app.post("/leagues/{league_id}/finish")
def finish_league(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT id, status
                FROM league_seasons
                WHERE id = %s;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(status_code=404, detail="Liga no encontrada")

            if league["status"] == "completed":
                raise HTTPException(status_code=400, detail="Esta liga ya fue finalizada")

            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM league_matches
                WHERE league_id = %s
                  AND status <> 'completed';
                """,
                (league_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} partidos por completar",
                )

            cur.execute(
                """
                SELECT
                    lm.id,
                    lm.winner_pair_id,
                    lm.rating_processed,

                    pa.id AS pair_a_id,
                    pa.player_1_id AS a1,
                    pa.player_2_id AS a2,

                    pb.id AS pair_b_id,
                    pb.player_1_id AS b1,
                    pb.player_2_id AS b2

                FROM league_matches lm

                JOIN league_pairs pa
                    ON pa.id = lm.pair_a_id

                JOIN league_pairs pb
                    ON pb.id = lm.pair_b_id

                WHERE lm.league_id = %s;
                """,
                (league_id,),
            )

            matches = cur.fetchall()

            for match in matches:

                if match["rating_processed"]:
                    continue

                winner = "A" if match["winner_pair_id"] == match["pair_a_id"] else "B"

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
                    winner=winner,
                    match_id=None,
                    source_type="league_match",
                    source_id=match["id"],
                    multiplier=get_rating_multiplier("league_match")
                )

                cur.execute(
                    """
                    UPDATE league_matches
                    SET rating_processed = TRUE
                    WHERE id = %s;
                    """,
                    (match["id"],),
                )

            cur.execute(
                """
                UPDATE league_seasons
                SET status = 'completed'
                WHERE id = %s
                RETURNING *;
                """,
                (league_id,),
            )

            result = cur.fetchone()

            conn.commit()

            return {
                "message": "Liga finalizada correctamente",
                "league": result,
            }

@app.get("/clubs/{club_id}/players")
def get_club_players(club_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.id,
                    p.name,
                    p.email,
                    p.gender,
                    p.category,
                    p.side,
                    p.is_registered,
                    ROUND(COALESCE(pr.rating, 1000), 2) AS rating
                FROM players p
                LEFT JOIN player_ratings pr
                    ON pr.player_id = p.id
                LEFT JOIN player_clubs pc
                    ON pc.player_id = p.id
                WHERE p.club_id = %s
                   OR pc.club_id = %s
                GROUP BY
                    p.id,
                    p.name,
                    p.email,
                    p.gender,
                    p.category,
                    p.side,
                    p.is_registered,
                    pr.rating
                ORDER BY p.name;
                """,
                (club_id, club_id),
            )

            return cur.fetchall()

@app.post("/leagues/{league_id}/generate-playoffs")
def generate_league_playoffs(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            # Verificar que todos los partidos regulares estén completos
            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'regular'
                  AND status <> 'completed';
                """,
                (league_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} partidos de fase regular",
                )

            # Evitar duplicar playoffs
            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'playoff';
                """,
                (league_id,),
            )

            existing = cur.fetchone()["existing"]

            if existing > 0:
                raise HTTPException(
                    status_code=400,
                    detail="Los playoffs ya fueron generados",
                )

            # Obtener standings
            cur.execute(
                """
                SELECT
                    lp.id AS pair_id,

                    COUNT(lm.id) FILTER (
                        WHERE lm.winner_pair_id = lp.id
                    ) AS wins,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.winner_pair_id = lp.id THEN 3
                                ELSE 0
                            END
                        ),
                        0
                    ) AS points

                FROM league_pairs lp

                LEFT JOIN league_matches lm
                    ON (
                        lm.pair_a_id = lp.id
                        OR lm.pair_b_id = lp.id
                    )
                   AND lm.phase = 'regular'
                   AND lm.status = 'completed'

                WHERE lp.league_id = %s

                GROUP BY lp.id

                ORDER BY points DESC, wins DESC, lp.id ASC;
                """,
                (league_id,),
            )

            standings = cur.fetchall()

            if len(standings) < 8:
                raise HTTPException(
                    status_code=400,
                    detail="Se necesitan al menos 8 parejas para generar Copa Oro y Copa Plata",
                )

            oro = standings[:4]
            plata = standings[4:8]

            playoff_matches = [
                ("oro", oro[0]["pair_id"], oro[3]["pair_id"]),
                ("oro", oro[1]["pair_id"], oro[2]["pair_id"]),
                ("plata", plata[0]["pair_id"], plata[3]["pair_id"]),
                ("plata", plata[1]["pair_id"], plata[2]["pair_id"]),
            ]

            created = 0

            for cup, pair_a, pair_b in playoff_matches:
                cur.execute(
                    """
                    INSERT INTO league_matches
                        (
                            league_id,
                            phase,
                            cup,
                            bracket_round,
                            round_number,
                            pair_a_id,
                            pair_b_id,
                            status
                        )
                    VALUES
                        (%s, 'playoff', %s, 'semifinal', 1, %s, %s, 'scheduled');
                    """,
                    (
                        league_id,
                        cup,
                        pair_a,
                        pair_b,
                    ),
                )

                created += 1

            conn.commit()

            return {
                "message": "Playoffs generados",
                "matches_created": created,
            }
@app.post("/leagues/{league_id}/generate-finals")
def generate_league_finals(league_id: int):

    with get_conn() as conn:
        with conn.cursor() as cur:

            # Verificar semifinales pendientes
            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'semifinal'
                  AND status <> 'completed';
                """,
                (league_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} semifinales por completar",
                )

            # Evitar duplicar finales
            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'final';
                """,
                (league_id,),
            )

            existing = cur.fetchone()["existing"]

            if existing > 0:
                raise HTTPException(
                    status_code=400,
                    detail="Las finales ya fueron generadas",
                )

            finals_created = 0

            for cup in ["oro", "plata"]:

                cur.execute(
                    """
                    SELECT winner_pair_id
                    FROM league_matches
                    WHERE league_id = %s
                      AND phase = 'playoff'
                      AND bracket_round = 'semifinal'
                      AND cup = %s
                    ORDER BY id;
                    """,
                    (league_id, cup),
                )

                winners = cur.fetchall()

                if len(winners) != 2:
                    raise HTTPException(
                        status_code=400,
                        detail=f"No hay 2 semifinales completas para copa {cup}",
                    )

                pair_a = winners[0]["winner_pair_id"]
                pair_b = winners[1]["winner_pair_id"]

                cur.execute(
                    """
                    INSERT INTO league_matches
                        (
                            league_id,
                            phase,
                            cup,
                            bracket_round,
                            round_number,
                            pair_a_id,
                            pair_b_id,
                            status
                        )
                    VALUES
                        (%s, 'playoff', %s, 'final', 2, %s, %s, 'scheduled');
                    """,
                    (
                        league_id,
                        cup,
                        pair_a,
                        pair_b,
                    ),
                )

                finals_created += 1

            conn.commit()

            return {
                "message": "Finales generadas",
                "finals_created": finals_created,
            }


# =========================
# Torneos - fase de grupos
# =========================


def parse_padel_score(score: str):
    """Convierte '6-4 4-6 10-8' en sets/games por pareja."""
    sets_a = sets_b = games_a = games_b = 0

    for raw_set in score.strip().split():
        if "-" not in raw_set:
            continue

        left, right = raw_set.split("-", 1)

        try:
            a = int(left)
            b = int(right)
        except ValueError:
            continue

        games_a += a
        games_b += b

        if a > b:
            sets_a += 1
        elif b > a:
            sets_b += 1

    return sets_a, sets_b, games_a, games_b



@app.post("/tournament_events")
def create_tournament(data: TournamentCreate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tournament_events
                    (club_id, name, category, gender, format, status)
                VALUES
                    (%s, %s, %s, %s, 'group_stage', 'registration')
                RETURNING *;
                """,
                (data.club_id, data.name, data.category, data.gender),
            )

            tournament = cur.fetchone()
            conn.commit()
            return tournament


@app.get("/club/{club_id}/tournament_events")
def get_club_tournaments(club_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    te.*,
                    COUNT(DISTINCT tp.id) AS pairs_count,
                    COUNT(DISTINCT tg.id) AS groups_count,
                    COUNT(DISTINCT tm.id) AS matches_count
                FROM tournament_events te
                LEFT JOIN tournament_pairs tp
                    ON tp.tournament_id = te.id
                LEFT JOIN tournament_groups tg
                    ON tg.tournament_id = te.id
                LEFT JOIN tournament_matches tm
                    ON tm.tournament_id = te.id
                WHERE te.club_id = %s
                GROUP BY te.id
                ORDER BY te.created_at DESC;
                """,
                (club_id,),
            )

            return cur.fetchall()


@app.get("/tournament_events/{tournament_id}")
def get_tournament(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM tournament_events
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            tournament = cur.fetchone()

            if not tournament:
                raise HTTPException(status_code=404, detail="Torneo no encontrado")

            return tournament


@app.post("/tournament_events/{tournament_id}/pairs")
def create_tournament_pair(tournament_id: int, data: TournamentPairCreate):
    if data.player_1_id == data.player_2_id:
        raise HTTPException(
            status_code=400,
            detail="La pareja debe tener dos jugadores distintos",
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM tournament_events
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Torneo no encontrado")

            cur.execute(
                """
                INSERT INTO tournament_pairs
                    (
                        tournament_id,
                        player_1_id,
                        player_2_id,
                        pair_name,
                        payment_status,
                        payment_method,
                        payment_link,
                        payment_reference,
                        payment_amount
                    )
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *;
                """,
                (
                    tournament_id,
                    data.player_1_id,
                    data.player_2_id,
                    data.pair_name,
                    data.payment_status,
                    data.payment_method,
                    data.payment_link,
                    data.payment_reference,
                    data.payment_amount,
                ),
            )

            pair = cur.fetchone()
            conn.commit()
            return pair


@app.get("/tournament_events/{tournament_id}/pairs")
def get_tournament_pairs(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tp.*,
                    tg.name AS group_name,
                    p1.name AS player_1_name,
                    p2.name AS player_2_name,
                    COALESCE(tp.pair_name, p1.name || ' / ' || p2.name) AS display_name
                FROM tournament_pairs tp
                JOIN players p1
                    ON p1.id = tp.player_1_id
                JOIN players p2
                    ON p2.id = tp.player_2_id
                LEFT JOIN tournament_group_members tgm
                    ON tgm.pair_id = tp.id
                LEFT JOIN tournament_groups tg
                    ON tg.id = tgm.group_id
                WHERE tp.tournament_id = %s
                ORDER BY tg.name NULLS LAST, tp.id;
                """,
                (tournament_id,),
            )

            return cur.fetchall()


@app.patch("/tournament-pairs/{pair_id}/payment")
def update_tournament_pair_payment(pair_id: int, data: TournamentPaymentUpdate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE tournament_pairs
                SET payment_status = %s,
                    payment_method = %s,
                    payment_link = %s,
                    payment_reference = %s,
                    payment_amount = %s
                WHERE id = %s
                RETURNING *;
                """,
                (
                    data.payment_status,
                    data.payment_method,
                    data.payment_link,
                    data.payment_reference,
                    data.payment_amount,
                    pair_id,
                ),
            )

            pair = cur.fetchone()

            if not pair:
                raise HTTPException(status_code=404, detail="Pareja no encontrada")

            conn.commit()
            return pair


@app.post("/tournament_events/{tournament_id}/generate-groups")
def generate_tournament_groups(tournament_id: int, data: TournamentGenerateGroups):
    if data.groups_count < 1:
        raise HTTPException(status_code=400, detail="Debes indicar al menos 1 grupo")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM tournament_events
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Torneo no encontrado")

            cur.execute(
                """
                SELECT id
                FROM tournament_pairs
                WHERE tournament_id = %s
                ORDER BY seed NULLS LAST, id;
                """,
                (tournament_id,),
            )

            pairs = [r["id"] for r in cur.fetchall()]

            if len(pairs) < data.groups_count * 2:
                raise HTTPException(
                    status_code=400,
                    detail="Cada grupo debe tener al menos 2 parejas",
                )

            # Limpiar estructura anterior del torneo.
            cur.execute(
                """
                DELETE FROM tournament_matches
                WHERE tournament_id = %s;
                """,
                (tournament_id,),
            )

            cur.execute(
                """
                DELETE FROM tournament_groups
                WHERE tournament_id = %s;
                """,
                (tournament_id,),
            )

            groups = []

            for i in range(data.groups_count):
                name = chr(ord("A") + i)
                cur.execute(
                    """
                    INSERT INTO tournament_groups (tournament_id, name)
                    VALUES (%s, %s)
                    RETURNING *;
                    """,
                    (tournament_id, name),
                )

                groups.append(cur.fetchone())

            for idx, pair_id in enumerate(pairs):
                group_id = groups[idx % data.groups_count]["id"]
                cur.execute(
                    """
                    INSERT INTO tournament_group_members (group_id, pair_id)
                    VALUES (%s, %s)
                    ON CONFLICT (group_id, pair_id) DO NOTHING;
                    """,
                    (group_id, pair_id),
                )

            cur.execute(
                """
                UPDATE tournament_events
                SET status = 'groups_ready'
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            conn.commit()

            return {
                "message": "Grupos generados",
                "groups_created": len(groups),
                "pairs_assigned": len(pairs),
            }


@app.post("/tournament_events/{tournament_id}/generate-group-matches")
def generate_tournament_group_matches(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name
                FROM tournament_groups
                WHERE tournament_id = %s
                ORDER BY name;
                """,
                (tournament_id,),
            )

            groups = cur.fetchall()

            if not groups:
                raise HTTPException(status_code=400, detail="Primero debes generar grupos")

            cur.execute(
                """
                DELETE FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'group_stage';
                """,
                (tournament_id,),
            )

            created = 0

            for group in groups:
                cur.execute(
                    """
                    SELECT tp.id
                    FROM tournament_group_members tgm
                    JOIN tournament_pairs tp
                        ON tp.id = tgm.pair_id
                    WHERE tgm.group_id = %s
                      AND tp.tournament_id = %s
                    ORDER BY tp.id;
                    """,
                    (group["id"], tournament_id),
                )

                pairs = [r["id"] for r in cur.fetchall()]

                if len(pairs) < 2:
                    continue

                round_number = 1

                for i in range(len(pairs)):
                    for j in range(i + 1, len(pairs)):
                        cur.execute(
                            """
                            INSERT INTO tournament_matches
                                (
                                    tournament_id,
                                    group_id,
                                    phase,
                                    round_number,
                                    pair_a_id,
                                    pair_b_id,
                                    status
                                )
                            VALUES
                                (%s, %s, 'group_stage', %s, %s, %s, 'scheduled');
                            """,
                            (
                                tournament_id,
                                group["id"],
                                round_number,
                                pairs[i],
                                pairs[j],
                            ),
                        )

                        created += 1
                        round_number += 1

            cur.execute(
                """
                UPDATE tournament_events
                SET status = 'group_stage'
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            conn.commit()

            return {
                "message": "Partidos de grupos generados",
                "matches_created": created,
            }


@app.get("/tournament_events/{tournament_id}/matches")
def get_tournament_matches(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tm.*,
                    tg.name AS group_name,
                    COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,
                    COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name
                FROM tournament_matches tm
                LEFT JOIN tournament_groups tg
                    ON tg.id = tm.group_id
                JOIN tournament_pairs pa
                    ON pa.id = tm.pair_a_id
                JOIN players p1a
                    ON p1a.id = pa.player_1_id
                JOIN players p2a
                    ON p2a.id = pa.player_2_id
                JOIN tournament_pairs pb
                    ON pb.id = tm.pair_b_id
                JOIN players p1b
                    ON p1b.id = pb.player_1_id
                JOIN players p2b
                    ON p2b.id = pb.player_2_id
                WHERE tm.tournament_id = %s
                ORDER BY
                    CASE
                        WHEN tm.phase = 'group_stage' THEN 1
                        WHEN tm.bracket_round = 'semifinal' THEN 2
                        WHEN tm.bracket_round = 'final' THEN 3
                        ELSE 4
                    END,
                    tg.name NULLS LAST,
                    tm.round_number,
                    tm.id;
                """,
                (tournament_id,),
            )

            return cur.fetchall()


@app.post("/tournament-matches/{match_id}/result")
def save_tournament_match_result(match_id: int, data: TournamentMatchResult):
    sets_a, sets_b, games_a, games_b = parse_padel_score(data.score)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pair_a_id, pair_b_id
                FROM tournament_matches
                WHERE id = %s;
                """,
                (match_id,),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(status_code=404, detail="Partido de torneo no encontrado")

            if data.winner_pair_id not in [match["pair_a_id"], match["pair_b_id"]]:
                raise HTTPException(
                    status_code=400,
                    detail="La pareja ganadora no pertenece a este partido",
                )

            cur.execute(
                """
                UPDATE tournament_matches
                SET score = %s,
                    winner_pair_id = %s,
                    sets_a = %s,
                    sets_b = %s,
                    games_a = %s,
                    games_b = %s,
                    status = 'completed',
                    played_at = NOW()
                WHERE id = %s
                RETURNING *;
                """,
                (
                    data.score,
                    data.winner_pair_id,
                    sets_a,
                    sets_b,
                    games_a,
                    games_b,
                    match_id,
                ),
            )

            result = cur.fetchone()
            conn.commit()
            return result


@app.get("/tournament_events/{tournament_id}/standings")
def get_tournament_standings(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tg.id AS group_id,
                    tg.name AS group_name,
                    tp.id AS pair_id,
                    COALESCE(tp.pair_name, p1.name || ' / ' || p2.name) AS pair_name,

                    COUNT(tm.id) FILTER (
                        WHERE tm.status = 'completed'
                    ) AS played,

                    COUNT(tm.id) FILTER (
                        WHERE tm.winner_pair_id = tp.id
                    ) AS wins,

                    COUNT(tm.id) FILTER (
                        WHERE tm.status = 'completed'
                          AND tm.winner_pair_id IS NOT NULL
                          AND tm.winner_pair_id <> tp.id
                    ) AS losses,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN tm.winner_pair_id = tp.id THEN 3
                                ELSE 0
                            END
                        ),
                        0
                    ) AS points,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN tm.pair_a_id = tp.id THEN tm.sets_a
                                WHEN tm.pair_b_id = tp.id THEN tm.sets_b
                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_for,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN tm.pair_a_id = tp.id THEN tm.sets_b
                                WHEN tm.pair_b_id = tp.id THEN tm.sets_a
                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_against,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN tm.pair_a_id = tp.id THEN tm.games_a
                                WHEN tm.pair_b_id = tp.id THEN tm.games_b
                                ELSE 0
                            END
                        ),
                        0
                    ) AS games_for,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN tm.pair_a_id = tp.id THEN tm.games_b
                                WHEN tm.pair_b_id = tp.id THEN tm.games_a
                                ELSE 0
                            END
                        ),
                        0
                    ) AS games_against

                FROM tournament_group_members tgm
                JOIN tournament_groups tg
                    ON tg.id = tgm.group_id
                JOIN tournament_pairs tp
                    ON tp.id = tgm.pair_id
                JOIN players p1
                    ON p1.id = tp.player_1_id
                JOIN players p2
                    ON p2.id = tp.player_2_id
                LEFT JOIN tournament_matches tm
                    ON (
                        tm.pair_a_id = tp.id
                        OR tm.pair_b_id = tp.id
                    )
                   AND tm.phase = 'group_stage'
                   AND tm.group_id = tg.id
                WHERE tg.tournament_id = %s
                GROUP BY
                    tg.id,
                    tg.name,
                    tp.id,
                    tp.pair_name,
                    p1.name,
                    p2.name
                ORDER BY
                    tg.name,
                    points DESC,
                    wins DESC,
                    (COALESCE(SUM(CASE WHEN tm.pair_a_id = tp.id THEN tm.sets_a WHEN tm.pair_b_id = tp.id THEN tm.sets_b ELSE 0 END), 0) -
                     COALESCE(SUM(CASE WHEN tm.pair_a_id = tp.id THEN tm.sets_b WHEN tm.pair_b_id = tp.id THEN tm.sets_a ELSE 0 END), 0)) DESC,
                    (COALESCE(SUM(CASE WHEN tm.pair_a_id = tp.id THEN tm.games_a WHEN tm.pair_b_id = tp.id THEN tm.games_b ELSE 0 END), 0) -
                     COALESCE(SUM(CASE WHEN tm.pair_a_id = tp.id THEN tm.games_b WHEN tm.pair_b_id = tp.id THEN tm.games_a ELSE 0 END), 0)) DESC,
                    pair_name;
                """,
                (tournament_id,),
            )

            rows = cur.fetchall()

            for r in rows:
                r["sets_diff"] = int(r["sets_for"] or 0) - int(r["sets_against"] or 0)
                r["games_diff"] = int(r["games_for"] or 0) - int(r["games_against"] or 0)

            return rows


@app.post("/tournament_events/{tournament_id}/generate-playoff")
def generate_tournament_playoff(tournament_id: int, data: TournamentGeneratePlayoff):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'group_stage'
                  AND status <> 'completed';
                """,
                (tournament_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} partidos de grupos",
                )

            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'playoff';
                """,
                (tournament_id,),
            )

            if cur.fetchone()["existing"] > 0:
                raise HTTPException(status_code=400, detail="El playoff ya fue generado")

            cur.execute(
                """
                WITH base AS (
                    SELECT
                        tg.name AS group_name,
                        tp.id AS pair_id,

                        COUNT(tm.id) FILTER (
                            WHERE tm.status = 'completed'
                        ) AS played,

                        COUNT(tm.id) FILTER (
                            WHERE tm.winner_pair_id = tp.id
                        ) AS wins,

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN tm.winner_pair_id = tp.id THEN 3
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS points,

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN tm.pair_a_id = tp.id THEN tm.sets_a
                                    WHEN tm.pair_b_id = tp.id THEN tm.sets_b
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS sets_for,

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN tm.pair_a_id = tp.id THEN tm.sets_b
                                    WHEN tm.pair_b_id = tp.id THEN tm.sets_a
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS sets_against,

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN tm.pair_a_id = tp.id THEN tm.games_a
                                    WHEN tm.pair_b_id = tp.id THEN tm.games_b
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS games_for,

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN tm.pair_a_id = tp.id THEN tm.games_b
                                    WHEN tm.pair_b_id = tp.id THEN tm.games_a
                                    ELSE 0
                                END
                            ),
                            0
                        ) AS games_against

                    FROM tournament_group_members tgm
                    JOIN tournament_groups tg
                        ON tg.id = tgm.group_id
                    JOIN tournament_pairs tp
                        ON tp.id = tgm.pair_id
                    LEFT JOIN tournament_matches tm
                        ON (
                            tm.pair_a_id = tp.id
                            OR tm.pair_b_id = tp.id
                        )
                       AND tm.phase = 'group_stage'
                       AND tm.group_id = tg.id
                    WHERE tg.tournament_id = %s
                    GROUP BY tg.name, tp.id
                ), ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY group_name
                            ORDER BY
                                points DESC,
                                wins DESC,
                                (sets_for - sets_against) DESC,
                                (games_for - games_against) DESC,
                                pair_id ASC
                        ) AS group_position
                    FROM base
                )
                SELECT *
                FROM ranked
                WHERE group_position <= %s
                ORDER BY
                    points DESC,
                    wins DESC,
                    (sets_for - sets_against) DESC,
                    (games_for - games_against) DESC,
                    pair_id ASC;
                """,
                (tournament_id, data.qualifiers_per_group),
            )

            qualifiers = cur.fetchall()

            if len(qualifiers) != 4:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Por ahora el playoff soporta exactamente 4 clasificados "
                        "para semifinal y final. Ajusta la cantidad de grupos o "
                        "clasificados por grupo para obtener 4 parejas."
                    ),
                )

            # Cruces: mejor clasificado vs cuarto, segundo vs tercero.
            playoff_matches = [
                (qualifiers[0]["pair_id"], qualifiers[3]["pair_id"]),
                (qualifiers[1]["pair_id"], qualifiers[2]["pair_id"]),
            ]

            created = 0

            for pair_a, pair_b in playoff_matches:
                cur.execute(
                    """
                    INSERT INTO tournament_matches
                        (
                            tournament_id,
                            phase,
                            bracket_round,
                            round_number,
                            pair_a_id,
                            pair_b_id,
                            status
                        )
                    VALUES
                        (%s, 'playoff', 'semifinal', 1, %s, %s, 'scheduled');
                    """,
                    (tournament_id, pair_a, pair_b),
                )

                created += 1

            cur.execute(
                """
                UPDATE tournament_events
                SET status = 'playoff'
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            conn.commit()

            return {
                "message": "Playoff generado",
                "matches_created": created,
            }


@app.post("/tournament_events/{tournament_id}/generate-final")
def generate_tournament_final(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'semifinal'
                  AND status <> 'completed';
                """,
                (tournament_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(status_code=400, detail=f"Faltan {pending} semifinales")

            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'final';
                """,
                (tournament_id,),
            )

            if cur.fetchone()["existing"] > 0:
                raise HTTPException(status_code=400, detail="La final ya fue generada")

            cur.execute(
                """
                SELECT winner_pair_id
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'semifinal'
                ORDER BY id;
                """,
                (tournament_id,),
            )

            winners = [r["winner_pair_id"] for r in cur.fetchall() if r["winner_pair_id"]]

            if len(winners) != 2:
                raise HTTPException(
                    status_code=400,
                    detail="No hay exactamente 2 ganadores de semifinal para crear la final",
                )

            cur.execute(
                """
                INSERT INTO tournament_matches
                    (
                        tournament_id,
                        phase,
                        bracket_round,
                        round_number,
                        pair_a_id,
                        pair_b_id,
                        status
                    )
                VALUES
                    (%s, 'playoff', 'final', 2, %s, %s, 'scheduled')
                RETURNING *;
                """,
                (tournament_id, winners[0], winners[1]),
            )

            final = cur.fetchone()
            conn.commit()

            return {
                "message": "Final generada",
                "final": final,
            }


@app.post("/tournament_events/{tournament_id}/finish")
def finish_tournament(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM tournament_events
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            tournament = cur.fetchone()

            if not tournament:
                raise HTTPException(status_code=404, detail="Torneo no encontrado")

            if tournament["status"] == "completed":
                raise HTTPException(status_code=400, detail="Este torneo ya fue finalizado")

            cur.execute(
                """
                SELECT COUNT(*) AS pending
                FROM tournament_matches
                WHERE tournament_id = %s
                  AND status <> 'completed';
                """,
                (tournament_id,),
            )

            pending = cur.fetchone()["pending"]

            if pending > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"Faltan {pending} partidos por completar",
                )

            cur.execute(
                """
                SELECT
                    tm.id,
                    tm.phase,
                    tm.bracket_round,
                    tm.winner_pair_id,
                    tm.rating_processed,
                    pa.id AS pair_a_id,
                    pa.player_1_id AS a1,
                    pa.player_2_id AS a2,
                    pb.id AS pair_b_id,
                    pb.player_1_id AS b1,
                    pb.player_2_id AS b2
                FROM tournament_matches tm
                JOIN tournament_pairs pa
                    ON pa.id = tm.pair_a_id
                JOIN tournament_pairs pb
                    ON pb.id = tm.pair_b_id
                WHERE tm.tournament_id = %s
                  AND tm.status = 'completed'
                  AND tm.winner_pair_id IS NOT NULL;
                """,
                (tournament_id,),
            )

            matches = cur.fetchall()

            for match in matches:
                if match["rating_processed"]:
                    continue

                winner = "A" if match["winner_pair_id"] == match["pair_a_id"] else "B"
                multiplier = get_rating_multiplier(
                    "tournament_match",
                    match["phase"],
                    match["bracket_round"]
                )

                update_rating_pair_vs_pair(
                    cur=cur,
                    player_ids_team_a=[match["a1"], match["a2"]],
                    player_ids_team_b=[match["b1"], match["b2"]],
                    winner=winner,
                    match_id=None,
                    source_type="tournament_match",
                    source_id=match["id"],
                    multiplier=multiplier,
                )

                cur.execute(
                    """
                    UPDATE tournament_matches
                    SET rating_processed = TRUE
                    WHERE id = %s;
                    """,
                    (match["id"],),
                )

            # Bonus final del torneo.
            cur.execute(
                """
                SELECT
                    tm.winner_pair_id,
                    tm.pair_a_id,
                    tm.pair_b_id,
                    wp.player_1_id AS champion_1,
                    wp.player_2_id AS champion_2,
                    CASE
                        WHEN tm.winner_pair_id = tm.pair_a_id THEN tm.pair_b_id
                        ELSE tm.pair_a_id
                    END AS finalist_pair_id
                FROM tournament_matches tm
                JOIN tournament_pairs wp
                    ON wp.id = tm.winner_pair_id
                WHERE tm.tournament_id = %s
                  AND tm.phase = 'playoff'
                  AND tm.bracket_round = 'final'
                  AND tm.status = 'completed'
                ORDER BY tm.id DESC
                LIMIT 1;
                """,
                (tournament_id,),
            )

            final = cur.fetchone()

            if final:
                apply_rating_bonus(cur, final["champion_1"], 15, "tournament_champion", tournament_id)
                apply_rating_bonus(cur, final["champion_2"], 15, "tournament_champion", tournament_id)

                cur.execute(
                    """
                    SELECT player_1_id, player_2_id
                    FROM tournament_pairs
                    WHERE id = %s;
                    """,
                    (final["finalist_pair_id"],),
                )

                finalist = cur.fetchone()

                if finalist:
                    apply_rating_bonus(cur, finalist["player_1_id"], 8, "tournament_finalist", tournament_id)
                    apply_rating_bonus(cur, finalist["player_2_id"], 8, "tournament_finalist", tournament_id)

            cur.execute(
                """
                UPDATE tournament_events
                SET status = 'completed',
                    finished_at = NOW()
                WHERE id = %s
                RETURNING *;
                """,
                (tournament_id,),
            )

            finished = cur.fetchone()
            conn.commit()

            return {
                "message": "Torneo finalizado correctamente",
                "tournament": finished,
            }


@app.get("/tournament_events/{tournament_id}/summary")
def get_tournament_summary(tournament_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    tm.bracket_round,
                    tm.winner_pair_id,
                    COALESCE(wp.pair_name, w1.name || ' / ' || w2.name) AS winner_pair_name,
                    COALESCE(pa.pair_name, a1.name || ' / ' || a2.name) AS pair_a_name,
                    COALESCE(pb.pair_name, b1.name || ' / ' || b2.name) AS pair_b_name,
                    tm.score
                FROM tournament_matches tm
                JOIN tournament_pairs pa
                    ON pa.id = tm.pair_a_id
                JOIN players a1
                    ON a1.id = pa.player_1_id
                JOIN players a2
                    ON a2.id = pa.player_2_id
                JOIN tournament_pairs pb
                    ON pb.id = tm.pair_b_id
                JOIN players b1
                    ON b1.id = pb.player_1_id
                JOIN players b2
                    ON b2.id = pb.player_2_id
                LEFT JOIN tournament_pairs wp
                    ON wp.id = tm.winner_pair_id
                LEFT JOIN players w1
                    ON w1.id = wp.player_1_id
                LEFT JOIN players w2
                    ON w2.id = wp.player_2_id
                WHERE tm.tournament_id = %s
                  AND tm.phase = 'playoff'
                ORDER BY
                    CASE
                        WHEN tm.bracket_round = 'final' THEN 1
                        WHEN tm.bracket_round = 'semifinal' THEN 2
                        ELSE 3
                    END,
                    tm.id;
                """,
                (tournament_id,),
            )

            playoff = cur.fetchall()

            cur.execute(
                """
                SELECT
                    tp.id,
                    COALESCE(tp.pair_name, p1.name || ' / ' || p2.name) AS pair_name,
                    tg.name AS group_name,
                    tp.payment_status,
                    tp.payment_method,
                    tp.payment_amount,
                    tp.payment_reference,
                    tp.payment_link
                FROM tournament_pairs tp
                JOIN players p1
                    ON p1.id = tp.player_1_id
                JOIN players p2
                    ON p2.id = tp.player_2_id
                LEFT JOIN tournament_group_members tgm
                    ON tgm.pair_id = tp.id
                LEFT JOIN tournament_groups tg
                    ON tg.id = tgm.group_id
                WHERE tp.tournament_id = %s
                ORDER BY tg.name NULLS LAST, pair_name;
                """,
                (tournament_id,),
            )

            pairs = cur.fetchall()

            cur.execute(
                """
                SELECT *
                FROM tournament_events
                WHERE id = %s;
                """,
                (tournament_id,),
            )

            tournament = cur.fetchone()

            return {
                "tournament": tournament,
                "playoff": playoff,
                "pairs": pairs,
            }


# =========================
# Player accounts / auth MVP
# =========================

PLAYER_SESSION_DAYS = 30

def create_player_session(cur, player_id: int):
    raw_token = generate_token()
    token_hash = hash_session_token(raw_token)

    cur.execute(
        """
        INSERT INTO player_sessions
            (player_id, token_hash, expires_at)
        VALUES
            (%s, %s, NOW() + INTERVAL '30 days')
        RETURNING id, expires_at;
        """,
        (player_id, token_hash),
    )

    session = cur.fetchone()
    return {
        "token": raw_token,
        "expires_at": session["expires_at"],
    }

def get_authenticated_player(cur, session_token: str):
    if not session_token:
        raise HTTPException(status_code=401, detail="Sesión requerida")

    token_hash = hash_session_token(session_token)

    cur.execute(
        """
        SELECT
            p.id,
            p.name,
            p.email,
            p.club_id,
            p.gender,
            p.category,
            p.side,
            p.is_registered,
            p.email_verified
        FROM player_sessions ps
        JOIN players p
            ON p.id = ps.player_id
        WHERE ps.token_hash = %s
          AND ps.revoked_at IS NULL
          AND ps.expires_at > NOW();
        """,
        (token_hash,),
    )

    player = cur.fetchone()

    if not player:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")

    return player

class PlayerAccountRegister(BaseModel):
    name: str
    email: str
    password: str
    club_id: int | None = None
    gender: str | None = None
    side: str | None = None
    category: str | None = None

class PlayerAccountLogin(BaseModel):
    email: str
    password: str

class PlayerSessionRequest(BaseModel):
    session_token: str

class PlayerMatchCreate(BaseModel):
    session_token: str
    club_id: int
    team_a_player_ids: list[int]
    team_b_player_ids: list[int]
    score: str
    winning_team: str
    category: str | None = None
    played_at: str | None = None

class PlayerMatchConfirm(BaseModel):
    session_token: str

@app.post("/player/register")
def player_account_register(data: PlayerAccountRegister):
    email = data.email.strip().lower()

    if len(data.password) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM players
                WHERE LOWER(email) = %s
                LIMIT 1;
                """,
                (email,),
            )

            existing = cur.fetchone()

            if existing:
                raise HTTPException(
                    status_code=400,
                    detail="Ya existe un perfil con este correo. Ingresa con tu cuenta.",
                )

            verification_token = secrets.token_urlsafe(24)

            cur.execute(
                """
                INSERT INTO players
                    (
                        name,
                        email,
                        club_id,
                        gender,
                        side,
                        category,
                        is_registered,
                        password_hash,
                        email_verified,
                        email_verification_token
                    )
                VALUES
                    (%s, %s, %s, %s, %s, %s, TRUE, %s, FALSE, %s)
                RETURNING
                    id,
                    name,
                    email,
                    club_id,
                    gender,
                    category,
                    side,
                    is_registered,
                    email_verified;
                """,
                (
                    data.name.strip(),
                    email,
                    data.club_id,
                    data.gender,
                    data.side,
                    data.category,
                    hash_password(data.password),
                    verification_token,
                ),
            )

            player = cur.fetchone()

            if data.club_id is not None:
                cur.execute(
                    """
                    INSERT INTO player_clubs (player_id, club_id, is_home_club)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (player_id, club_id) DO NOTHING;
                    """,
                    (player["id"], data.club_id),
                )

            ensure_player_rating(cur, player["id"])
            session = create_player_session(cur, player["id"])

            conn.commit()

            # MVP: devolvemos verification_token solo para pruebas.
            # Luego se envía por correo y no se expone en la respuesta.
            return {
                "player": player,
                "session": session,
                "verification_token": verification_token,
                "message": "Perfil creado correctamente",
            }

@app.post("/player/login")
def player_account_login(data: PlayerAccountLogin):
    email = data.email.strip().lower()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    name,
                    email,
                    club_id,
                    gender,
                    category,
                    side,
                    is_registered,
                    email_verified,
                    password_hash
                FROM players
                WHERE LOWER(email) = %s
                LIMIT 1;
                """,
                (email,),
            )

            player = cur.fetchone()

            if not player or not verify_password(data.password, player["password_hash"]):
                raise HTTPException(status_code=401, detail="Correo o clave incorrectos")

            cur.execute(
                """
                UPDATE players
                SET last_login_at = NOW()
                WHERE id = %s;
                """,
                (player["id"],),
            )

            session = create_player_session(cur, player["id"])

            safe_player = dict(player)
            safe_player.pop("password_hash", None)

            conn.commit()

            return {
                "player": safe_player,
                "session": session,
            }


@app.post("/player/forgot-password")
def player_forgot_password(data: PlayerForgotPassword):
    email = data.email.strip().lower()
    token = generate_token()
    expires_at = datetime.utcnow() + timedelta(minutes=PASSWORD_RESET_MINUTES)
    frontend_url = FRONTEND_URL
    reset_link = f"{frontend_url}/player-reset-password.html?token={token}"

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, email
                FROM players
                WHERE lower(email) = %s
                LIMIT 1;
                """,
                (email,),
            )

            player = cur.fetchone()

            if player:
                cur.execute(
                    """
                    UPDATE players
                    SET password_reset_token = %s,
                        password_reset_expires_at = %s
                    WHERE id = %s;
                    """,
                    (token, expires_at, player["id"]),
                )

                html = f"""
                <div style="font-family:Arial,sans-serif;max-width:560px;margin:auto;padding:24px;">
                  <h1 style="color:#111827;">PuntoRank</h1>
                  <p>Hola {player["name"]},</p>
                  <p>Recibimos una solicitud para restablecer tu contraseña.</p>
                  <p>
                    <a href="{reset_link}"
                       style="display:inline-block;background:#5cad59;color:white;padding:12px 18px;border-radius:12px;text-decoration:none;font-weight:bold;">
                      Restablecer contraseña
                    </a>
                  </p>
                  <p>Este enlace vence en 30 minutos.</p>
                  <p>Si no solicitaste este cambio, puedes ignorar este correo.</p>
                </div>
                """

                notify_password_reset(
                    email=player["email"],
                    token=raw_token,
                )

            conn.commit()

    return {
        "message": "Si el correo existe, enviaremos instrucciones para restablecer la contraseña."
    }

@app.post("/player/reset-password")
def player_reset_password(data: PlayerResetPassword):
    if len(data.new_password) < 6:
        raise HTTPException(
            status_code=400,
            detail="La contraseña debe tener al menos 6 caracteres",
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM players
                WHERE password_reset_token = %s
                  AND password_reset_expires_at > NOW()
                LIMIT 1;
                """,
                (data.token,),
            )

            player = cur.fetchone()

            if not player:
                raise HTTPException(
                    status_code=400,
                    detail="Token inválido o expirado",
                )

            cur.execute(
                """
                UPDATE players
                SET password_hash = %s,
                    password_reset_token = NULL,
                    password_reset_expires_at = NULL
                WHERE id = %s;
                """,
                (hash_password(data.new_password), player["id"]),
            )

            conn.commit()

    return {"message": "Contraseña actualizada correctamente"}


@app.post("/player/logout")
def player_logout(data: PlayerSessionRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE player_sessions
                SET revoked_at = NOW()
                WHERE token_hash = %s;
                """,
                (hash_session_token(data.session_token),),
            )
            conn.commit()
            return {"message": "Sesión cerrada"}

@app.get("/player/me")
def player_me(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(cur, token)
            return player

@app.get("/player/dashboard")
def player_dashboard(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(cur, token)
            player_id = player["id"]

            cur.execute(
                """
                SELECT
                    ROUND(COALESCE(pr.rating, 1000), 2) AS rating,
                    COALESCE(pr.matches_count, 0) AS matches_count
                FROM players p
                LEFT JOIN player_ratings pr
                    ON pr.player_id = p.id
                WHERE p.id = %s;
                """,
                (player_id,),
            )
            rating = cur.fetchone()

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE delta > 0) AS positive_movements,
                    COUNT(*) FILTER (WHERE delta < 0) AS negative_movements,
                    COALESCE(SUM(delta), 0) AS total_delta
                FROM rating_history
                WHERE player_id = %s;
                """,
                (player_id,),
            )
            stats = cur.fetchone()

            cur.execute(
                """
                SELECT
                    rh.source_type,
                    rh.source_id,
                    ROUND(rh.rating_before, 2) AS rating_before,
                    ROUND(rh.rating_after, 2) AS rating_after,
                    ROUND(rh.delta, 2) AS delta,
                    rh.created_at
                FROM rating_history rh
                WHERE rh.player_id = %s
                ORDER BY rh.created_at DESC
                LIMIT 5;
                """,
                (player_id,),
            )
            recent_rating = cur.fetchall()

            cur.execute(
                """
                SELECT
                    ls.id,
                    ls.name,
                    ls.category,
                    ls.gender,
                    ls.status,
                    ls.created_at
                FROM league_seasons ls
                JOIN league_pairs lp ON lp.league_id = ls.id
                WHERE %s IN (lp.player_1_id, lp.player_2_id)
                GROUP BY
                    ls.id,
                    ls.name,
                    ls.category,
                    ls.gender,
                    ls.status,
                    ls.created_at
                ORDER BY ls.created_at DESC;
                """,
                (player_id,),
            )
            leagues = cur.fetchall()

            cur.execute(
                """
                SELECT
                    te.id,
                    te.name,
                    te.category,
                    te.gender,
                    te.status,
                    te.created_at
                FROM tournament_events te
                JOIN tournament_pairs tp ON tp.tournament_id = te.id
                WHERE %s IN (tp.player_1_id, tp.player_2_id)
                GROUP BY
                    te.id,
                    te.name,
                    te.category,
                    te.gender,
                    te.status,
                    te.created_at
                ORDER BY te.created_at DESC;
                """,
                (player_id,),
            )
            tournaments = cur.fetchall()

            cur.execute(
                """
                SELECT
                    m.id,
                    m.status,
                    m.played_at,
                    m.category,
                    m.match_type,
                    c.name AS club_name,
                    mr.score,
                    mr.winning_team,
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'A') AS team_a,
                    ARRAY_AGG(p.name) FILTER (WHERE mp.team = 'B') AS team_b
                FROM matches m
                JOIN match_players mp
                    ON mp.match_id = m.id
                JOIN players p
                    ON p.id = mp.player_id
                LEFT JOIN match_results mr
                    ON mr.match_id = m.id
                LEFT JOIN clubs c
                    ON c.id = m.club_id
                WHERE m.id IN (
                    SELECT match_id
                    FROM match_players
                    WHERE player_id = %s
                )
                GROUP BY
                    m.id,
                    m.status,
                    m.played_at,
                    m.category,
                    m.match_type,
                    c.name,
                    mr.score,
                    mr.winning_team
                ORDER BY m.played_at DESC, m.id DESC
                LIMIT 10;
                """,
                (player_id,),
            )
            recent_matches = cur.fetchall()

            return {
                "player": player,
                "rating": rating,
                "stats": stats,
                "recent_rating": recent_rating,
                "leagues": leagues,
                "tournaments": tournaments,
                "recent_matches": recent_matches,
            }

@app.post("/player/matches/report")
def player_report_match(data: PlayerMatchCreate):
    if len(data.team_a_player_ids) != 2 or len(data.team_b_player_ids) != 2:
        raise HTTPException(status_code=400, detail="Cada equipo debe tener exactamente 2 jugadores")

    all_players = data.team_a_player_ids + data.team_b_player_ids

    if len(set(all_players)) != 4:
        raise HTTPException(status_code=400, detail="Los 4 jugadores deben ser distintos")

    if data.winning_team not in ("A", "B"):
        raise HTTPException(status_code=400, detail="winning_team debe ser A o B")

    if not data.score.strip():
        raise HTTPException(status_code=400, detail="Debes ingresar el resultado")

    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(cur, data.session_token)

            if player["id"] not in all_players:
                raise HTTPException(
                    status_code=403,
                    detail="El jugador autenticado debe participar en el partido",
                )

            cur.execute(
                """
                SELECT id
                FROM clubs
                WHERE id = %s;
                """,
                (data.club_id,),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Club no encontrado")

            cur.execute(
                """
                INSERT INTO matches
                    (club_id, event_id, match_type, status, created_by, played_at, category)
                VALUES
                    (%s, NULL, 'match', 'pending_confirmation', %s, COALESCE(%s::timestamp, NOW()), %s)
                RETURNING *;
                """,
                (
                    data.club_id,
                    player["id"],
                    data.played_at,
                    data.category,
                ),
            )

            match = cur.fetchone()
            match_id = match["id"]

            for pid in data.team_a_player_ids:
                ensure_player_rating(cur, pid)
                cur.execute(
                    """
                    INSERT INTO match_players (match_id, player_id, team)
                    VALUES (%s, %s, 'A');
                    """,
                    (match_id, pid),
                )

            for pid in data.team_b_player_ids:
                ensure_player_rating(cur, pid)
                cur.execute(
                    """
                    INSERT INTO match_players (match_id, player_id, team)
                    VALUES (%s, %s, 'B');
                    """,
                    (match_id, pid),
                )

            cur.execute(
                """
                INSERT INTO match_results (match_id, score, winning_team)
                VALUES (%s, %s, %s);
                """,
                (match_id, data.score, data.winning_team),
            )

            cur.execute(
                """
                INSERT INTO match_confirmations (match_id, player_id, confirmed)
                VALUES (%s, %s, TRUE)
                ON CONFLICT DO NOTHING;
                """,
                (match_id, player["id"]),
            )

            conn.commit()

            return {
                "message": "Partido creado. Comparte el link o QR para validación.",
                "match": match,
                "validation_path": f"confirm-match.html?id={match_id}",
            }

@app.get("/player/matches/{match_id}/public")
def player_match_public_detail(match_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.id,
                    m.status,
                    m.played_at,
                    m.category,
                    m.match_type,
                    c.name AS club_name,
                    mr.score,
                    mr.winning_team,
                    ARRAY_AGG(
                        JSON_BUILD_OBJECT(
                            'player_id', p.id,
                            'name', p.name,
                            'team', mp.team
                        )
                    ) AS players
                FROM matches m
                JOIN match_players mp
                    ON mp.match_id = m.id
                JOIN players p
                    ON p.id = mp.player_id
                LEFT JOIN match_results mr
                    ON mr.match_id = m.id
                LEFT JOIN clubs c
                    ON c.id = m.club_id
                WHERE m.id = %s
                GROUP BY
                    m.id,
                    m.status,
                    m.played_at,
                    m.category,
                    m.match_type,
                    c.name,
                    mr.score,
                    mr.winning_team;
                """,
                (match_id,),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(status_code=404, detail="Partido no encontrado")

            return match

@app.post("/player/matches/{match_id}/confirm")
def player_confirm_match(match_id: int, data: PlayerMatchConfirm):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(cur, data.session_token)

            cur.execute(
                """
                SELECT team
                FROM match_players
                WHERE match_id = %s
                  AND player_id = %s;
                """,
                (match_id, player["id"]),
            )

            membership = cur.fetchone()

            if not membership:
                raise HTTPException(status_code=403, detail="No perteneces a este partido")

            cur.execute(
                """
                INSERT INTO match_confirmations (match_id, player_id, confirmed)
                VALUES (%s, %s, TRUE)
                ON CONFLICT DO NOTHING;
                """,
                (match_id, player["id"]),
            )

            cur.execute(
                """
                SELECT mp.team, COUNT(mc.id) AS confirmations
                FROM match_players mp
                LEFT JOIN match_confirmations mc
                    ON mc.match_id = mp.match_id
                   AND mc.player_id = mp.player_id
                   AND mc.confirmed = TRUE
                WHERE mp.match_id = %s
                GROUP BY mp.team;
                """,
                (match_id,),
            )

            team_confirmations = cur.fetchall()
            confirmed_teams = [r for r in team_confirmations if r["confirmations"] > 0]

            auto_approved = False

            if len(confirmed_teams) == 2:
                cur.execute(
                    """
                    SELECT status, rating_processed
                    FROM matches
                    WHERE id = %s;
                    """,
                    (match_id,),
                )
                match = cur.fetchone()

                if match and not match["rating_processed"]:
                    cur.execute(
                        """
                        UPDATE matches
                        SET status = 'approved'
                        WHERE id = %s;
                        """,
                        (match_id,),
                    )

                    update_ratings_for_match(cur, match_id)

                    cur.execute(
                        """
                        UPDATE matches
                        SET rating_processed = TRUE
                        WHERE id = %s;
                        """,
                        (match_id,),
                    )

                    auto_approved = True

            conn.commit()

            return {
                "message": "Partido confirmado",
                "auto_approved": auto_approved,
            }

@app.post("/player/matches/{match_id}/dispute")
def player_dispute_match(match_id: int, data: PlayerMatchConfirm):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(cur, data.session_token)

            cur.execute(
                """
                SELECT id
                FROM match_players
                WHERE match_id = %s
                  AND player_id = %s;
                """,
                (match_id, player["id"]),
            )

            if not cur.fetchone():
                raise HTTPException(status_code=403, detail="No perteneces a este partido")

            cur.execute(
                """
                UPDATE matches
                SET status = 'disputed'
                WHERE id = %s;
                """,
                (match_id,),
            )

            conn.commit()

            return {"message": "Partido marcado como disputado. El club deberá revisarlo."}

