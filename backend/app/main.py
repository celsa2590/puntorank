import os
import psycopg2
import random
import hashlib
import secrets
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
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
    notify_welcome,
    notify_league_match_schedule,
)
from app.services.match_service import (
    register_match_metadata,
    notify_friendly_match_players,
    requires_confirmation,
)
from app.routers.matches import router as matches_router
from app.routers.player_password import router as player_password_router
from fastapi import (
    FastAPI,
    HTTPException,
    Header,
    File,
    Form,
    UploadFile,
)
from pydantic import BaseModel
from app.services.r2_service import (
    delete_player_photo_by_url,
    process_profile_image,
    upload_player_photo,
)
from typing import Literal
from app.templates.email.league_registration import (
    league_registration_template,
    league_welcome_email_template,
)

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
app.include_router(player_password_router)

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
                    p.photo_url,
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
                GROUP BY p.id, p.name, p.side, p.category, p.is_registered, p.photo_url, pr.rating, pr.matches_count;
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

            if player.email:
                try:
                    notify_welcome(
                        email=player.email,
                        player_name=player.name,
                    )
                except Exception as e:
                    print(f"Error enviando correo de bienvenida a {player.email}: {e}")

            return new_player



@app.post("/club/login")
def club_login(login: ClubLogin):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT id, name, logo_url
                FROM clubs
                WHERE username = %s
                  AND password = %s;
                """,
                (login.username, login.password),
            )

            club = cur.fetchone()

            if not club:
                raise HTTPException(
                    status_code=401,
                    detail="Usuario o clave incorrectos"
                )

            session_token = secrets.token_urlsafe(32)
            expires_at = datetime.utcnow() + timedelta(days=30)

            cur.execute(
                """
                INSERT INTO club_sessions (club_id, session_token, expires_at)
                VALUES (%s, %s, %s);
                """,
                (club["id"], session_token, expires_at),
            )

            conn.commit()

            return {
                "id": club["id"],
                "name": club["name"],
                "logo_url": club.get("logo_url"),
                "session_token": session_token
            }

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
    allowed_genders = {
        "femenino",
        "masculino",
        "mixto",
    }

    allowed_pair_targets = {
        4,
        6,
        8,
    }

    if data.gender not in allowed_genders:
        raise HTTPException(
            status_code=400,
            detail="Género no válido",
        )

    if data.pairs_target not in allowed_pair_targets:
        raise HTTPException(
            status_code=400,
            detail=(
                "Por ahora puedes crear eventos "
                "de 4 o 6 parejas"
            ),
        )

    if data.courts < 1:
        raise HTTPException(
            status_code=400,
            detail="Debe existir al menos una cancha",
        )


    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO americano_events
                    (
                        club_id,
                        name,
                        category,
                        gender,
                        courts,
                        duration_minutes,
                        pairs_target,
                        status
                    )
                VALUES
                    (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        'draft'
                    )
                RETURNING *;
                """,
                (
                    data.club_id,
                    data.name,
                    data.category,
                    data.gender,
                    data.courts,
                    data.duration_minutes,
                    data.pairs_target,
                ),
            )

            americano = cur.fetchone()
            conn.commit()

            return americano

@app.get("/americanos/{americano_id}/eligible-players")
def get_americano_eligible_players(
    americano_id: int,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    club_id,
                    gender,
                    category
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
                SELECT DISTINCT
                    p.id AS player_id,
                    p.name,
                    p.email,
                    p.gender,
                    p.category,
                    p.side,

                    COALESCE(
                        home_club.name,
                        direct_club.name
                    ) AS club_name,

                    CASE
                        WHEN p.club_id = %s
                        THEN TRUE

                        WHEN EXISTS (
                            SELECT 1
                            FROM player_clubs pc2
                            WHERE pc2.player_id = p.id
                              AND pc2.club_id = %s
                        )
                        THEN TRUE

                        ELSE FALSE
                    END AS belongs_to_event_club

                FROM players p

                LEFT JOIN player_clubs pc
                    ON pc.player_id = p.id
                   AND pc.is_home_club = TRUE

                LEFT JOIN clubs home_club
                    ON home_club.id = pc.club_id

                LEFT JOIN clubs direct_club
                    ON direct_club.id = p.club_id

                WHERE p.id NOT IN (
                    SELECT ap.player_id
                    FROM americano_players ap
                    WHERE ap.americano_id = %s
                )

                ORDER BY
                    belongs_to_event_club DESC,
                    p.name;
                """,
                (
                    americano["club_id"],
                    americano["club_id"],
                    americano_id,
                ),
            )

            return cur.fetchall()


@app.post("/americanos/{americano_id}/players")
def add_player_to_americano(
    americano_id: int,
    data: AmericanoAddPlayer,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    club_id,
                    gender,
                    category,
                    pairs_target,
                    status
                FROM americano_events
                WHERE id = %s
                FOR UPDATE;
                """,
                (americano_id,),
            )

            americano = cur.fetchone()

            if not americano:
                raise HTTPException(
                    status_code=404,
                    detail="Americano no encontrado",
                )

            if americano["status"] != "draft":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No puedes inscribir jugadores "
                        "después de generar las rondas"
                    ),
                )

            player_id = data.player_id

            if player_id is not None:
                cur.execute(
                    """
                    SELECT
                        id,
                        name,
                        gender,
                        category
                    FROM players
                    WHERE id = %s;
                    """,
                    (player_id,),
                )

                player = cur.fetchone()

                if not player:
                    raise HTTPException(
                        status_code=404,
                        detail="Jugador no encontrado",
                    )

            else:
                if not data.name:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Debes enviar player_id "
                            "o el nombre del nuevo jugador"
                        ),
                    )

                cur.execute(
                    """
                    INSERT INTO players
                        (
                            name,
                            email,
                            club_id,
                            gender,
                            category,
                            side,
                            is_registered
                        )
                    VALUES
                        (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            FALSE
                        )
                    RETURNING id;
                    """,
                    (
                        data.name,
                        data.email,
                        americano["club_id"],
                        data.gender,
                        data.category,
                        data.side,
                    ),
                )

                player = cur.fetchone()
                player_id = player["id"]

                ensure_player_rating(
                    cur,
                    player_id,
                )

                cur.execute(
                    """
                    INSERT INTO player_clubs
                        (
                            player_id,
                            club_id,
                            is_home_club
                        )
                    VALUES
                        (%s, %s, FALSE)
                    ON CONFLICT (
                        player_id,
                        club_id
                    )
                    DO NOTHING;
                    """,
                    (
                        player_id,
                        americano["club_id"],
                    ),
                )

            cur.execute(
                """
                SELECT id
                FROM americano_players
                WHERE americano_id = %s
                  AND player_id = %s;
                """,
                (
                    americano_id,
                    player_id,
                ),
            )

            if cur.fetchone():
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Este jugador ya está inscrito "
                        "en el evento"
                    ),
                )

            maximum_players = (
                int(americano["pairs_target"]) * 2
            )

            cur.execute(
                """
                SELECT COUNT(*) AS players_count
                FROM americano_players
                WHERE americano_id = %s;
                """,
                (americano_id,),
            )

            players_count = cur.fetchone()[
                "players_count"
            ]

            if players_count >= maximum_players:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"El evento admite como máximo "
                        f"{maximum_players} jugadores"
                    ),
                )

            cur.execute(
                """
                INSERT INTO americano_players
                    (
                        americano_id,
                        player_id
                    )
                VALUES
                    (%s, %s)
                RETURNING *;
                """,
                (
                    americano_id,
                    player_id,
                ),
            )

            americano_player = cur.fetchone()
            conn.commit()

            return {
                "message": "Jugador inscrito",
                "americano_player": americano_player,
            }







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
def generate_americano_rounds(
    americano_id: int,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    courts,
                    duration_minutes,
                    pairs_target,
                    status
                FROM americano_events
                WHERE id = %s
                FOR UPDATE;
                """,
                (americano_id,),
            )

            americano = cur.fetchone()

            if not americano:
                raise HTTPException(
                    status_code=404,
                    detail="Evento no encontrado",
                )

            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM americano_matches am
                JOIN americano_rounds ar
                    ON ar.id = am.round_id
                WHERE ar.americano_id = %s;
                """,
                (americano_id,),
            )

            existing = cur.fetchone()["existing"]

            if existing > 0:
                raise HTTPException(
                    status_code=400,
                    detail="Las rondas ya fueron generadas",
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

            pairs = [
                row["id"]
                for row in cur.fetchall()
            ]

            pairs_target = int(
                americano["pairs_target"] or 4
            )

            if pairs_target not in (4, 6, 8):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "El formato debe tener "
                        "4, 6 u 8 parejas"
                    ),
                )

            if len(pairs) != pairs_target:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Este evento requiere exactamente "
                        f"{pairs_target} parejas. "
                        f"Actualmente hay {len(pairs)}."
                    ),
                )

            # Verificar que ninguna persona esté en dos parejas.
            cur.execute(
                """
                SELECT
                    player_1_id AS player_id
                FROM americano_pairs
                WHERE americano_id = %s

                UNION ALL

                SELECT
                    player_2_id AS player_id
                FROM americano_pairs
                WHERE americano_id = %s;
                """,
                (
                    americano_id,
                    americano_id,
                ),
            )

            player_ids = [
                row["player_id"]
                for row in cur.fetchall()
            ]

            if len(player_ids) != len(set(player_ids)):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Un jugador aparece en más "
                        "de una pareja"
                    ),
                )

            courts = int(americano["courts"] or 1)

            if courts < 1:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Debe existir al menos una cancha"
                    ),
                )

            pairs_work = pairs.copy()

            # Se mantiene por compatibilidad futura
            # con cantidades impares.
            if len(pairs_work) % 2 != 0:
                pairs_work.append(None)

            participant_slots = len(pairs_work)
            rounds_needed = participant_slots - 1
            matches_per_round = participant_slots // 2

            recommended_minutes = int(
                americano["duration_minutes"]
                / rounds_needed
            )

            matches_created = 0
            matches_without_court = 0
            rounds_summary = []

            for round_number in range(
                1,
                rounds_needed + 1,
            ):
                round_matches = 0
                round_without_court = 0

                for index in range(matches_per_round):
                    pair_a = pairs_work[index]

                    pair_b = pairs_work[
                        participant_slots - 1 - index
                    ]

                    if pair_a is None or pair_b is None:
                        continue

                    # Solo se asigna cancha a los partidos
                    # que caben simultáneamente.
                    court_number = (
                        index + 1
                        if index < courts
                        else None
                    )

                    if court_number is None:
                        matches_without_court += 1
                        round_without_court += 1

                    cur.execute(
                        """
                        INSERT INTO americano_rounds
                            (
                                americano_id,
                                round_number,
                                court_number
                            )
                        VALUES
                            (%s, %s, %s)
                        RETURNING id;
                        """,
                        (
                            americano_id,
                            round_number,
                            court_number,
                        ),
                    )

                    round_id = cur.fetchone()["id"]

                    cur.execute(
                        """
                        INSERT INTO americano_matches
                            (
                                round_id,
                                pair_a_id,
                                pair_b_id
                            )
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
                    round_matches += 1

                rounds_summary.append(
                    {
                        "round_number": round_number,
                        "matches": round_matches,
                        "simultaneous_matches": min(
                            round_matches,
                            courts,
                        ),
                        "matches_without_court": (
                            round_without_court
                        ),
                    }
                )

                # Circle method.
                pairs_work = (
                    [pairs_work[0]]
                    + [pairs_work[-1]]
                    + pairs_work[1:-1]
                )

            cur.execute(
                """
                UPDATE americano_events
                SET status = 'scheduled'
                WHERE id = %s
                RETURNING *;
                """,
                (americano_id,),
            )

            updated = cur.fetchone()
            conn.commit()

            format_names = {
                4: "cuadrangular",
                6: "hexagonal",
                8: "octagonal",
            }

            return {
                "message": "Rondas generadas",
                "americano": updated,
                "format": format_names[pairs_target],
                "pairs": pairs_target,
                "courts": courts,
                "matches_created": matches_created,
                "rounds_created": rounds_needed,
                "matches_per_round": matches_per_round,
                "matches_without_court": (
                    matches_without_court
                ),
                "recommended_minutes_per_match": (
                    recommended_minutes
                ),
                "rounds": rounds_summary,
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
                    ls.id AS league_id,
                    ls.name AS league_name,
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

@app.patch("/leagues/{league_id}/configuration")
def update_league_configuration(
    league_id: int,
    data: LeagueConfigurationUpdate,
):
    allowed_scoring_modes = {
        "sets_2_plus_match_1",
        "win_1_no_substitute_penalty",
        "match_win_3",
    }

    allowed_playoff_formats = {
        "gold_silver",
        "none",
    }

    if data.group_count < 1:
        raise HTTPException(
            status_code=400,
            detail="La cantidad de grupos debe ser al menos 1",
        )

    if data.courts_count < 1:
        raise HTTPException(
            status_code=400,
            detail="La cantidad de canchas debe ser al menos 1",
        )

    if data.scoring_mode not in allowed_scoring_modes:
        raise HTTPException(
            status_code=400,
            detail="Sistema de puntuación no válido",
        )

    if data.playoff_format not in allowed_playoff_formats:
        raise HTTPException(
            status_code=400,
            detail="Formato de playoffs no válido",
        )

    if data.gold_qualifiers < 0:
        raise HTTPException(
            status_code=400,
            detail="gold_qualifiers no puede ser negativo",
        )

    if data.silver_qualifiers < 0:
        raise HTTPException(
            status_code=400,
            detail="silver_qualifiers no puede ser negativo",
        )

    if (
        data.playoff_format == "gold_silver"
        and data.gold_qualifiers != 4
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Por ahora la Copa Oro requiere "
                "exactamente 4 clasificados"
            ),
        )

    if (
        data.playoff_format == "gold_silver"
        and data.silver_qualifiers != 4
    ):
        raise HTTPException(
            status_code=400,
            detail=(
                "Por ahora la Copa Plata requiere "
                "exactamente 4 clasificados"
            ),
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    status
                FROM league_seasons
                WHERE id = %s
                FOR UPDATE;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            cur.execute(
                """
                SELECT COUNT(*) AS matches_count
                FROM league_matches
                WHERE league_id = %s;
                """,
                (league_id,),
            )

            matches_count = cur.fetchone()["matches_count"]

            if matches_count > 0:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No puedes cambiar la configuración "
                        "después de generar el fixture"
                    ),
                )

            cur.execute(
                """
                SELECT COUNT(*) AS pairs_count
                FROM league_pairs
                WHERE league_id = %s;
                """,
                (league_id,),
            )

            pairs_count = cur.fetchone()["pairs_count"]

            if pairs_count > 0 and data.group_count > pairs_count:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No puede haber más grupos que parejas"
                    ),
                )

            cur.execute(
                """
                UPDATE league_seasons
                SET
                    group_count = %s,
                    courts_count = %s,
                    scoring_mode = %s,
                    playoff_format = %s,
                    gold_qualifiers = %s,
                    silver_qualifiers = %s
                WHERE id = %s
                RETURNING *;
                """,
                (
                    data.group_count,
                    data.courts_count,
                    data.scoring_mode,
                    data.playoff_format,
                    data.gold_qualifiers,
                    data.silver_qualifiers,
                    league_id,
                ),
            )

            updated = cur.fetchone()
            conn.commit()

            return {
                "message": "Configuración guardada",
                "league": updated,
            }


@app.post("/leagues/{league_id}/generate-fixture")
def generate_league_fixture(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    status,
                    group_count,
                    courts_count,
                    scoring_mode,
                    playoff_format,
                    gold_qualifiers,
                    silver_qualifiers
                FROM league_seasons
                WHERE id = %s
                FOR UPDATE;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            cur.execute(
                """
                SELECT COUNT(*) AS existing
                FROM league_matches
                WHERE league_id = %s;
                """,
                (league_id,),
            )

            existing = cur.fetchone()["existing"]

            if existing > 0:
                raise HTTPException(
                    status_code=400,
                    detail="El fixture ya fue generado",
                )

            cur.execute(
                """
                SELECT id
                FROM league_pairs
                WHERE league_id = %s
                ORDER BY id;
                """,
                (league_id,),
            )

            pair_rows = cur.fetchall()
            pair_ids = [row["id"] for row in pair_rows]

            if len(pair_ids) < 2:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Se necesitan al menos 2 parejas "
                        "para generar el fixture"
                    ),
                )

            group_count = int(league["group_count"] or 1)
            courts_count = int(league["courts_count"] or 1)

            if group_count > len(pair_ids):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No puede haber más grupos que parejas"
                    ),
                )

            # Distribución balanceada:
            # pareja 1 -> grupo 1
            # pareja 2 -> grupo 2
            # ...
            # y vuelve a comenzar.
            grouped_pairs = {
                index: []
                for index in range(1, group_count + 1)
            }

            for index, pair_id in enumerate(pair_ids):
                group_number = (index % group_count) + 1
                grouped_pairs[group_number].append(pair_id)

                cur.execute(
                    """
                    UPDATE league_pairs
                    SET group_name = %s
                    WHERE id = %s;
                    """,
                    (
                        f"Grupo {group_number}",
                        pair_id,
                    ),
                )

            groups_with_too_few_pairs = [
                group_number
                for group_number, pairs in grouped_pairs.items()
                if len(pairs) < 2
            ]

            if groups_with_too_few_pairs:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Cada grupo necesita al menos "
                        "2 parejas"
                    ),
                )

            total_matches_created = 0
            max_rounds_created = 0
            groups_summary = []

            for group_number, pairs in grouped_pairs.items():
                pairs_work = pairs.copy()

                # En grupos impares se agrega un descanso.
                if len(pairs_work) % 2 != 0:
                    pairs_work.append(None)

                participant_slots = len(pairs_work)
                rounds_needed = participant_slots - 1
                matches_per_round = participant_slots // 2

                max_rounds_created = max(
                    max_rounds_created,
                    rounds_needed,
                )

                group_matches_created = 0

                for round_number in range(
                    1,
                    rounds_needed + 1,
                ):
                    round_match_index = 0

                    for index in range(matches_per_round):
                        pair_a = pairs_work[index]
                        pair_b = pairs_work[
                            participant_slots - 1 - index
                        ]

                        if pair_a is None or pair_b is None:
                            continue

                        round_match_index += 1

                        assigned_court = None

                        if round_match_index <= courts_count:
                            assigned_court = (
                                f"Cancha {round_match_index}"
                            )

                        cur.execute(
                            """
                            INSERT INTO league_matches
                                (
                                    league_id,
                                    round_number,
                                    pair_a_id,
                                    pair_b_id,
                                    phase,
                                    status,
                                    court
                                )
                            VALUES
                                (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    'regular',
                                    'scheduled',
                                    %s
                                );
                            """,
                            (
                                league_id,
                                round_number,
                                pair_a,
                                pair_b,
                                assigned_court,
                            ),
                        )

                        total_matches_created += 1
                        group_matches_created += 1

                    # Algoritmo circle method.
                    pairs_work = (
                        [pairs_work[0]]
                        + [pairs_work[-1]]
                        + pairs_work[1:-1]
                    )

                groups_summary.append(
                    {
                        "group_name": f"Grupo {group_number}",
                        "pairs": len(pairs),
                        "rounds": rounds_needed,
                        "matches": group_matches_created,
                    }
                )

            cur.execute(
                """
                UPDATE league_seasons
                SET status = 'scheduled'
                WHERE id = %s
                RETURNING *;
                """,
                (league_id,),
            )

            updated_league = cur.fetchone()
            conn.commit()

            return {
                "message": "Fixture generado correctamente",
                "league": updated_league,
                "configuration": {
                    "group_count": group_count,
                    "courts_count": courts_count,
                    "scoring_mode": league["scoring_mode"],
                    "playoff_format": league["playoff_format"],
                },
                "groups": groups_summary,
                "rounds_created": max_rounds_created,
                "matches_created": total_matches_created,
            }


@app.get("/leagues/{league_id}/matches")
def get_league_matches(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    lm.id,
                    lm.league_id,
                    lm.round_number,
                    lm.phase,
                    lm.cup,
                    lm.scheduled_at,
                    lm.court,
                    lm.last_schedule_notified_at,
                    lm.bracket_round,
                    lm.score,
                    lm.status,
                    lm.played_at,
                    lm.winner_pair_id,

                    lm.pair_a_sets_won,
                    lm.pair_b_sets_won,

                    lm.pair_a_used_substitute,
                    lm.pair_b_used_substitute,

                    COALESCE(
                        pa.group_name,
                        'Grupo 1'
                    ) AS group_name,

                    pa.id AS pair_a_id,

                    COALESCE(
                        pa.pair_name,
                        p1a.name || ' / ' || p2a.name
                    ) AS pair_a_name,

                    pb.id AS pair_b_id,

                    COALESCE(
                        pb.pair_name,
                        p1b.name || ' / ' || p2b.name
                    ) AS pair_b_name

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
                        WHEN lm.phase = 'regular'
                        THEN 1

                        WHEN (
                            lm.phase = 'playoff'
                            AND lm.bracket_round = 'semifinal'
                        )
                        THEN 2

                        WHEN (
                            lm.phase = 'playoff'
                            AND lm.bracket_round = 'final'
                        )
                        THEN 3

                        ELSE 4
                    END,

                    COALESCE(
                        pa.group_name,
                        'Grupo 1'
                    ),

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


def process_league_match_rating(
    cur,
    match_id: int,
) -> bool:
    """
    Procesa el rating de un partido de liga una sola vez.

    Devuelve:
        True  -> el rating fue procesado ahora.
        False -> ya había sido procesado.
    """

    cur.execute(
        """
        SELECT
            lm.id,
            lm.status,
            lm.played_at,
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

        WHERE lm.id = %s

        FOR UPDATE;
        """,
        (match_id,),
    )

    match = cur.fetchone()

    if not match:
        raise HTTPException(
            status_code=404,
            detail="Partido de liga no encontrado",
        )

    if match["rating_processed"]:
        return False

    if match["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=(
                "El rating solo puede procesarse "
                "cuando el partido está completado"
            ),
        )

    if match["winner_pair_id"] is None:
        raise HTTPException(
            status_code=400,
            detail="El partido no tiene pareja ganadora",
        )

    if match["winner_pair_id"] == match["pair_a_id"]:
        winner = "A"

    elif match["winner_pair_id"] == match["pair_b_id"]:
        winner = "B"

    else:
        raise HTTPException(
            status_code=400,
            detail=(
                "La pareja ganadora no pertenece "
                "al partido"
            ),
        )

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
        multiplier=get_rating_multiplier(
            "league_match"
        ),
    )

    cur.execute(
        """
        UPDATE league_matches
        SET rating_processed = TRUE
        WHERE id = %s;
        """,
        (match_id,),
    )

    # Mantiene el historial ordenado por la fecha real
    # del partido, especialmente para recuperaciones.
    cur.execute(
        """
        UPDATE rating_history
        SET created_at = COALESCE(%s, created_at)
        WHERE source_type = 'league_match'
          AND source_id = %s;
        """,
        (
            match["played_at"],
            match_id,
        ),
    )

    return True


@app.post("/league-matches/{match_id}/result")
def save_league_match_result(
    match_id: int,
    data: LeagueMatchResult,
):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    lm.id,
                    lm.status,
                    lm.league_id,
                    lm.pair_a_id,
                    lm.pair_b_id,
                    lm.rating_processed,

                    ls.name AS league_name,
                    ls.status AS league_status,
                    ls.scoring_mode,

                    c.name AS club_name,

                    COALESCE(
                        pa.pair_name,
                        p1a.name || ' / ' || p2a.name
                    ) AS pair_a_name,

                    COALESCE(
                        pb.pair_name,
                        p1b.name || ' / ' || p2b.name
                    ) AS pair_b_name,

                    pa.player_1_id AS a1_id,
                    p1a.name AS a1_name,
                    p1a.email AS a1_email,

                    pa.player_2_id AS a2_id,
                    p2a.name AS a2_name,
                    p2a.email AS a2_email,

                    pb.player_1_id AS b1_id,
                    p1b.name AS b1_name,
                    p1b.email AS b1_email,

                    pb.player_2_id AS b2_id,
                    p2b.name AS b2_name,
                    p2b.email AS b2_email

                FROM league_matches lm

                JOIN league_seasons ls
                    ON ls.id = lm.league_id

                LEFT JOIN clubs c
                    ON c.id = ls.club_id

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

                WHERE lm.id = %s

                FOR UPDATE OF lm;
                """,
                (match_id,),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(
                    status_code=404,
                    detail="Partido de liga no encontrado",
                )

            if match["league_status"] == "completed":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No puedes modificar una liga "
                        "finalizada"
                    ),
                )

            # Una corrección posterior requeriría revertir
            # primero el movimiento Elo anterior.
            if match["status"] == "completed":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Este resultado ya fue registrado. "
                        "No puede modificarse porque ya "
                        "impactó el ranking"
                    ),
                )

            if data.winner_pair_id not in (
                match["pair_a_id"],
                match["pair_b_id"],
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "La pareja ganadora seleccionada "
                        "no pertenece a este partido"
                    ),
                )

            score = data.score.strip()

            if not score:
                raise HTTPException(
                    status_code=400,
                    detail="Debes ingresar el resultado",
                )

            raw_sets = score.split()

            sets_a = 0
            sets_b = 0

            for raw_set in raw_sets:
                if raw_set.count("-") != 1:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Formato de resultado inválido. "
                            "Usa, por ejemplo: "
                            "6-4 3-6 6-2"
                        ),
                    )

                left, right = raw_set.split("-", 1)

                try:
                    games_a = int(left)
                    games_b = int(right)

                except ValueError as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "El resultado debe contener "
                            "números. Ejemplo: "
                            "6-4 3-6 6-2"
                        ),
                    ) from exc

                if games_a < 0 or games_b < 0:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Los games no pueden "
                            "ser negativos"
                        ),
                    )

                if games_a == games_b:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Un set no puede terminar "
                            f"empatado: {raw_set}"
                        ),
                    )

                if games_a > games_b:
                    sets_a += 1
                else:
                    sets_b += 1

            if (
                match["scoring_mode"]
                == "sets_2_plus_match_1"
            ):
                if len(raw_sets) != 3:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Esta liga exige cargar "
                            "exactamente 3 sets. "
                            "Ejemplo: 6-4 3-6 6-2"
                        ),
                    )

                expected_winner_pair_id = (
                    match["pair_a_id"]
                    if sets_a > sets_b
                    else match["pair_b_id"]
                )

                if (
                    data.winner_pair_id
                    != expected_winner_pair_id
                ):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "La pareja ganadora seleccionada "
                            "no coincide con los sets "
                            "ingresados"
                        ),
                    )

            cur.execute(
                """
                UPDATE league_matches
                SET
                    score = %s,
                    winner_pair_id = %s,
                    pair_a_sets_won = %s,
                    pair_b_sets_won = %s,
                    pair_a_used_substitute = %s,
                    pair_b_used_substitute = %s,
                    status = 'completed',
                    played_at = COALESCE(
                        played_at,
                        NOW()
                    )
                WHERE id = %s
                RETURNING *;
                """,
                (
                    score,
                    data.winner_pair_id,
                    sets_a,
                    sets_b,
                    data.pair_a_used_substitute,
                    data.pair_b_used_substitute,
                    match_id,
                ),
            )

            updated_match = cur.fetchone()

            # Procesa Elo dentro de la misma transacción.
            # Si falla el rating, tampoco se guarda el resultado.
            rating_processed_now = (
                process_league_match_rating(
                    cur,
                    match_id,
                )
            )

            cur.execute(
                """
                SELECT
                    player_id,
                    ROUND(rating_before, 2)
                        AS rating_before,
                    ROUND(rating_after, 2)
                        AS rating_after,
                    ROUND(delta, 2)
                        AS delta
                FROM rating_history
                WHERE source_type = 'league_match'
                  AND source_id = %s
                ORDER BY player_id;
                """,
                (match_id,),
            )

            rating_changes = cur.fetchall()

            conn.commit()

            winner_name = (
                match["pair_a_name"]
                if (
                    data.winner_pair_id
                    == match["pair_a_id"]
                )
                else match["pair_b_name"]
            )

            players = [
                {
                    "id": match["a1_id"],
                    "name": match["a1_name"],
                    "email": match["a1_email"],
                    "pair_id": match["pair_a_id"],
                    "pair_name": (
                        match["pair_a_name"]
                    ),
                },
                {
                    "id": match["a2_id"],
                    "name": match["a2_name"],
                    "email": match["a2_email"],
                    "pair_id": match["pair_a_id"],
                    "pair_name": (
                        match["pair_a_name"]
                    ),
                },
                {
                    "id": match["b1_id"],
                    "name": match["b1_name"],
                    "email": match["b1_email"],
                    "pair_id": match["pair_b_id"],
                    "pair_name": (
                        match["pair_b_name"]
                    ),
                },
                {
                    "id": match["b2_id"],
                    "name": match["b2_name"],
                    "email": match["b2_email"],
                    "pair_id": match["pair_b_id"],
                    "pair_name": (
                        match["pair_b_name"]
                    ),
                },
            ]

            emails_sent = 0
            email_errors = []
            players_without_email = []

            # El resultado y el rating ya están confirmados.
            # Un error de correo no los revierte.
            for player in players:
                email = (
                    player["email"].strip()
                    if player["email"]
                    else ""
                )

                if not email:
                    players_without_email.append(
                        {
                            "player_id": player["id"],
                            "player_name": (
                                player["name"]
                            ),
                        }
                    )
                    continue

                player_won = (
                    player["pair_id"]
                    == data.winner_pair_id
                )

                html, text = (
                    league_match_result_email_template(
                        player_name=player["name"],
                        league_name=(
                            match["league_name"]
                        ),
                        club_name=(
                            match["club_name"]
                            or "PuntoRank"
                        ),
                        league_id=(
                            match["league_id"]
                        ),
                        pair_a_name=(
                            match["pair_a_name"]
                        ),
                        pair_b_name=(
                            match["pair_b_name"]
                        ),
                        score=score,
                        winner_name=winner_name,
                        player_pair_name=(
                            player["pair_name"]
                        ),
                        player_won=player_won,
                    )
                )

                try:
                    send_email(
                        to_email=email,
                        subject=(
                            "Resultado registrado: "
                            f"{match['pair_a_name']} vs "
                            f"{match['pair_b_name']}"
                        ),
                        html=html,
                        text=text,
                    )

                    emails_sent += 1

                except Exception as exc:
                    print(
                        "Error enviando resultado de liga "
                        f"match={match_id} "
                        f"player={player['id']} "
                        f"email={email}: "
                        f"{type(exc).__name__}: {exc}"
                    )

                    email_errors.append(
                        {
                            "player_id": player["id"],
                            "player_name": (
                                player["name"]
                            ),
                            "email": email,
                            "error": str(exc),
                        }
                    )




            if match["scoring_mode"] == "sets_2_plus_match_1":
                pair_a_points = (
                    sets_a * 2
                    + (
                        1
                        if data.winner_pair_id == match["pair_a_id"]
                        else 0
                    )
                )

                pair_b_points = (
                    sets_b * 2
                    + (
                        1
                        if data.winner_pair_id == match["pair_b_id"]
                        else 0
                    )
                )

                if data.pair_a_used_substitute:
                    pair_a_points -= 1

                if data.pair_b_used_substitute:
                    pair_b_points -= 1

            elif (
                match["scoring_mode"]
                == "win_1_no_substitute_penalty"
            ):
                pair_a_points = (
                    1
                    if data.winner_pair_id == match["pair_a_id"]
                    else 0
                )

                pair_b_points = (
                    1
                    if data.winner_pair_id == match["pair_b_id"]
                    else 0
                )

            else:
                pair_a_points = (
                    3
                    if data.winner_pair_id == match["pair_a_id"]
                    else 0
                )

                pair_b_points = (
                    3
                    if data.winner_pair_id == match["pair_b_id"]
                    else 0
                )




            if data.pair_a_used_substitute:
                pair_a_points -= 1

            if data.pair_b_used_substitute:
                pair_b_points -= 1

            return {
                "message": (
                    "Resultado guardado y "
                    "ranking actualizado"
                ),
                "match": updated_match,
                "rating": {
                    "processed": (
                        rating_processed_now
                    ),
                    "changes": rating_changes,
                },
                "scoring": {
                    "mode": match["scoring_mode"],
                    "pair_a_sets_won": sets_a,
                    "pair_b_sets_won": sets_b,
                    "pair_a_points": pair_a_points,
                    "pair_b_points": pair_b_points,
                },
                "notification": {
                    "sent": True,
                    "emails_sent": emails_sent,
                    "players_without_email": (
                        players_without_email
                    ),
                    "errors": email_errors,
                },
            }


@app.get("/leagues/{league_id}/standings")
def get_league_standings(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(
                        lp.group_name,
                        'Grupo 1'
                    ) AS group_name,

                    lp.id AS pair_id,
                    lp.points_adjustment,

                    COALESCE(
                        lp.pair_name,
                        p1.name || ' / ' || p2.name
                    ) AS pair_name,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                    ) AS played,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id = lp.id
                    ) AS wins,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id IS NOT NULL
                          AND lm.winner_pair_id <> lp.id
                    ) AS losses,

                    (
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN lm.status <> 'completed'
                                    THEN 0

                                    WHEN (
                                        ls.scoring_mode
                                        = 'sets_2_plus_match_1'
                                    )
                                    THEN
                                        (
                                            2 * CASE
                                                WHEN lm.pair_a_id = lp.id
                                                THEN COALESCE(
                                                    lm.pair_a_sets_won,
                                                    0
                                                )
                                                WHEN lm.pair_b_id = lp.id
                                                THEN COALESCE(
                                                    lm.pair_b_sets_won,
                                                    0
                                                )
                                                ELSE 0
                                            END
                                        )
                                        +
                                        CASE
                                            WHEN lm.winner_pair_id = lp.id
                                            THEN 1
                                            ELSE 0
                                        END

                                    WHEN (
                                        ls.scoring_mode
                                        = 'win_1_no_substitute_penalty'
                                    )
                                    THEN
                                        CASE
                                            WHEN lm.winner_pair_id = lp.id
                                            THEN 1
                                            ELSE 0
                                        END

                                    WHEN (
                                        ls.scoring_mode = 'match_win_3'
                                    )
                                    THEN
                                        CASE
                                            WHEN lm.winner_pair_id = lp.id
                                            THEN 3
                                            ELSE 0
                                        END

                                    ELSE 0
                                END
                            ),
                            0
                        )

                        -

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN lm.status <> 'completed'
                                    THEN 0

                                    WHEN (
                                        ls.scoring_mode
                                        <> 'sets_2_plus_match_1'
                                    )
                                    THEN 0

                                    WHEN (
                                        lm.pair_a_id = lp.id
                                        AND lm.pair_a_used_substitute = TRUE
                                    )
                                    THEN 1

                                    WHEN (
                                        lm.pair_b_id = lp.id
                                        AND lm.pair_b_used_substitute = TRUE
                                    )
                                    THEN 1

                                    ELSE 0
                                END
                            ),
                            0
                        )

                        + lp.points_adjustment
                    ) AS points,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_won,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_lost,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN (
                                    ls.scoring_mode
                                    <> 'sets_2_plus_match_1'
                                )
                                THEN 0

                                WHEN (
                                    lm.pair_a_id = lp.id
                                    AND lm.pair_a_used_substitute = TRUE
                                )
                                THEN 1

                                WHEN (
                                    lm.pair_b_id = lp.id
                                    AND lm.pair_b_used_substitute = TRUE
                                )
                                THEN 1

                                ELSE 0
                            END
                        ),
                        0
                    ) AS substitute_penalties

                FROM league_pairs lp

                JOIN league_seasons ls
                    ON ls.id = lp.league_id

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                LEFT JOIN league_matches lm
                    ON (
                        lm.pair_a_id = lp.id
                        OR lm.pair_b_id = lp.id
                    )
                    AND lm.league_id = lp.league_id
                    AND lm.phase = 'regular'

                WHERE lp.league_id = %s

                GROUP BY
                    lp.id,
                    lp.group_name,
                    lp.pair_name,
                    lp.points_adjustment,
                    p1.name,
                    p2.name,
                    ls.scoring_mode

                ORDER BY
                    COALESCE(lp.group_name, 'Grupo 1'),
                    points DESC,
                    wins DESC,
                    sets_won DESC,
                    sets_lost ASC,
                    pair_name ASC;
                """,
                (league_id,),
            )

            return cur.fetchall()


@app.get("/public/leagues/{league_id}")
def get_public_league_profile(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            # ==================================================
            # 1. Información general de la liga
            # ==================================================

            cur.execute(
                """
                SELECT
                    ls.id,
                    ls.name,
                    ls.category,
                    ls.gender,
                    ls.format,
                    ls.status,
                    ls.start_date,
                    ls.end_date,
                    ls.scoring_mode,
                    c.name AS club_name,
                    c.logo_url AS club_logo_url
                FROM league_seasons ls
                LEFT JOIN clubs c
                    ON c.id = ls.club_id
                WHERE ls.id = %s;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            # ==================================================
            # 2. Parejas y participantes
            # ==================================================

            cur.execute(
                """
                SELECT
                    lp.id AS pair_id,

                    COALESCE(
                        lp.group_name,
                        'Grupo único'
                    ) AS group_name,

                    COALESCE(
                        lp.pair_name,
                        p1.name || ' / ' || p2.name
                    ) AS pair_name,

                    lp.points_adjustment,

                    p1.id AS player_1_id,
                    p1.name AS player_1_name,

                    p2.id AS player_2_id,
                    p2.name AS player_2_name

                FROM league_pairs lp

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                WHERE lp.league_id = %s

                ORDER BY
                    COALESCE(
                        lp.group_name,
                        'Grupo único'
                    ),
                    pair_name;
                """,
                (league_id,),
            )

            pairs = cur.fetchall()

            # ==================================================
            # 3. Tabla de posiciones
            #
            # Regla especial:
            # - 2 puntos por set ganado
            # - 1 punto adicional por ganar el partido
            # - 1 punto menos por usar parche
            # - points_adjustment para correcciones manuales
            # ==================================================

            cur.execute(
                """
                SELECT
                    COALESCE(
                        lp.group_name,
                        'Grupo único'
                    ) AS group_name,

                    lp.id AS pair_id,

                    COALESCE(
                        lp.pair_name,
                        p1.name || ' / ' || p2.name
                    ) AS pair_name,

                    lp.points_adjustment,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                    ) AS played,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id = lp.id
                    ) AS wins,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id IS NOT NULL
                          AND lm.winner_pair_id <> lp.id
                    ) AS losses,

                    (
                        COALESCE(
                            SUM(
                                CASE
                                    WHEN lm.status <> 'completed'
                                    THEN 0

                                    WHEN (
                                        ls.scoring_mode
                                        = 'sets_2_plus_match_1'
                                    )
                                    THEN
                                        2 * (
                                            CASE
                                                WHEN lm.pair_a_id = lp.id
                                                THEN COALESCE(
                                                    lm.pair_a_sets_won,
                                                    0
                                                )

                                                WHEN lm.pair_b_id = lp.id
                                                THEN COALESCE(
                                                    lm.pair_b_sets_won,
                                                    0
                                                )

                                                ELSE 0
                                            END
                                        )
                                        +
                                        CASE
                                            WHEN lm.winner_pair_id = lp.id
                                            THEN 1
                                            ELSE 0
                                        END

                                    ELSE
                                        CASE
                                            WHEN lm.winner_pair_id = lp.id
                                            THEN 3
                                            ELSE 0
                                        END
                                END
                            ),
                            0
                        )

                        -

                        COALESCE(
                            SUM(
                                CASE
                                    WHEN lm.status <> 'completed'
                                    THEN 0

                                    WHEN (
                                        lm.pair_a_id = lp.id
                                        AND lm.pair_a_used_substitute = TRUE
                                    )
                                    THEN 1

                                    WHEN (
                                        lm.pair_b_id = lp.id
                                        AND lm.pair_b_used_substitute = TRUE
                                    )
                                    THEN 1

                                    ELSE 0
                                END
                            ),
                            0
                        )

                        + lp.points_adjustment
                    ) AS points,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_won,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_lost,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN (
                                    lm.pair_a_id = lp.id
                                    AND lm.pair_a_used_substitute = TRUE
                                )
                                THEN 1

                                WHEN (
                                    lm.pair_b_id = lp.id
                                    AND lm.pair_b_used_substitute = TRUE
                                )
                                THEN 1

                                ELSE 0
                            END
                        ),
                        0
                    ) AS substitute_penalties

                FROM league_pairs lp

                JOIN league_seasons ls
                    ON ls.id = lp.league_id

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                LEFT JOIN league_matches lm
                    ON (
                        lm.pair_a_id = lp.id
                        OR lm.pair_b_id = lp.id
                    )
                    AND lm.league_id = lp.league_id

                WHERE lp.league_id = %s

                GROUP BY
                    lp.id,
                    lp.group_name,
                    lp.pair_name,
                    lp.points_adjustment,
                    p1.name,
                    p2.name,
                    ls.scoring_mode

                ORDER BY
                    COALESCE(
                        lp.group_name,
                        'Grupo único'
                    ),
                    points DESC,
                    wins DESC,
                    sets_won DESC,
                    sets_lost ASC,
                    pair_name ASC;
                """,
                (league_id,),
            )

            standings = cur.fetchall()

            # ==================================================
            # 4. Fixture y resultados
            # ==================================================

            cur.execute(
                """
                SELECT
                    lm.id,
                    lm.league_id,
                    lm.round_number,
                    lm.phase,
                    lm.cup,
                    lm.bracket_round,
                    lm.scheduled_at,
                    lm.court,
                    lm.score,
                    lm.status,
                    lm.played_at,
                    lm.winner_pair_id,

                    lm.pair_a_sets_won,
                    lm.pair_b_sets_won,

                    lm.pair_a_used_substitute,
                    lm.pair_b_used_substitute,

                    COALESCE(
                        pa.group_name,
                        'Grupo único'
                    ) AS group_name,

                    pa.id AS pair_a_id,

                    COALESCE(
                        pa.pair_name,
                        p1a.name || ' / ' || p2a.name
                    ) AS pair_a_name,

                    pb.id AS pair_b_id,

                    COALESCE(
                        pb.pair_name,
                        p1b.name || ' / ' || p2b.name
                    ) AS pair_b_name

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
                    COALESCE(
                        pa.group_name,
                        'Grupo único'
                    ),

                    CASE
                        WHEN lm.phase = 'regular'
                        THEN 1

                        WHEN (
                            lm.phase = 'playoff'
                            AND lm.bracket_round = 'semifinal'
                        )
                        THEN 2

                        WHEN (
                            lm.phase = 'playoff'
                            AND lm.bracket_round = 'final'
                        )
                        THEN 3

                        ELSE 4
                    END,

                    lm.round_number,
                    lm.scheduled_at NULLS LAST,
                    lm.id;
                """,
                (league_id,),
            )

            matches = cur.fetchall()

            return {
                "league": league,
                "pairs": pairs,
                "standings": standings,
                "matches": matches,
            }


@app.get("/public/leagues")
def get_public_leagues():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ls.id,
                    ls.name,
                    ls.category,
                    ls.gender,
                    ls.format,
                    ls.status,
                    ls.start_date,
                    ls.end_date,
                    c.name AS club_name,
                    c.logo_url AS club_logo_url,
                    COUNT(DISTINCT lp.id) AS pairs_count,
                    COUNT(DISTINCT lm.id) AS matches_count
                FROM league_seasons ls
                LEFT JOIN clubs c ON c.id = ls.club_id
                LEFT JOIN league_pairs lp ON lp.league_id = ls.id
                LEFT JOIN league_matches lm ON lm.league_id = ls.id
                GROUP BY ls.id, c.name, c.logo_url
                ORDER BY ls.created_at DESC;
                """
            )
            return cur.fetchall()


@app.post("/leagues/{league_id}/finish")
def finish_league(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    status
                FROM league_seasons
                WHERE id = %s
                FOR UPDATE;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            if league["status"] == "completed":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Esta liga ya fue finalizada"
                    ),
                )

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
                    detail=(
                        f"Faltan {pending} partidos "
                        "por completar"
                    ),
                )

            cur.execute(
                """
                SELECT id
                FROM league_matches
                WHERE league_id = %s
                  AND status = 'completed'
                  AND rating_processed = FALSE
                ORDER BY
                    played_at NULLS LAST,
                    id;
                """,
                (league_id,),
            )

            pending_rating_matches = (
                cur.fetchall()
            )

            ratings_processed = 0

            for row in pending_rating_matches:
                processed = (
                    process_league_match_rating(
                        cur,
                        row["id"],
                    )
                )

                if processed:
                    ratings_processed += 1

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
                "message": (
                    "Liga finalizada correctamente"
                ),
                "league": result,
                "ratings_processed_as_backup": (
                    ratings_processed
                ),
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

            cur.execute(
                """
                SELECT
                    id,
                    status,
                    group_count,
                    courts_count,
                    scoring_mode,
                    playoff_format,
                    gold_qualifiers,
                    silver_qualifiers
                FROM league_seasons
                WHERE id = %s
                FOR UPDATE;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            if league["playoff_format"] == "none":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Esta liga está configurada "
                        "sin playoffs"
                    ),
                )

            if league["playoff_format"] != "gold_silver":
                raise HTTPException(
                    status_code=400,
                    detail="Formato de playoffs no soportado",
                )

            group_count = int(
                league["group_count"] or 1
            )

            courts_count = int(
                league["courts_count"] or 1
            )

            if group_count not in (1, 2):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Por ahora los playoffs automáticos "
                        "soportan ligas de 1 o 2 grupos"
                    ),
                )

            if (
                int(league["gold_qualifiers"] or 0) != 4
                or int(
                    league["silver_qualifiers"] or 0
                ) != 4
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "El formato Oro/Plata actual exige "
                        "4 clasificadas a cada copa"
                    ),
                )

            # La fase regular debe estar completa.
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
                    detail=(
                        f"Faltan {pending} partidos "
                        "de fase regular"
                    ),
                )

            # Debe existir al menos un partido regular.
            cur.execute(
                """
                SELECT COUNT(*) AS regular_count
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'regular';
                """,
                (league_id,),
            )

            regular_count = cur.fetchone()[
                "regular_count"
            ]

            if regular_count == 0:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Primero debes generar y completar "
                        "el fixture regular"
                    ),
                )

            # Evitar duplicar playoffs.
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

            # Tabla regular usando el scoring_mode real.
            cur.execute(
                """
                SELECT
                    COALESCE(
                        lp.group_name,
                        'Grupo 1'
                    ) AS group_name,

                    lp.id AS pair_id,

                    COALESCE(
                        lp.pair_name,
                        p1.name || ' / ' || p2.name
                    ) AS pair_name,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                    ) AS played,

                    COUNT(lm.id) FILTER (
                        WHERE lm.status = 'completed'
                          AND lm.winner_pair_id = lp.id
                    ) AS wins,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN (
                                    ls.scoring_mode
                                    = 'sets_2_plus_match_1'
                                )
                                THEN
                                    (
                                        2 * CASE
                                            WHEN lm.pair_a_id = lp.id
                                            THEN COALESCE(
                                                lm.pair_a_sets_won,
                                                0
                                            )

                                            WHEN lm.pair_b_id = lp.id
                                            THEN COALESCE(
                                                lm.pair_b_sets_won,
                                                0
                                            )

                                            ELSE 0
                                        END
                                    )
                                    +
                                    CASE
                                        WHEN lm.winner_pair_id = lp.id
                                        THEN 1
                                        ELSE 0
                                    END

                                WHEN (
                                    ls.scoring_mode
                                    = 'win_1_no_substitute_penalty'
                                )
                                THEN
                                    CASE
                                        WHEN lm.winner_pair_id = lp.id
                                        THEN 1
                                        ELSE 0
                                    END

                                WHEN (
                                    ls.scoring_mode = 'match_win_3'
                                )
                                THEN
                                    CASE
                                        WHEN lm.winner_pair_id = lp.id
                                        THEN 3
                                        ELSE 0
                                    END

                                ELSE 0
                            END
                        ),
                        0
                    )

                    -

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.status <> 'completed'
                                THEN 0

                                WHEN (
                                    ls.scoring_mode
                                    <> 'sets_2_plus_match_1'
                                )
                                THEN 0

                                WHEN (
                                    lm.pair_a_id = lp.id
                                    AND lm.pair_a_used_substitute = TRUE
                                )
                                THEN 1

                                WHEN (
                                    lm.pair_b_id = lp.id
                                    AND lm.pair_b_used_substitute = TRUE
                                )
                                THEN 1

                                ELSE 0
                            END
                        ),
                        0
                    )

                    + lp.points_adjustment

                    AS points,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_won,

                    COALESCE(
                        SUM(
                            CASE
                                WHEN lm.pair_a_id = lp.id
                                THEN COALESCE(
                                    lm.pair_b_sets_won,
                                    0
                                )

                                WHEN lm.pair_b_id = lp.id
                                THEN COALESCE(
                                    lm.pair_a_sets_won,
                                    0
                                )

                                ELSE 0
                            END
                        ),
                        0
                    ) AS sets_lost

                FROM league_pairs lp

                JOIN league_seasons ls
                    ON ls.id = lp.league_id

                JOIN players p1
                    ON p1.id = lp.player_1_id

                JOIN players p2
                    ON p2.id = lp.player_2_id

                LEFT JOIN league_matches lm
                    ON (
                        lm.pair_a_id = lp.id
                        OR lm.pair_b_id = lp.id
                    )
                    AND lm.league_id = lp.league_id
                    AND lm.phase = 'regular'

                WHERE lp.league_id = %s

                GROUP BY
                    lp.id,
                    lp.group_name,
                    lp.pair_name,
                    lp.points_adjustment,
                    p1.name,
                    p2.name,
                    ls.scoring_mode

                ORDER BY
                    COALESCE(
                        lp.group_name,
                        'Grupo 1'
                    ),
                    points DESC,
                    wins DESC,
                    sets_won DESC,
                    sets_lost ASC,
                    pair_name ASC;
                """,
                (league_id,),
            )

            standings = cur.fetchall()

            grouped_standings = {}

            for row in standings:
                group_name = row["group_name"]

                if group_name not in grouped_standings:
                    grouped_standings[group_name] = []

                grouped_standings[group_name].append(
                    row
                )

            playoff_matches = []

            if group_count == 1:
                if len(standings) < 8:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Se necesitan al menos 8 parejas "
                            "para Copa Oro y Copa Plata"
                        ),
                    )

                oro = standings[:4]
                plata = standings[4:8]

                playoff_matches = [
                    (
                        "oro",
                        oro[0]["pair_id"],
                        oro[3]["pair_id"],
                    ),
                    (
                        "oro",
                        oro[1]["pair_id"],
                        oro[2]["pair_id"],
                    ),
                    (
                        "plata",
                        plata[0]["pair_id"],
                        plata[3]["pair_id"],
                    ),
                    (
                        "plata",
                        plata[1]["pair_id"],
                        plata[2]["pair_id"],
                    ),
                ]

            else:
                group_names = sorted(
                    grouped_standings.keys()
                )

                if len(group_names) != 2:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "La liga está configurada con "
                            "2 grupos, pero no se encontraron "
                            "exactamente 2 grupos"
                        ),
                    )

                group_a = grouped_standings[
                    group_names[0]
                ]

                group_b = grouped_standings[
                    group_names[1]
                ]

                if len(group_a) < 4 or len(group_b) < 4:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Cada grupo necesita al menos "
                            "4 parejas para generar Oro y Plata"
                        ),
                    )

                # Oro:
                # A1 vs B2
                # B1 vs A2
                playoff_matches.extend(
                    [
                        (
                            "oro",
                            group_a[0]["pair_id"],
                            group_b[1]["pair_id"],
                        ),
                        (
                            "oro",
                            group_b[0]["pair_id"],
                            group_a[1]["pair_id"],
                        ),
                    ]
                )

                # Plata:
                # A3 vs B4
                # B3 vs A4
                playoff_matches.extend(
                    [
                        (
                            "plata",
                            group_a[2]["pair_id"],
                            group_b[3]["pair_id"],
                        ),
                        (
                            "plata",
                            group_b[2]["pair_id"],
                            group_a[3]["pair_id"],
                        ),
                    ]
                )

            created_matches = []

            for index, (
                cup,
                pair_a_id,
                pair_b_id,
            ) in enumerate(
                playoff_matches,
                start=1,
            ):
                assigned_court = None

                if index <= courts_count:
                    assigned_court = f"Cancha {index}"

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
                            status,
                            court
                        )
                    VALUES
                        (
                            %s,
                            'playoff',
                            %s,
                            'semifinal',
                            1,
                            %s,
                            %s,
                            'scheduled',
                            %s
                        )
                    RETURNING id;
                    """,
                    (
                        league_id,
                        cup,
                        pair_a_id,
                        pair_b_id,
                        assigned_court,
                    ),
                )

                row = cur.fetchone()

                created_matches.append(
                    {
                        "match_id": row["id"],
                        "cup": cup,
                        "pair_a_id": pair_a_id,
                        "pair_b_id": pair_b_id,
                        "court": assigned_court,
                    }
                )

            conn.commit()

            return {
                "message": (
                    "Semifinales de Copa Oro "
                    "y Copa Plata generadas"
                ),
                "group_count": group_count,
                "matches_created": len(
                    created_matches
                ),
                "matches": created_matches,
            }


@app.post("/leagues/{league_id}/generate-finals")
def generate_league_finals(league_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    playoff_format,
                    courts_count
                FROM league_seasons
                WHERE id = %s
                FOR UPDATE;
                """,
                (league_id,),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada",
                )

            if league["playoff_format"] != "gold_silver":
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Esta liga no utiliza el formato "
                        "Copa Oro y Copa Plata"
                    ),
                )

            # No debe existir una final previa.
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

            # Leer las cuatro semifinales.
            cur.execute(
                """
                SELECT
                    id,
                    cup,
                    status,
                    winner_pair_id
                FROM league_matches
                WHERE league_id = %s
                  AND phase = 'playoff'
                  AND bracket_round = 'semifinal'
                ORDER BY cup, id;
                """,
                (league_id,),
            )

            semifinals = cur.fetchall()

            if len(semifinals) != 4:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Deben existir exactamente "
                        "4 semifinales"
                    ),
                )

            pending = [
                match
                for match in semifinals
                if (
                    match["status"] != "completed"
                    or match["winner_pair_id"] is None
                )
            ]

            if pending:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Debes completar todas las "
                        "semifinales antes de generar "
                        "las finales"
                    ),
                )

            by_cup = {
                "oro": [],
                "plata": [],
            }

            for match in semifinals:
                if match["cup"] not in by_cup:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Se encontró una semifinal "
                            "con copa no válida"
                        ),
                    )

                by_cup[match["cup"]].append(
                    match
                )

            if (
                len(by_cup["oro"]) != 2
                or len(by_cup["plata"]) != 2
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Deben existir 2 semifinales "
                        "de Oro y 2 de Plata"
                    ),
                )

            courts_count = int(
                league["courts_count"] or 1
            )

            finals_created = []

            for index, cup in enumerate(
                ("oro", "plata"),
                start=1,
            ):
                semifinal_1 = by_cup[cup][0]
                semifinal_2 = by_cup[cup][1]

                pair_a_id = semifinal_1[
                    "winner_pair_id"
                ]

                pair_b_id = semifinal_2[
                    "winner_pair_id"
                ]

                if pair_a_id == pair_b_id:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"La final de Copa {cup} "
                            "tiene la misma pareja en "
                            "ambos lados"
                        ),
                    )

                assigned_court = None

                if index <= courts_count:
                    assigned_court = f"Cancha {index}"

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
                            status,
                            court
                        )
                    VALUES
                        (
                            %s,
                            'playoff',
                            %s,
                            'final',
                            2,
                            %s,
                            %s,
                            'scheduled',
                            %s
                        )
                    RETURNING id;
                    """,
                    (
                        league_id,
                        cup,
                        pair_a_id,
                        pair_b_id,
                        assigned_court,
                    ),
                )

                final_row = cur.fetchone()

                finals_created.append(
                    {
                        "match_id": final_row["id"],
                        "cup": cup,
                        "pair_a_id": pair_a_id,
                        "pair_b_id": pair_b_id,
                        "court": assigned_court,
                    }
                )

            conn.commit()

            return {
                "message": (
                    "Finales de Copa Oro "
                    "y Copa Plata generadas"
                ),
                "matches_created": len(
                    finals_created
                ),
                "matches": finals_created,
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



@app.get("/public/tournaments")
def get_public_tournaments():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    te.id,
                    te.name,
                    te.category,
                    te.gender,
                    te.format,
                    te.status,
                    te.created_at,
                    c.name AS club_name,
                    c.logo_url AS club_logo_url,
                    COUNT(DISTINCT tp.id) AS pairs_count,
                    COUNT(DISTINCT tg.id) AS groups_count,
                    COUNT(DISTINCT tm.id) AS matches_count
                FROM tournament_events te
                LEFT JOIN clubs c ON c.id = te.club_id
                LEFT JOIN tournament_pairs tp ON tp.tournament_id = te.id
                LEFT JOIN tournament_groups tg ON tg.tournament_id = te.id
                LEFT JOIN tournament_matches tm ON tm.tournament_id = te.id
                GROUP BY te.id, c.name, c.logo_url
                ORDER BY te.created_at DESC;
                """
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
            p.email_verified,
            p.must_change_password,
            p.photo_url
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

class ClubChangePassword(BaseModel):
    token: str
    current_password: str
    new_password: str

class ClubCredentialsEmailRequest(BaseModel):
    token: str
    mode: Literal["club", "league", "player"]
    league_id: int | None = None
    player_id: int | None = None

class ClubLeagueWelcomeRequest(BaseModel):
    token: str
    league_id: int


class InternalTemplateTestRequest(BaseModel):
    to_email: str
    template: str
    player_name: str = "Celsa Sánchez"
    temporary_password: str = "Prueba123"
    league_name: str = "Liga A/B Invierno 2026"
    club_name: str = "Arena Padel"

def require_password_changed(player):
    if player.get("must_change_password"):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "PASSWORD_CHANGE_REQUIRED",
                "message": "Debes cambiar tu contraseña temporal antes de continuar",
            },
        )

@app.post("/club/change-password")
def club_change_password(data: ClubChangePassword):
    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT c.*
                FROM club_sessions cs
                JOIN clubs c ON c.id = cs.club_id
                WHERE cs.session_token = %s
                  AND cs.expires_at > NOW();
                """,
                (data.token,),
            )

            club = cur.fetchone()

            if not club:
                raise HTTPException(status_code=401, detail="Sesión inválida")

            if club["password"] != data.current_password:
                raise HTTPException(
                    status_code=400,
                    detail="La contraseña actual no es correcta"
                )

            if len(data.new_password) < 6:
                raise HTTPException(
                    status_code=400,
                    detail="La nueva contraseña debe tener al menos 6 caracteres"
                )

            cur.execute(
                """
                UPDATE clubs
                SET password = %s
                WHERE id = %s;
                """,
                (data.new_password, club["id"]),
            )

            conn.commit()

            return {"message": "Contraseña actualizada correctamente"}

def get_authenticated_club(cur, token: str):
    if not token:
        raise HTTPException(status_code=401, detail="Token de club requerido")

    cur.execute(
        """
        SELECT
            c.id,
            c.name,
            c.logo_url,
            c.username
        FROM club_sessions cs
        JOIN clubs c ON c.id = cs.club_id
        WHERE cs.session_token = %s
          AND cs.expires_at > NOW();
        """,
        (token,),
    )

    club = cur.fetchone()

    if not club:
        raise HTTPException(
            status_code=401,
            detail="Sesión de club inválida o expirada",
        )

    return club

def credentials_email_template(
    player_name: str,
    email: str,
    temporary_password: str,
    club_name: str,
):
    login_url = f"{FRONTEND_URL}/player-login.html"

    html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
      <meta charset="UTF-8">
      <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
      >
    </head>

    <body
      style="
        margin:0;
        padding:0;
        background:#f3f6f4;
        font-family:Arial, Helvetica, sans-serif;
      "
    >
      <table
        role="presentation"
        width="100%"
        cellspacing="0"
        cellpadding="0"
        border="0"
        style="background:#f3f6f4;"
      >
        <tr>
          <td align="center" style="padding:28px 16px;">

            <table
              role="presentation"
              width="100%"
              cellspacing="0"
              cellpadding="0"
              border="0"
              style="
                max-width:640px;
                background:#ffffff;
                border:1px solid #e5e7eb;
                border-radius:22px;
              "
            >
              <tr>
                <td
                  align="center"
                  style="
                    padding:30px 24px;
                    background:#0f172a;
                    color:#ffffff;
                    border-radius:22px 22px 0 0;
                  "
                >
                  <div
                    style="
                      font-size:38px;
                      line-height:1;
                    "
                  >
                    🎾
                  </div>

                  <div
                    style="
                      margin-top:10px;
                      font-size:28px;
                      font-weight:900;
                    "
                  >
                    PuntoRank
                  </div>

                  <div
                    style="
                      margin-top:6px;
                      font-size:14px;
                      opacity:0.85;
                    "
                  >
                    El ranking donde cada partido cuenta
                  </div>
                </td>
              </tr>

              <tr>
                <td
                  style="
                    padding:30px 26px;
                    color:#374151;
                    font-size:15px;
                    line-height:1.65;
                  "
                >
                  <h2
                    style="
                      margin:0 0 18px;
                      color:#111827;
                      font-size:24px;
                    "
                  >
                    Bienvenida/o a PuntoRank
                  </h2>

                  <p style="margin:0 0 16px;">
                    Hola <strong>{player_name}</strong>,
                  </p>

                  <p style="margin:0 0 22px;">
                    <strong>{club_name}</strong> creó tu acceso
                    a PuntoRank. Desde tu cuenta podrás revisar
                    tus ligas, partidos, resultados y rating.
                  </p>

                  <!-- Acción principal visible antes de las credenciales -->
                  <table
                    role="presentation"
                    cellspacing="0"
                    cellpadding="0"
                    border="0"
                    style="margin:0 auto 24px;"
                  >
                    <tr>
                      <td
                        align="center"
                        bgcolor="#16a34a"
                        style="border-radius:999px;"
                      >
                        <a
                          href="{login_url}"
                          target="_blank"
                          style="
                            display:inline-block;
                            padding:15px 26px;
                            color:#ffffff;
                            text-decoration:none;
                            font-weight:800;
                            font-size:15px;
                          "
                        >
                          Ingresar a PuntoRank
                        </a>
                      </td>
                    </tr>
                  </table>

                  <table
                    role="presentation"
                    width="100%"
                    cellspacing="0"
                    cellpadding="0"
                    border="0"
                    style="
                      margin:0 0 22px;
                      background:#f1f5f2;
                      border-radius:16px;
                    "
                  >
                    <tr>
                      <td style="padding:20px;">
                        <p style="margin:0 0 12px;">
                          <strong>Usuario:</strong>
                          <a
                            href="mailto:{email}"
                            style="color:#2563eb;"
                          >
                            {email}
                          </a>
                        </p>

                        <p style="margin:0;">
                          <strong>Contraseña temporal:</strong>
                          {temporary_password}
                        </p>
                      </td>
                    </tr>
                  </table>

                  <p style="margin:0 0 14px;">
                    Por seguridad, deberás cambiar esta contraseña
                    cuando ingreses por primera vez.
                  </p>

                  <p
                    style="
                      margin:0;
                      color:#6b7280;
                      font-size:13px;
                      line-height:1.5;
                    "
                  >
                    Si el botón no funciona, abre este enlace:
                  </p>

                  <p
                    style="
                      margin:6px 0 0;
                      font-size:13px;
                      line-height:1.5;
                    "
                  >
                    <a
                      href="{login_url}"
                      target="_blank"
                      style="
                        color:#16a34a;
                        word-break:break-all;
                      "
                    >
                      {login_url}
                    </a>
                  </p>

                  <div
                    style="
                      margin-top:30px;
                      padding-top:18px;
                      border-top:1px solid #e5e7eb;
                    "
                  >
                    <p
                      style="
                        margin:0;
                        color:#374151;
                        font-size:14px;
                      "
                    >
                      Nos vemos en la cancha 🎾<br>
                      <strong>Equipo PuntoRank</strong>
                    </p>
                  </div>
                </td>
              </tr>
            </table>

            <div
              style="
                margin-top:16px;
                color:#6b7280;
                font-size:12px;
                text-align:center;
              "
            >
              © PuntoRank ·
              <a
                href="{FRONTEND_URL}"
                style="color:#16a34a;"
              >
                {FRONTEND_URL}
              </a>
            </div>
          </td>
        </tr>
      </table>
    </body>
    </html>
    """

    text = (
        "Bienvenida/o a PuntoRank\n\n"
        f"Hola {player_name},\n\n"
        f"{club_name} creó tu acceso a PuntoRank.\n\n"
        f"Ingresar a PuntoRank:\n{login_url}\n\n"
        f"Usuario: {email}\n"
        f"Contraseña temporal: {temporary_password}\n\n"
        "Por seguridad, deberás cambiar esta contraseña "
        "cuando ingreses por primera vez.\n"
    )

    return html, text


def league_match_result_email_template(
    player_name: str,
    league_name: str,
    club_name: str,
    league_id: int,
    pair_a_name: str,
    pair_b_name: str,
    score: str,
    winner_name: str,
    player_pair_name: str,
    player_won: bool,
):
    league_url = f"{FRONTEND_URL}/league-public.html?id={league_id}"
    dashboard_url = f"{FRONTEND_URL}/player-dashboard.html"

    result_title = "¡Victoria!" if player_won else "Resultado registrado"
    result_message = (
        "Tu pareja ganó este partido."
        if player_won
        else "El resultado de tu partido ya fue registrado."
    )

    html = f"""
    <div style="
        font-family:Arial,sans-serif;
        max-width:620px;
        margin:auto;
        padding:28px;
        color:#111827;
    ">
      <div style="
          background:#ffffff;
          border:1px solid #e5e7eb;
          border-radius:20px;
          padding:28px;
      ">
        <h1 style="margin:0 0 8px;font-size:30px;">
          {result_title}
        </h1>

        <div style="
            width:55px;
            height:5px;
            background:#18a957;
            border-radius:20px;
            margin:14px 0 24px;
        "></div>

        <p>Hola <strong>{player_name}</strong>,</p>

        <p>
          {result_message}
        </p>

        <div style="
            background:#f4f8f5;
            border-radius:16px;
            padding:20px;
            margin:22px 0;
        ">
          <p style="margin:0 0 8px;">
            <strong>Liga:</strong> {league_name}
          </p>

          <p style="margin:0 0 8px;">
            <strong>Club:</strong> {club_name}
          </p>

          <p style="margin:0 0 8px;">
            <strong>Partido:</strong>
            {pair_a_name} vs {pair_b_name}
          </p>

          <p style="margin:0 0 8px;">
            <strong>Resultado:</strong> {score}
          </p>

          <p style="margin:0 0 8px;">
            <strong>Pareja ganadora:</strong> {winner_name}
          </p>

          <p style="margin:0;">
            <strong>Tu pareja:</strong> {player_pair_name}
          </p>
        </div>

        <p>
          <a
            href="{league_url}"
            style="
              display:inline-block;
              background:#18a957;
              color:white;
              padding:13px 20px;
              border-radius:12px;
              text-decoration:none;
              font-weight:bold;
              margin-right:8px;
            "
          >
            Ver la liga
          </a>
        </p>

        <p style="margin-top:20px;color:#66736d;font-size:14px;">
          También puedes revisar tus partidos y rating desde
          <a href="{dashboard_url}">Mi PuntoRank</a>.
        </p>
      </div>
    </div>
    """

    text = (
        f"Hola {player_name}.\n"
        f"{result_message}\n\n"
        f"Liga: {league_name}\n"
        f"Club: {club_name}\n"
        f"Partido: {pair_a_name} vs {pair_b_name}\n"
        f"Resultado: {score}\n"
        f"Pareja ganadora: {winner_name}\n"
        f"Tu pareja: {player_pair_name}\n\n"
        f"Ver liga: {league_url}\n"
        f"Mi PuntoRank: {dashboard_url}"
    )

    return html, text


@app.post("/internal/email/test-template")
def test_email_template(
    data: InternalTemplateTestRequest,
    x_internal_key: str | None = Header(default=None),
):
    expected_key = os.getenv("INTERNAL_EMAIL_TEST_KEY")

    if not expected_key:
        raise HTTPException(
            status_code=500,
            detail="INTERNAL_EMAIL_TEST_KEY no está configurada",
        )

    if not x_internal_key or not secrets.compare_digest(
        x_internal_key.encode("utf-8"),
        expected_key.encode("utf-8"),
    ):
        raise HTTPException(
            status_code=403,
            detail="Acceso denegado",
        )

    email = data.to_email.strip()
    template = data.template.strip().lower()

    if not email:
        raise HTTPException(
            status_code=400,
            detail="Debes indicar to_email",
        )

    if template == "credentials":
        html, text = credentials_email_template(
            player_name=data.player_name,
            email=email,
            temporary_password=data.temporary_password,
            club_name=data.club_name,
        )
        subject = "Tu acceso a PuntoRank"

    elif template == "league_welcome":
        html, text = league_welcome_email_template(
            player_name=data.player_name,
            league_name=data.league_name,
            club_name=data.club_name,
            league_id=data.league_id,
        )
        subject = f"Bienvenida/o a {data.league_name}"

    else:
        raise HTTPException(
            status_code=400,
            detail="Plantilla no válida. Usa credentials o league_welcome",
        )

    try:
        result = send_email(
            to_email=email,
            subject=subject,
            html=html,
            text=text,
        )
    except Exception as exc:
        print(
            f"Error enviando plantilla {template} "
            f"a {email}: {type(exc).__name__}: {exc}"
        )

        raise HTTPException(
            status_code=502,
            detail=f"No se pudo enviar el correo: {str(exc)}",
        ) from exc

    return {
        "message": "Correo de prueba enviado",
        "template": template,
        "to_email": email,
        "result": result,
    }

def validate_club_league(
    cur,
    club_id: int,
    league_id: int,
):
    cur.execute(
        """
        SELECT
            id,
            name,
            club_id
        FROM league_seasons
        WHERE id = %s
          AND club_id = %s;
        """,
        (
            league_id,
            club_id,
        ),
    )

    league = cur.fetchone()

    if not league:
        raise HTTPException(
            status_code=404,
            detail="Liga no encontrada o no pertenece al club",
        )

    return league


def get_club_credential_recipients(
    cur,
    club_id: int,
):
    cur.execute(
        """
        SELECT DISTINCT
            p.id,
            p.name,
            p.email
        FROM players p
        LEFT JOIN player_clubs pc
          ON pc.player_id = p.id
        WHERE (
            p.club_id = %s
            OR pc.club_id = %s
        )
          AND p.email IS NOT NULL
          AND BTRIM(p.email) <> ''
        ORDER BY p.name;
        """,
        (
            club_id,
            club_id,
        ),
    )

    return cur.fetchall()


def get_league_credential_recipients(
    cur,
    club_id: int,
    league_id: int,
):
    validate_club_league(
        cur,
        club_id,
        league_id,
    )

    cur.execute(
        """
        SELECT DISTINCT
            p.id,
            p.name,
            p.email
        FROM league_pairs lp
        JOIN players p
          ON p.id IN (
              lp.player_1_id,
              lp.player_2_id
          )
        WHERE lp.league_id = %s
          AND p.email IS NOT NULL
          AND BTRIM(p.email) <> ''
        ORDER BY p.name;
        """,
        (league_id,),
    )

    return cur.fetchall()


def get_single_credential_recipient(
    cur,
    club_id: int,
    player_id: int,
):
    cur.execute(
        """
        SELECT DISTINCT
            p.id,
            p.name,
            p.email
        FROM players p
        LEFT JOIN player_clubs pc
          ON pc.player_id = p.id
        WHERE p.id = %s
          AND p.email IS NOT NULL
          AND BTRIM(p.email) <> ''
          AND (
              p.club_id = %s
              OR pc.club_id = %s

              OR EXISTS (
                  SELECT 1
                  FROM league_pairs lp
                  JOIN league_seasons ls
                    ON ls.id = lp.league_id
                  WHERE ls.club_id = %s
                    AND p.id IN (
                        lp.player_1_id,
                        lp.player_2_id
                    )
              )
          )
        LIMIT 1;
        """,
        (
            player_id,
            club_id,
            club_id,
            club_id,
        ),
    )

    player = cur.fetchone()

    if not player:
        raise HTTPException(
            status_code=404,
            detail=(
                "Jugador no encontrado, no tiene correo "
                "o no está relacionado con este club"
            ),
        )

    return [player]


@app.post("/club/communications/send-credentials")
def send_club_player_credentials(
    data: ClubCredentialsEmailRequest,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            club = get_authenticated_club(
                cur,
                data.token,
            )

            if data.mode == "club":
                players = get_club_credential_recipients(
                    cur,
                    club["id"],
                )

                target_description = (
                    f"jugadores del club {club['name']}"
                )

            elif data.mode == "league":
                if data.league_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Debes indicar league_id "
                            "para el modo league"
                        ),
                    )

                league = validate_club_league(
                    cur,
                    club["id"],
                    data.league_id,
                )

                players = get_league_credential_recipients(
                    cur,
                    club["id"],
                    data.league_id,
                )

                target_description = (
                    f"participantes de {league['name']}"
                )

            elif data.mode == "player":
                if data.player_id is None:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Debes indicar player_id "
                            "para el modo player"
                        ),
                    )

                players = get_single_credential_recipient(
                    cur,
                    club["id"],
                    data.player_id,
                )

                target_description = (
                    f"jugador/a {players[0]['name']}"
                )

            else:
                raise HTTPException(
                    status_code=400,
                    detail="Modo de envío no válido",
                )

            if not players:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "No se encontraron jugadores "
                        "con correo para este envío"
                    ),
                )

            sent = 0
            errors = []

            for player in players:
                email = player["email"].strip()
                temporary_password = (
                    secrets.token_urlsafe(8)
                )

                html, text = credentials_email_template(
                    player_name=player["name"],
                    email=email,
                    temporary_password=temporary_password,
                    club_name=club["name"],
                )

                try:
                    send_email(
                        to_email=email,
                        subject="Tu acceso a PuntoRank",
                        html=html,
                        text=text,
                    )

                    cur.execute(
                        """
                        UPDATE players
                        SET
                            password_hash = %s,
                            must_change_password = TRUE,
                            is_registered = TRUE
                        WHERE id = %s;
                        """,
                        (
                            hash_password(
                                temporary_password
                            ),
                            player["id"],
                        ),
                    )

                    conn.commit()
                    sent += 1

                except Exception as exc:
                    conn.rollback()

                    print(
                        "Error enviando credenciales "
                        f"player={player['id']} "
                        f"email={email}: "
                        f"{type(exc).__name__}: {exc}"
                    )

                    errors.append(
                        {
                            "player_id": player["id"],
                            "player_name": player["name"],
                            "email": email,
                            "error": str(exc),
                        }
                    )

            return {
                "message": (
                    "Proceso de credenciales finalizado"
                ),
                "mode": data.mode,
                "target": target_description,
                "players_found": len(players),
                "emails_sent": sent,
                "errors_count": len(errors),
                "errors": errors,
            }

@app.get("/club/communications/eligible-players")
def get_communication_eligible_players(
    token: str,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            club = get_authenticated_club(
                cur,
                token,
            )

            cur.execute(
                """
                SELECT DISTINCT
                    p.id,
                    p.name,
                    p.email
                FROM players p
                LEFT JOIN player_clubs pc
                  ON pc.player_id = p.id
                WHERE p.email IS NOT NULL
                  AND BTRIM(p.email) <> ''
                  AND (
                      p.club_id = %s
                      OR pc.club_id = %s

                      OR EXISTS (
                          SELECT 1
                          FROM league_pairs lp
                          JOIN league_seasons ls
                            ON ls.id = lp.league_id
                          WHERE ls.club_id = %s
                            AND p.id IN (
                                lp.player_1_id,
                                lp.player_2_id
                            )
                      )
                  )
                ORDER BY p.name;
                """,
                (
                    club["id"],
                    club["id"],
                    club["id"],
                ),
            )

            return cur.fetchall()

@app.post("/club/communications/send-league-welcome")
def send_league_welcome(data: ClubLeagueWelcomeRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            club = get_authenticated_club(cur, data.token)

            cur.execute(
                """
                SELECT
                    ls.id,
                    ls.name,
                    ls.club_id,
                    c.name AS club_name
                FROM league_seasons ls
                JOIN clubs c ON c.id = ls.club_id
                WHERE ls.id = %s
                  AND ls.club_id = %s;
                """,
                (data.league_id, club["id"]),
            )

            league = cur.fetchone()

            if not league:
                raise HTTPException(
                    status_code=404,
                    detail="Liga no encontrada o no pertenece al club",
                )

            cur.execute(
                """
                SELECT DISTINCT
                    p.id,
                    p.name,
                    p.email
                FROM league_pairs lp
                JOIN players p
                  ON p.id IN (lp.player_1_id, lp.player_2_id)
                WHERE lp.league_id = %s
                  AND p.email IS NOT NULL
                  AND BTRIM(p.email) <> ''
                ORDER BY p.name;
                """,
                (data.league_id,),
            )

            players = cur.fetchall()

            if not players:
                raise HTTPException(
                    status_code=400,
                    detail="La liga no tiene participantes con correo",
                )

            sent = 0
            errors = []

            for player in players:
                html, text = league_welcome_email_template(
                    player_name=player["name"],
                    league_name=league["name"],
                    club_name=league["club_name"],
                    league_id=league["id"],
                )

                try:
                    send_email(
                        to_email=player["email"],
                        subject=f"Bienvenida/o a {league['name']}",
                        html=html,
                        text=text,
                    )
                    sent += 1
                except Exception as exc:
                    errors.append(
                        {
                            "player_id": player["id"],
                            "email": player["email"],
                            "error": str(exc),
                        }
                    )

            return {
                "message": "Bienvenida de liga procesada",
                "league_id": league["id"],
                "league_name": league["name"],
                "players_found": len(players),
                "emails_sent": sent,
                "errors": errors,
            }

@app.get("/club/communications/active-leagues")
def get_communication_active_leagues(token: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            club = get_authenticated_club(cur, token)

            cur.execute(
                """
                SELECT
                    id,
                    name,
                    category,
                    gender,
                    status
                FROM league_seasons
                WHERE club_id = %s
                  AND status <> 'completed'
                ORDER BY created_at DESC;
                """,
                (club["id"],),
            )

            return cur.fetchall()

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

            try:
                notify_welcome(
                    email=email,
                    player_name=player["name"],
                )
            except Exception as e:
                print(f"Error enviando correo de bienvenida a {email}: {e}")

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
                    must_change_password,
                    password_hash
                FROM players
                WHERE LOWER(email) = %s
                LIMIT 1;
                """,
                (email,),
            )

            player = cur.fetchone()

            if (
                not player
                or not player["password_hash"]
                or not verify_password(data.password, player["password_hash"])
            ):
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
                "must_change_password": bool(player["must_change_password"]),
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
                    token=token,
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
                    must_change_password = FALSE,
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
            require_password_changed(player)
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

@app.post("/player/photo")
async def upload_player_photo_endpoint(
    session_token: str = Form(...),
    photo: UploadFile = File(...),
):
    content_type = (
        photo.content_type.lower()
        if photo.content_type
        else None
    )

    # Leemos como máximo 5 MB más un byte.
    raw_image = await photo.read(
        5 * 1024 * 1024 + 1
    )

    await photo.close()

    if len(raw_image) > 5 * 1024 * 1024:
        raise HTTPException(
            status_code=413,
            detail="La imagen supera el máximo de 5 MB",
        )

    try:
        processed_image = process_profile_image(
            raw_image=raw_image,
            content_type=content_type,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc

    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_authenticated_player(
                cur,
                session_token,
            )

            require_password_changed(player)

            cur.execute(
                """
                SELECT photo_url
                FROM players
                WHERE id = %s;
                """,
                (player["id"],),
            )

            current = cur.fetchone()
            previous_photo_url = (
                current["photo_url"]
                if current
                else None
            )

            try:
                object_key, photo_url = (
                    upload_player_photo(
                        player_id=player["id"],
                        image_bytes=processed_image,
                    )
                )
            except RuntimeError as exc:
                print(
                    "Error subiendo foto a R2:",
                    type(exc).__name__,
                    str(exc),
                )

                raise HTTPException(
                    status_code=502,
                    detail=(
                        "No se pudo guardar la foto "
                        "en el almacenamiento"
                    ),
                ) from exc

            try:
                cur.execute(
                    """
                    UPDATE players
                    SET photo_url = %s
                    WHERE id = %s
                    RETURNING photo_url;
                    """,
                    (
                        photo_url,
                        player["id"],
                    ),
                )

                updated = cur.fetchone()
                conn.commit()

            except Exception:
                conn.rollback()

                # Evita dejar el archivo nuevo huérfano.
                delete_player_photo_by_url(
                    photo_url,
                )

                raise

    # Se borra la anterior después del commit.
    if previous_photo_url != photo_url:
        delete_player_photo_by_url(
            previous_photo_url,
        )

    return {
        "message": "Foto actualizada correctamente",
        "photo_url": updated["photo_url"],
        "object_key": object_key,
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

@app.patch("/league-matches/{match_id}/schedule")
def update_league_match_schedule(match_id: int, data: LeagueMatchScheduleUpdate):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE league_matches
                SET scheduled_at = %s,
                    court = %s
                WHERE id = %s
                RETURNING *;
                """,
                (data.scheduled_at, data.court, match_id),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(status_code=404, detail="Partido de liga no encontrado")

            emails_sent = 0

            if data.notify_players:
                cur.execute(
                    """
                    SELECT
                        ls.name AS league_name,
                        c.name AS club_name,

                        COALESCE(pa.pair_name, p1a.name || ' / ' || p2a.name) AS pair_a_name,
                        COALESCE(pb.pair_name, p1b.name || ' / ' || p2b.name) AS pair_b_name,

                        p.email
                    FROM league_matches lm
                    JOIN league_seasons ls ON ls.id = lm.league_id
                    LEFT JOIN clubs c ON c.id = ls.club_id

                    JOIN league_pairs pa ON pa.id = lm.pair_a_id
                    JOIN players p1a ON p1a.id = pa.player_1_id
                    JOIN players p2a ON p2a.id = pa.player_2_id

                    JOIN league_pairs pb ON pb.id = lm.pair_b_id
                    JOIN players p1b ON p1b.id = pb.player_1_id
                    JOIN players p2b ON p2b.id = pb.player_2_id

                    JOIN players p ON p.id IN (
                        pa.player_1_id,
                        pa.player_2_id,
                        pb.player_1_id,
                        pb.player_2_id
                    )

                    WHERE lm.id = %s;
                    """,
                    (match_id,),
                )

                rows = cur.fetchall()

                for row in rows:
                    if not row["email"]:
                        continue

                    try:
                        notify_league_match_schedule(
                            email=row["email"],
                            league_name=row["league_name"],
                            club_name=row["club_name"],
                            pair_a_name=row["pair_a_name"],
                            pair_b_name=row["pair_b_name"],
                            scheduled_at=str(data.scheduled_at) if data.scheduled_at else None,
                            court=data.court,
                        )
                        emails_sent += 1
                    except Exception as e:
                        print(f"Error notificando partido de liga a {row['email']}: {e}")

                cur.execute(
                    """
                    UPDATE league_matches
                    SET last_schedule_notified_at = NOW()
                    WHERE id = %s;
                    """,
                    (match_id,),
                )

            conn.commit()

            return {
                "message": "Programación actualizada",
                "match": match,
                "emails_sent": emails_sent,
            }
