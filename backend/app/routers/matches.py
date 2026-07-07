from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.database import get_conn
from app.schemas import MatchConfirmationTokenRequest
from app.services.auth_service import hash_session_token
from app.services.rating_service import update_ratings_for_match


router = APIRouter()


def get_player_from_session(cur, session_token: str):
    token_hash = hash_session_token(session_token)

    cur.execute(
        """
        SELECT p.*
        FROM player_sessions ps
        JOIN players p ON p.id = ps.player_id
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


def get_confirmation_context(cur, confirmation_token: str, session_player_id: int):
    confirmation_hash = hash_session_token(confirmation_token)

    cur.execute(
        """
        SELECT
            mct.id AS token_id,
            mct.match_id,
            mct.player_id,
            mct.expires_at,
            mct.used_at
        FROM match_confirmation_tokens mct
        WHERE mct.token_hash = %s;
        """,
        (confirmation_hash,),
    )

    token_row = cur.fetchone()

    if not token_row:
        raise HTTPException(status_code=404, detail="Enlace inválido")

    if token_row["player_id"] != session_player_id:
        raise HTTPException(status_code=403, detail="Este enlace no corresponde a tu usuario")

    if token_row["used_at"]:
        raise HTTPException(status_code=400, detail="Este enlace ya fue utilizado")

    if token_row["expires_at"] < datetime.utcnow():
        raise HTTPException(status_code=400, detail="Este enlace expiró")

    return token_row


@router.post("/player/matches/confirmation-preview")
def player_match_confirmation_preview(data: MatchConfirmationTokenRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_player_from_session(cur, data.session_token)

            token_row = get_confirmation_context(
                cur,
                data.confirmation_token,
                player["id"],
            )

            cur.execute(
                """
                SELECT
                    m.id,
                    m.status,
                    m.played_at,
                    m.match_type,
                    c.name AS club_name,
                    mr.score,
                    mr.winning_team
                FROM matches m
                LEFT JOIN clubs c ON c.id = m.club_id
                LEFT JOIN match_results mr ON mr.match_id = m.id
                WHERE m.id = %s;
                """,
                (token_row["match_id"],),
            )

            match = cur.fetchone()

            if not match:
                raise HTTPException(status_code=404, detail="Partido no encontrado")

            cur.execute(
                """
                SELECT
                    p.id,
                    p.name,
                    mp.team
                FROM match_players mp
                JOIN players p ON p.id = mp.player_id
                WHERE mp.match_id = %s
                ORDER BY mp.team, p.name;
                """,
                (token_row["match_id"],),
            )

            players = cur.fetchall()

            return {
                **match,
                "players": players,
            }


@router.post("/player/matches/confirm")
def player_match_confirm(data: MatchConfirmationTokenRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_player_from_session(cur, data.session_token)

            token_row = get_confirmation_context(
                cur,
                data.confirmation_token,
                player["id"],
            )

            match_id = token_row["match_id"]

            cur.execute(
                """
                INSERT INTO match_confirmations
                    (match_id, player_id, confirmed, disputed)
                VALUES
                    (%s, %s, TRUE, FALSE)
                ON CONFLICT (match_id, player_id)
                DO UPDATE SET confirmed = TRUE,
                              disputed = FALSE,
                              created_at = NOW();
                """,
                (match_id, player["id"]),
            )

            cur.execute(
                """
                UPDATE match_confirmation_tokens
                SET used_at = NOW()
                WHERE id = %s;
                """,
                (token_row["token_id"],),
            )

            cur.execute(
                """
                SELECT COUNT(*) AS confirmations
                FROM match_confirmations
                WHERE match_id = %s
                  AND confirmed = TRUE;
                """,
                (match_id,),
            )

            confirmations = cur.fetchone()["confirmations"]
            auto_approved = False

            if confirmations >= 3:
                cur.execute(
                    """
                    UPDATE matches
                    SET status = 'approved',
                        approved_at = NOW()
                    WHERE id = %s
                      AND status <> 'approved'
                      AND COALESCE(rating_processed, FALSE) = FALSE;
                    """,
                    (match_id,),
                )

                update_ratings_for_match(cur, match_id)

                cur.execute(
                    """
                    UPDATE matches
                    SET rating_processed = TRUE,
                        rating_applied_at = NOW()
                    WHERE id = %s;
                    """,
                    (match_id,),
                )

                auto_approved = True

            conn.commit()

            return {
                "message": "Confirmación registrada",
                "confirmations": confirmations,
                "auto_approved": auto_approved,
            }


@router.post("/player/matches/dispute")
def player_match_dispute(data: MatchConfirmationTokenRequest):
    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_player_from_session(cur, data.session_token)

            token_row = get_confirmation_context(
                cur,
                data.confirmation_token,
                player["id"],
            )

            match_id = token_row["match_id"]

            cur.execute(
                """
                INSERT INTO match_confirmations
                    (match_id, player_id, confirmed, disputed)
                VALUES
                    (%s, %s, FALSE, TRUE)
                ON CONFLICT (match_id, player_id)
                DO UPDATE SET disputed = TRUE,
                              confirmed = FALSE,
                              created_at = NOW();
                """,
                (match_id, player["id"]),
            )

            cur.execute(
                """
                UPDATE matches
                SET status = 'disputed',
                    dispute_opened_at = NOW()
                WHERE id = %s;
                """,
                (match_id,),
            )

            cur.execute(
                """
                UPDATE match_confirmation_tokens
                SET used_at = NOW()
                WHERE id = %s;
                """,
                (token_row["token_id"],),
            )

            conn.commit()

            return {"message": "Partido marcado como disputado"}
