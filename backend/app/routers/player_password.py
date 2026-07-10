from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.database import get_conn
from app.services.auth_service import hash_password, hash_session_token, verify_password

router = APIRouter(tags=["player-auth"])


class PlayerChangePasswordRequest(BaseModel):
    session_token: str
    current_password: str
    new_password: str


def get_player_from_session(cur, session_token: str):
    if not session_token:
        raise HTTPException(status_code=401, detail="Sesión requerida")

    cur.execute(
        """
        SELECT p.id, p.password_hash, p.must_change_password
        FROM player_sessions ps
        JOIN players p ON p.id = ps.player_id
        WHERE ps.token_hash = %s
          AND ps.revoked_at IS NULL
          AND ps.expires_at > NOW()
        LIMIT 1;
        """,
        (hash_session_token(session_token),),
    )

    player = cur.fetchone()
    if not player:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")
    return player


@router.post("/player/change-password")
def player_change_password(data: PlayerChangePasswordRequest):
    if len(data.new_password) < 8:
        raise HTTPException(
            status_code=400,
            detail="La nueva contraseña debe tener al menos 8 caracteres",
        )

    if data.current_password == data.new_password:
        raise HTTPException(
            status_code=400,
            detail="La nueva contraseña debe ser distinta de la contraseña actual",
        )

    with get_conn() as conn:
        with conn.cursor() as cur:
            player = get_player_from_session(cur, data.session_token)

            if (
                not player["password_hash"]
                or not verify_password(data.current_password, player["password_hash"])
            ):
                raise HTTPException(
                    status_code=400,
                    detail="La contraseña actual no es correcta",
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

    return {
        "message": "Contraseña actualizada correctamente",
        "must_change_password": False,
    }
