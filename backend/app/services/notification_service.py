from app.config import FRONTEND_URL
from app.services.email_service import send_email
from app.templates.email.password_reset import password_reset_template
from app.templates.email.match_confirmation import match_confirmation_template

def notify_password_reset(email: str, token: str):
    reset_url = f"{FRONTEND_URL}/player-reset-password.html?token={token}"

    html = password_reset_template(reset_url)

    return send_email(
        to_email=email,
        subject="Restablecer contraseña - PuntoRank",
        html=html,
        text=f"Restablece tu contraseña aquí: {reset_url}",
    )

def notify_match_confirmation(email: str, match_id: int, player_id: int, match_summary: str):
    confirm_url = f"{FRONTEND_URL}/confirm-match.html?match_id={match_id}&player_id={player_id}"

    html = match_confirmation_template(
        match_summary=match_summary,
        confirm_url=confirm_url,
    )

    return send_email(
        to_email=email,
        subject="Confirma tu resultado - PuntoRank",
        html=html,
        text=f"Confirma tu resultado aquí: {confirm_url}",
    )
