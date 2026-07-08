from app.config import FRONTEND_URL
from app.services.email_service import send_email
from app.templates.email.password_reset import password_reset_template
from app.templates.email.match_confirmation import match_confirmation_template
from app.templates.email.league_match_schedule import league_match_schedule_template

def notify_password_reset(email: str, token: str):
    reset_url = f"{FRONTEND_URL}/player-reset-password.html?token={token}"

    html = password_reset_template(reset_url)

    return send_email(
        to_email=email,
        subject="Restablecer contraseña - PuntoRank",
        html=html,
        text=f"Restablece tu contraseña aquí: {reset_url}",
    )

def notify_match_confirmation(email: str, confirmation_token: str, match_summary: str):
    confirm_url = f"{FRONTEND_URL}/confirm-match.html?token={confirmation_token}"

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
def notify_league_match_schedule(
    email: str,
    league_name: str,
    club_name: str,
    pair_a_name: str,
    pair_b_name: str,
    scheduled_at: str | None,
    court: str | None,
):
    html = league_match_schedule_template(
        league_name=league_name,
        club_name=club_name,
        pair_a_name=pair_a_name,
        pair_b_name=pair_b_name,
        scheduled_at=scheduled_at,
        court=court,
    )

    return send_email(
        to_email=email,
        subject="Actualización de partido de liga - PuntoRank",
        html=html,
        text=f"{league_name}: {pair_a_name} vs {pair_b_name} - {scheduled_at or 'Por definir'} - Cancha {court or 'Por definir'}",
    )
