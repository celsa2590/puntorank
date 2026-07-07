from app.config import FRONTEND_URL
from app.services.email_service import send_email
from app.templates.email.password_reset import password_reset_template


def notify_password_reset(email: str, token: str):
    reset_url = f"{FRONTEND_URL}/player-reset-password.html?token={token}"

    html = password_reset_template(reset_url)

    return send_email(
        to_email=email,
        subject="Restablecer contraseña - PuntoRank",
        html=html,
        text=f"Restablece tu contraseña aquí: {reset_url}",
    )
