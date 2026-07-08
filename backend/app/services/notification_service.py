from app.config import FRONTEND_URL
from app.services.email_service import send_email
from app.templates.email.password_reset import password_reset_template
from app.templates.email.match_confirmation import match_confirmation_template
from app.templates.email.league_match_schedule import league_match_schedule_template
from app.templates.email.welcome import welcome_template
from app.templates.email.email_verification import (
    email_verification_template,
)

from app.templates.email.league_registration import (
    league_registration_template,
)

from app.templates.email.fixture_published import (
    fixture_published_template,
)

from app.templates.email.schedule_changed import (
    schedule_changed_template,
)

from app.templates.email.match_result_confirmed import (
    match_result_confirmed_template,
)

from app.templates.email.match_disputed import (
    match_disputed_template,
)

from app.templates.email.match_reminder import (
    match_reminder_template,
)


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
def notify_welcome(email: str, player_name: str):
    html = welcome_template(player_name)

    return send_email(
        to_email=email,
        subject="Bienvenida a PuntoRank",
        html=html,
        text=f"Hola {player_name}, tu perfil en PuntoRank fue creado correctamente.",
    )
def notify_email_verification(email, player_name, verify_url):

    html = email_verification_template(
        player_name,
        verify_url,
    )

    return send_email(
        to_email=email,
        subject="Confirma tu correo",
        html=html,
    )

def notify_league_registration(
    email,
    player_name,
    league_name,
    club_name,
    pair_name=None,
):

    html = league_registration_template(
        player_name,
        league_name,
        club_name,
        pair_name,
    )

    return send_email(
        to_email=email,
        subject="Inscripción a liga",
        html=html,
    )
def notify_fixture_published(
    email,
    player_name,
    league_name,
    club_name,
    fixture_url,
):

    html = fixture_published_template(
        player_name,
        league_name,
        club_name,
        fixture_url,
    )

    return send_email(
        to_email=email,
        subject="Fixture disponible",
        html=html,
    )
def notify_schedule_changed(
    email,
    player_name,
    event_name,
    club_name,
    match_name,
    scheduled_at,
    court,
):

    html = schedule_changed_template(
        player_name,
        event_name,
        club_name,
        match_name,
        scheduled_at,
        court,
    )

    return send_email(
        to_email=email,
        subject="Cambio de programación",
        html=html,
    )
def notify_match_reminder(
    email,
    player_name,
    event_name,
    club_name,
    match_name,
    scheduled_at,
    court,
):

    html = match_reminder_template(
        player_name,
        event_name,
        club_name,
        match_name,
        scheduled_at,
        court,
    )

    return send_email(
        to_email=email,
        subject="Recordatorio de partido",
        html=html,
    )
def notify_match_result_confirmed(
    email,
    player_name,
    summary,
    rating_delta=None,
):

    html = match_result_confirmed_template(
        player_name,
        summary,
        rating_delta,
    )

    return send_email(
        to_email=email,
        subject="Resultado confirmado",
        html=html,
    )
def notify_match_disputed(
    email,
    player_name,
    summary,
):

    html = match_disputed_template(
        player_name,
        summary,
    )

    return send_email(
        to_email=email,
        subject="Resultado en disputa",
        html=html,
    )
