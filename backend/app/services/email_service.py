import os
import requests


MAILERSEND_API_URL = "https://api.mailersend.com/v1/email"


def get_email_from() -> str:
    return os.getenv("EMAIL_FROM", "notificaciones@puntorank.cl")


def send_email(to_email: str, subject: str, html: str, text: str | None = None):
    api_token = os.getenv("MAILERSEND_API_TOKEN")

    if not api_token:
        raise RuntimeError("MAILERSEND_API_TOKEN no está configurado")

    payload = {
        "from": {
            "email": get_email_from(),
            "name": "PuntoRank"
        },
        "to": [
            {
                "email": to_email
            }
        ],
        "subject": subject,
        "html": html,
        "text": text or "Tienes una notificación de PuntoRank."
    }

    response = requests.post(
        MAILERSEND_API_URL,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        },
        json=payload,
        timeout=15
    )

    if response.status_code not in (200, 202):
        raise RuntimeError(
            f"MailerSend error {response.status_code}: {response.text}"
        )

    return response.json() if response.text else {"status": "sent"}
