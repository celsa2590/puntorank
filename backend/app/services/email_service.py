import os
import requests


def get_email_from():
    return os.getenv("EMAIL_FROM", "notificaciones@puntorank.cl")


def send_email(
    to_email: str,
    subject: str,
    html: str,
    text: str | None = None,
):
    api_token = os.getenv("CLOUDFLARE_API_TOKEN")
    account_id = os.getenv("CLOUDFLARE_ACCOUNT_ID")

    if not api_token:
        raise RuntimeError("CLOUDFLARE_API_TOKEN no configurado")

    if not account_id:
        raise RuntimeError("CLOUDFLARE_ACCOUNT_ID no configurado")

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{account_id}/email/sending/send"
    )

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        json={
            "from": get_email_from(),
            "to": to_email,
            "subject": subject,
            "html": html,
            "text": text or "Tienes una notificación de PuntoRank.",
        },
        timeout=20,
    )

    if response.status_code >= 300:
        raise RuntimeError(
            f"Cloudflare error {response.status_code}: {response.text}"
        )

    return response.json()
