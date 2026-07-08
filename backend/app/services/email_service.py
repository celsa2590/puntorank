import os
import requests


API_URL = "https://api.cloudflare.com/client/v4/accounts/{account_id}/email/v1"


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

    response = requests.post(
        API_URL.format(account_id=account_id),
        headers={
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        },
        json={
            "from": get_email_from(),
            "to": to_email,
            "subject": subject,
            "html": html,
            "text": text or "",
        },
        timeout=20,
    )

    if response.status_code >= 300:
        raise RuntimeError(
            f"Cloudflare error {response.status_code}: {response.text}"
        )

    return response.json()
