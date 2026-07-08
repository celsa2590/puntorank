import os
import requests


CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID")
CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
EMAIL_FROM = os.getenv("EMAIL_FROM")


def send_email(to, subject, html):

    url = (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{CLOUDFLARE_ACCOUNT_ID}/email/routing/messages"
    )

    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "from": EMAIL_FROM,
        "to": to,
        "subject": subject,
        "html": html,
    }

    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=20,
    )

    if response.status_code >= 300:
        raise Exception(
            f"Cloudflare error {response.status_code}: {response.text}"
        )

    return response.json()
