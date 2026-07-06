import hashlib
import secrets
from app.config import PLAYER_SESSION_DAYS


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)

    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        salt.encode(),
        120000,
    ).hex()

    return f"{salt}${digest}"


def verify_password(password: str, stored_hash: str | None) -> bool:

    if not stored_hash or "$" not in stored_hash:
        return False

    salt, digest = stored_hash.split("$", 1)

    candidate = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode(),
        salt.encode(),
        120000,
    ).hex()

    return secrets.compare_digest(candidate, digest)


def hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def generate_token(length: int = 32) -> str:
    return secrets.token_urlsafe(length)
