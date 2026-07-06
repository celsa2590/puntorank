import os

FRONTEND_URL = os.getenv("FRONTEND_URL", "https://www.puntorank.cl")
EMAIL_FROM = os.getenv("EMAIL_FROM", "notificaciones@puntorank.cl")

PLAYER_SESSION_DAYS = 30
PASSWORD_RESET_MINUTES = 30
MATCH_CONFIRMATION_HOURS = 24

DEFAULT_RATING = 1000
BASE_K_FACTOR = 32
