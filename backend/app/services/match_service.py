from datetime import datetime, timedelta
from fastapi import HTTPException

from app.config import MATCH_CONFIRMATION_HOURS
from app.services.notification_service import notify_match_confirmation

MATCH_SOURCE_RULES = {
    "friendly": {
        "requires_confirmation": True,
        "initial_status": "pending_confirmation",
    },
    "league": {
        "requires_confirmation": False,
        "initial_status": "approved",
    },
    "tournament": {
        "requires_confirmation": False,
        "initial_status": "approved",
    },
    "americano": {
        "requires_confirmation": False,
        "initial_status": "approved",
    },
}


def get_match_source_rules(match_source: str | None):
    source = match_source or "friendly"
    return MATCH_SOURCE_RULES.get(source, MATCH_SOURCE_RULES["friendly"])


def requires_confirmation(match_source: str | None) -> bool:
    return get_match_source_rules(match_source)["requires_confirmation"]


def initial_status_for_match(match_source: str | None) -> str:
    return get_match_source_rules(match_source)["initial_status"]


def build_match_summary(match, players):
    team_a = [p["player_name"] for p in players if p["team"] == "A"]
    team_b = [p["player_name"] for p in players if p["team"] == "B"]

    return f"""
    <p><strong>Club:</strong> {match.get("club_name", "Club no asignado")}</p>
    <p><strong>Resultado:</strong> {match.get("score", "")}</p>
    <p><strong>Equipo A:</strong> {" / ".join(team_a)}</p>
    <p><strong>Equipo B:</strong> {" / ".join(team_b)}</p>
    <p><strong>Ganador:</strong> Equipo {match.get("winning_team", "")}</p>
    """


def notify_friendly_match_players(cur, match_id: int, created_by_player_id: int | None = None):
    cur.execute(
        """
        SELECT
            m.id,
            m.score,
            mr.winning_team,
            c.name AS club_name
        FROM matches m
        LEFT JOIN match_results mr ON mr.match_id = m.id
        LEFT JOIN clubs c ON c.id = m.club_id
        WHERE m.id = %s;
        """,
        (match_id,),
    )

    match = cur.fetchone()

    if not match:
        raise HTTPException(status_code=404, detail="Partido no encontrado")

    cur.execute(
        """
        SELECT
            p.id AS player_id,
            p.name AS player_name,
            p.email,
            mp.team
        FROM match_players mp
        JOIN players p ON p.id = mp.player_id
        WHERE mp.match_id = %s
        ORDER BY mp.team, p.name;
        """,
        (match_id,),
    )

    players = cur.fetchall()
    summary = build_match_summary(match, players)

    sent = 0

    for player in players:
        if created_by_player_id and player["player_id"] == created_by_player_id:
            continue

        if not player["email"]:
            continue

        notify_match_confirmation(
            email=player["email"],
            match_id=match_id,
            player_id=player["player_id"],
            match_summary=summary,
        )

        sent += 1

    return sent


def register_match_metadata(
    cur,
    match_id: int,
    created_by_player_id: int | None,
    match_source: str = "friendly",
):
    rules = get_match_source_rules(match_source)

    cur.execute(
        """
        UPDATE matches
        SET status = %s,
            match_source = %s,
            requires_confirmation = %s,
            created_by_player_id = %s,
            confirmation_deadline = CASE
                WHEN %s = TRUE THEN NOW() + (%s || ' hours')::interval
                ELSE NULL
            END
        WHERE id = %s;
        """,
        (
            rules["initial_status"],
            match_source,
            rules["requires_confirmation"],
            created_by_player_id,
            rules["requires_confirmation"],
            MATCH_CONFIRMATION_HOURS,
            match_id,
        ),
    )
