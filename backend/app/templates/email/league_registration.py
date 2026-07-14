from app.config import FRONTEND_URL
from app.templates.email.base_email import base_email_template


def league_registration_template(
    player_name: str,
    league_name: str,
    club_name: str,
    pair_name: str | None = None,
) -> str:
    pair_line = (
        f"<p><strong>Pareja:</strong> {pair_name}</p>"
        if pair_name
        else ""
    )

    content = f"""
    <p>Hola {player_name},</p>

    <p>
      Fuiste inscrita/o en una liga de PuntoRank.
    </p>

    <div
      style="
        background:#f9fafb;
        border:1px solid #e5e7eb;
        border-radius:12px;
        padding:16px;
        margin:18px 0;
      "
    >
      <p><strong>Liga:</strong> {league_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      {pair_line}
    </div>

    <p>
      Te avisaremos cuando el fixture esté disponible.
    </p>
    """

    return base_email_template(
        title="Inscripción a liga",
        content=content,
        icon="🏆",
        subtitle="Ya formas parte de una liga",
    )


def league_welcome_email_template(
    player_name: str,
    league_name: str,
    club_name: str,
    league_id: int,
    pair_name: str | None = None,
) -> tuple[str, str]:
    league_url = (
        f"{FRONTEND_URL}/league-public.html?id={league_id}"
    )

    pair_line_html = (
        f"<p><strong>Pareja:</strong> {pair_name}</p>"
        if pair_name
        else ""
    )

    content = f"""
    <p>Hola <strong>{player_name}</strong>,</p>

    <p>
      <strong>{club_name}</strong> te da la bienvenida a
      <strong>{league_name}</strong>.
    </p>

    <div
      style="
        background:#f9fafb;
        border:1px solid #e5e7eb;
        border-radius:12px;
        padding:16px;
        margin:18px 0;
      "
    >
      <p><strong>Liga:</strong> {league_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      {pair_line_html}
    </div>

    <p>
      Desde PuntoRank podrás revisar el fixture,
      los resultados y la tabla de posiciones.
    </p>
    """

    html = base_email_template(
        title=f"Bienvenida/o a {league_name}",
        content=content,
        button_text="Ver liga",
        button_url=league_url,
        icon="🏆",
        subtitle=f"Organizada por {club_name}",
    )

    text_lines = [
        f"Bienvenida/o a {league_name}",
        "",
        f"Hola {player_name},",
        "",
        (
            f"{club_name} te da la bienvenida a "
            f"{league_name}."
        ),
        "",
        f"Liga: {league_name}",
        f"Club: {club_name}",
    ]

    if pair_name:
        text_lines.append(f"Pareja: {pair_name}")

    text_lines.extend(
        [
            "",
            (
                "Puedes revisar el fixture, los resultados "
                "y la tabla de posiciones aquí:"
            ),
            league_url,
        ]
    )

    text = "\n".join(text_lines)

    return html, text
