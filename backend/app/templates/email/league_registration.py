from app.templates.email.base_email import base_email_template


def league_registration_template(player_name: str, league_name: str, club_name: str, pair_name: str | None = None) -> str:
    pair_line = f"<p><strong>Pareja:</strong> {pair_name}</p>" if pair_name else ""

    content = f"""
    <p>Hola {player_name},</p>
    <p>Fuiste inscrita/o en una liga de PuntoRank.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      <p><strong>Liga:</strong> {league_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      {pair_line}
    </div>

    <p>Te avisaremos cuando el fixture esté disponible.</p>
    """

    return base_email_template(
        title="Inscripción a liga",
        content=content,
        icon="🏆",
        subtitle="Ya formas parte de una liga",
    )
