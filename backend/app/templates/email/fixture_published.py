from app.templates.email.base_email import base_email_template


def fixture_published_template(player_name: str, league_name: str, club_name: str, fixture_url: str) -> str:
    content = f"""
    <p>Hola {player_name},</p>
    <p>El fixture de tu liga ya está disponible.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      <p><strong>Liga:</strong> {league_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
    </div>
    """

    return base_email_template(
        title="Fixture publicado",
        content=content,
        button_text="Ver fixture",
        button_url=fixture_url,
        icon="📅",
        subtitle="Revisa tus próximos partidos",
    )
