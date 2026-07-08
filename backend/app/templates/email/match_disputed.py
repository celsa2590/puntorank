from app.templates.email.base_email import base_email_template


def match_disputed_template(player_name: str, match_summary: str) -> str:
    content = f"""
    <p>Hola {player_name},</p>
    <p>Un resultado fue marcado como disputado.</p>

    <div style="background:#fff7ed; border:1px solid #fed7aa; border-radius:12px; padding:16px; margin:18px 0;">
      {match_summary}
    </div>

    <p>El club organizador deberá revisar el caso.</p>
    """

    return base_email_template(
        title="Partido en disputa",
        content=content,
        icon="⚠️",
        subtitle="El resultado requiere revisión",
    )
