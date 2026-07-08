from app.templates.email.base_email import base_email_template


def match_result_confirmed_template(player_name: str, match_summary: str, rating_delta: str | None = None) -> str:
    rating_line = f"<p><strong>Cambio de ranking:</strong> {rating_delta}</p>" if rating_delta else ""

    content = f"""
    <p>Hola {player_name},</p>
    <p>El resultado de tu partido fue confirmado.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      {match_summary}
      {rating_line}
    </div>
    """

    return base_email_template(
        title="Resultado confirmado",
        content=content,
        icon="🎉",
        subtitle="Tu ranking fue actualizado",
    )
