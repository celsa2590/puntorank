from app.templates.email.base_email import base_email_template


def league_match_schedule_template(
    league_name: str,
    club_name: str,
    pair_a_name: str,
    pair_b_name: str,
    scheduled_at: str | None,
    court: str | None,
) -> str:
    content = f"""
    <p>Tu partido de liga fue programado o actualizado.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      <p><strong>Liga:</strong> {league_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      <p><strong>Partido:</strong> {pair_a_name} vs {pair_b_name}</p>
      <p><strong>Fecha y hora:</strong> {scheduled_at or "Por definir"}</p>
      <p><strong>Cancha:</strong> {court or "Por definir"}</p>
    </div>

    <p>Si tienes dudas sobre el horario, contacta al club organizador.</p>
    """

    return base_email_template(
        title="Actualización de partido de liga",
        content=content,
        icon="📅",
        subtitle="Tu próximo partido ya tiene programación",
    )
