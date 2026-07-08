from app.templates.email.base_email import base_email_template


def match_reminder_template(
    player_name: str,
    event_name: str,
    club_name: str,
    match_name: str,
    scheduled_at: str,
    court: str | None,
) -> str:
    content = f"""
    <p>Hola {player_name},</p>
    <p>Te recordamos tu próximo partido.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      <p><strong>Evento:</strong> {event_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      <p><strong>Partido:</strong> {match_name}</p>
      <p><strong>Fecha y hora:</strong> {scheduled_at}</p>
      <p><strong>Cancha:</strong> {court or "Por definir"}</p>
    </div>

    <p>¡Nos vemos en la cancha!</p>
    """

    return base_email_template(
        title="Recordatorio de partido",
        content=content,
        icon="⏰",
        subtitle="Tu partido se acerca",
    )
