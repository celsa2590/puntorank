from app.templates.email.base_email import base_email_template


def schedule_changed_template(
    player_name: str,
    event_name: str,
    club_name: str,
    match_name: str,
    scheduled_at: str | None,
    court: str | None,
) -> str:
    content = f"""
    <p>Hola {player_name},</p>
    <p>La programación de tu partido fue actualizada.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      <p><strong>Evento:</strong> {event_name}</p>
      <p><strong>Club:</strong> {club_name}</p>
      <p><strong>Partido:</strong> {match_name}</p>
      <p><strong>Fecha y hora:</strong> {scheduled_at or "Por definir"}</p>
      <p><strong>Cancha:</strong> {court or "Por definir"}</p>
    </div>
    """

    return base_email_template(
        title="Cambio de programación",
        content=content,
        icon="🔄",
        subtitle="Tu partido fue actualizado",
    )
