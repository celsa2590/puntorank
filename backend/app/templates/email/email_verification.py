from app.templates.email.base_email import base_email_template


def email_verification_template(player_name: str, verify_url: str) -> str:
    content = f"""
    <p>Hola {player_name},</p>
    <p>Confirma tu correo para activar tu cuenta en PuntoRank.</p>
    <p>Esto nos ayuda a proteger tu perfil y validar tus notificaciones.</p>
    """

    return base_email_template(
        title="Confirma tu correo",
        content=content,
        button_text="Confirmar correo",
        button_url=verify_url,
        icon="📩",
        subtitle="Activa tu cuenta de PuntoRank",
    )
