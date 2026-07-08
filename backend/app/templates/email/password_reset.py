from app.templates.email.base_email import base_email_template


def password_reset_template(reset_url: str) -> str:
    content = """
    <p>Recibimos una solicitud para restablecer tu contraseña.</p>
    <p>Haz clic en el botón para crear una nueva contraseña.</p>
    <p>Este enlace expira en 30 minutos.</p>
    <p>Si tú no solicitaste este cambio, puedes ignorar este correo.</p>
    """

    return base_email_template(
        title="Restablecer contraseña",
        content=content,
        button_text="Restablecer contraseña",
        button_url=reset_url,
        icon="🔐",
        subtitle="Recupera el acceso a tu cuenta",
    )
