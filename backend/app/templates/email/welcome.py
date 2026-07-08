from app.templates.email.base_email import base_email_template


def welcome_template(player_name: str) -> str:
    content = f"""
    <p>Hola {player_name},</p>

    <p>Tu perfil en PuntoRank fue creado correctamente.</p>

    <p>Desde ahora podrás:</p>

    <ul>
      <li>Registrar partidos amistosos.</li>
      <li>Participar en ligas y torneos.</li>
      <li>Ver tu ranking y progreso.</li>
      <li>Confirmar resultados enviados por otros jugadores.</li>
    </ul>

    <p>¡Nos vemos en la cancha!</p>
    """

    return base_email_template(
        title="Bienvenida a PuntoRank",
        content=content,
        icon="🏆",
        subtitle="Tu cuenta ya está lista",
    )
