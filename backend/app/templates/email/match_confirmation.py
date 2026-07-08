from app.templates.email.base_email import base_email_template


def match_confirmation_template(match_summary: str, confirm_url: str) -> str:
    content = f"""
    <p>Se registró un resultado de partido en PuntoRank.</p>

    <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:12px; padding:16px; margin:18px 0;">
      {match_summary}
    </div>

    <p>Si el resultado es correcto, confírmalo usando el botón.</p>
    <p>Si no reconoces este resultado o hay un error, no lo confirmes. Más adelante agregaremos la opción de disputa.</p>
    """

    return base_email_template(
        title="Confirma el resultado de tu partido",
        content=content,
        button_text="Confirmar resultado",
        button_url=confirm_url,
        icon="✅",
        subtitle="Valida el resultado y protege tu ranking",
    )
