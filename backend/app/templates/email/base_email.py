from app.config import FRONTEND_URL


def base_email_template(
    title: str,
    content: str,
    button_text: str | None = None,
    button_url: str | None = None,
    icon: str = "🎾",
    subtitle: str = "El ranking donde cada partido cuenta",
) -> str:
    button_html = ""

    if button_text and button_url:
        button_html = f"""
        <div style="text-align:center; margin:30px 0;">
            <a href="{button_url}"
               style="background:#16a34a; color:#ffffff; padding:15px 24px; border-radius:999px;
                      text-decoration:none; font-weight:800; display:inline-block; font-size:15px;">
                {button_text}
            </a>
        </div>
        """

    link_help = ""
    if button_url:
        link_help = f"""
        <p style="font-size:13px; color:#6b7280; line-height:1.5; margin-top:24px;">
          Si el botón no funciona, copia y pega este enlace en tu navegador:<br>
          <span style="word-break:break-all;">{button_url}</span>
        </p>
        """

    return f"""
    <div style="margin:0; padding:0; background:#f3f6f4; font-family:Arial, Helvetica, sans-serif;">
      <div style="max-width:640px; margin:0 auto; padding:28px 16px;">
        <div style="background:#ffffff; border-radius:22px; overflow:hidden; border:1px solid #e5e7eb;">

          <div style="background:#0f172a; color:#ffffff; padding:30px 24px; text-align:center;">
            <div style="font-size:38px; line-height:1;">{icon}</div>
            <div style="font-size:28px; font-weight:900; margin-top:10px;">PuntoRank</div>
            <div style="font-size:14px; opacity:0.85; margin-top:6px;">{subtitle}</div>
          </div>

          <div style="padding:30px 26px;">
            <h2 style="margin:0 0 18px; color:#111827; font-size:24px;">{title}</h2>

            <div style="color:#374151; font-size:15px; line-height:1.65;">
              {content}
            </div>

            {button_html}
            {link_help}

            <div style="margin-top:30px; padding-top:18px; border-top:1px solid #e5e7eb;">
              <p style="margin:0; color:#374151; font-size:14px;">
                Nos vemos en la cancha 🎾<br>
                <strong>Equipo PuntoRank</strong>
              </p>
            </div>
          </div>
        </div>

        <div style="text-align:center; color:#6b7280; font-size:12px; margin-top:16px;">
          © PuntoRank · <a href="{FRONTEND_URL}" style="color:#16a34a;">{FRONTEND_URL}</a>
        </div>
      </div>
    </div>
    """
