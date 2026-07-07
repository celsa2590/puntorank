from app.config import FRONTEND_URL


def base_email_template(title: str, content: str, button_text: str | None = None, button_url: str | None = None) -> str:
    button_html = ""

    if button_text and button_url:
        button_html = f"""
        <div style="text-align:center; margin:28px 0;">
            <a href="{button_url}"
               style="background:#16a34a; color:#ffffff; padding:14px 22px; border-radius:10px;
                      text-decoration:none; font-weight:700; display:inline-block;">
                {button_text}
            </a>
        </div>
        """

    return f"""
    <div style="margin:0; padding:0; background:#f4f7f5; font-family:Arial, sans-serif;">
      <div style="max-width:620px; margin:0 auto; padding:28px 16px;">
        <div style="background:#ffffff; border-radius:18px; overflow:hidden; border:1px solid #e5e7eb;">
          <div style="background:#0f172a; color:#ffffff; padding:24px; text-align:center;">
            <div style="font-size:26px; font-weight:800;">PuntoRank 🎾</div>
            <div style="font-size:14px; opacity:0.85; margin-top:6px;">Ranking, ligas y torneos de pádel</div>
          </div>

          <div style="padding:28px;">
            <h2 style="margin:0 0 16px; color:#111827;">{title}</h2>

            <div style="color:#374151; font-size:15px; line-height:1.6;">
              {content}
            </div>

            {button_html}

            <p style="font-size:13px; color:#6b7280; line-height:1.5;">
              Si el botón no funciona, copia y pega este enlace en tu navegador:<br>
              <span style="word-break:break-all;">{button_url or FRONTEND_URL}</span>
            </p>
          </div>
        </div>

        <div style="text-align:center; color:#6b7280; font-size:12px; margin-top:16px;">
          © PuntoRank · <a href="{FRONTEND_URL}" style="color:#16a34a;">{FRONTEND_URL}</a>
        </div>
      </div>
    </div>
    """
