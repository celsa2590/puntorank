export function renderHeader(league) {
  const logoHtml = league.club_logo_url
    ? `<img src="${league.club_logo_url}" alt="${league.club_name || "Club"}" style="width:72px;height:72px;object-fit:contain;border-radius:16px;background:white;border:1px solid #e5e7eb;padding:8px;">`
    : `<div style="width:72px;height:72px;border-radius:16px;background:#e6f7ee;display:flex;align-items:center;justify-content:center;font-size:34px;">🏟️</div>`;

  document.getElementById("leagueHeader").innerHTML = `
    <div style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;">
      ${logoHtml}

      <div>
        <h1>${league.name}</h1>
        <div class="meta">
          ${league.club_name || "Club no asignado"} · ${league.category || ""} · ${league.gender || ""}
        </div>
        <p>Estado: <strong>${league.status}</strong></p>
      </div>
    </div>
  `;
}
