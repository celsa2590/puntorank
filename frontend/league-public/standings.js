export function renderStandings(rows) {
  const container = document.getElementById("standings");
  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    container.innerHTML = `<div class="card">Aún no hay posiciones disponibles.</div>`;
    return;
  }

  const groups = {};

  rows.forEach(r => {
    const group = r.group_name || "Grupo único";
    if (!groups[group]) groups[group] = [];
    groups[group].push(r);
  });

  Object.keys(groups).forEach(group => {
    let html = `<div class="card"><h2>${group}</h2>`;

    groups[group].forEach((team, index) => {
      const medal = index === 0 ? "🥇" : index === 1 ? "🥈" : index === 2 ? "🥉" : `${index + 1}.`;

      html += `
        <div class="ranking-card">
          <div class="position">${medal}</div>

          <div>
            <div class="name">${team.pair_name}</div>
            <div class="meta">
              PJ ${team.played} · PG ${team.wins} · PP ${team.losses}
            </div>
          </div>

          <div class="rating-box">
            ${team.points}
            <div class="meta">pts</div>
          </div>
        </div>
      `;
    });

    html += `</div>`;
    container.innerHTML += html;
  });
}
