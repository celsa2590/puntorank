export function renderMatches(matches) {
  const container = document.getElementById("matches");
  container.innerHTML = "";

  if (!matches || matches.length === 0) {
    container.innerHTML = `<div class="card">Aún no hay partidos generados.</div>`;
    return;
  }

  const groups = {};

  matches.forEach(match => {
    const group = match.group_name || "Grupo único";
    const round = match.round_number || "Sin ronda";

    if (!groups[group]) groups[group] = {};
    if (!groups[group][round]) groups[group][round] = [];

    groups[group][round].push(match);
  });

  Object.keys(groups).forEach(groupName => {
    let groupHtml = `<div class="card"><h2>${groupName}</h2>`;

    Object.keys(groups[groupName]).forEach(roundNumber => {
      groupHtml += `<h3>Ronda ${roundNumber}</h3>`;

      groups[groupName][roundNumber].forEach(match => {
        const scheduled = match.scheduled_at
          ? new Date(match.scheduled_at).toLocaleString("es-CL", {
              dateStyle: "medium",
              timeStyle: "short",
            })
          : "Fecha por definir";

        const court = match.court || "Cancha por definir";

        const winner =
          match.winner_pair_id === match.pair_a_id
            ? match.pair_a_name
            : match.winner_pair_id === match.pair_b_id
              ? match.pair_b_name
              : null;

        const statusLabel = match.status === "completed"
          ? "Finalizado"
          : "Pendiente";

        groupHtml += `
          <div class="match-card">
            <div class="meta">📅 ${scheduled} · 🎾 ${court}</div>

            <div style="display:grid; grid-template-columns:1fr auto 1fr; gap:12px; align-items:center; margin-top:14px;">
              <div>
                <strong>${match.pair_a_name}</strong>
              </div>

              <div class="badge">VS</div>

              <div style="text-align:right;">
                <strong>${match.pair_b_name}</strong>
              </div>
            </div>

            <div style="margin-top:14px;">
              ${
                match.score
                  ? `<span class="badge">Resultado: ${match.score}</span>`
                  : `<span class="meta">Resultado pendiente</span>`
              }

              <span class="badge" style="margin-left:8px;">${statusLabel}</span>
            </div>

            ${
              winner
                ? `<div class="meta" style="margin-top:8px;">Ganador: <strong>${winner}</strong></div>`
                : ""
            }
          </div>
        `;
      });
    });

    groupHtml += `</div>`;
    container.innerHTML += groupHtml;
  });
}
