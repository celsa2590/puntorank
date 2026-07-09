export function renderParticipants(pairs) {
  const container = document.getElementById("participants");
  container.innerHTML = "";

  if (!pairs || pairs.length === 0) {
    container.innerHTML = `<div class="card">Aún no hay parejas inscritas.</div>`;
    return;
  }

  const groups = {};

  pairs.forEach(pair => {
    const group = pair.group_name || "Grupo único";
    if (!groups[group]) groups[group] = [];
    groups[group].push(pair);
  });

  Object.keys(groups).forEach(groupName => {
    let html = `<div class="card"><h2>${groupName}</h2><div class="grid">`;

    groups[groupName].forEach(pair => {
      html += `
        <div class="card">
          <h3>${pair.pair_name}</h3>

          <div class="row">
            👤 <a href="player-profile.html?id=${pair.player_1_id || ""}">
              ${pair.player_1_name}
            </a>
          </div>

          <div class="row">
            👤 <a href="player-profile.html?id=${pair.player_2_id || ""}">
              ${pair.player_2_name}
            </a>
          </div>
        </div>
      `;
    });

    html += `</div></div>`;
    container.innerHTML += html;
  });
}
