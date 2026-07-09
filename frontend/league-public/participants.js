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
    let html = `<div class="card"><h2>${groupName}</h2>`;

    groups[groupName].forEach(pair => {
      html += `
        <div class="row">
          <strong>${pair.pair_name}</strong>
          <div class="meta">
            👤 ${pair.player_1_name}<br>
            👤 ${pair.player_2_name}
          </div>
        </div>
      `;
    });

    html += `</div>`;
    container.innerHTML += html;
  });
}
