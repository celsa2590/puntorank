export function renderStandings(rows) {
  const container = document.getElementById("standings");
  container.innerHTML = "";

  const groups = {};

  rows.forEach(r => {
    const group = r.group_name || "Grupo único";
    if (!groups[group]) groups[group] = [];
    groups[group].push(r);
  });

  Object.keys(groups).forEach(group => {
    let html = `
      <div class="card">
        <h2>${group}</h2>
        <table class="table">
          <thead>
            <tr>
              <th>#</th>
              <th>Pareja</th>
              <th>PJ</th>
              <th>PG</th>
              <th>PP</th>
              <th>Pts</th>
            </tr>
          </thead>
          <tbody>
    `;

    groups[group].forEach((team, index) => {
      html += `
        <tr>
          <td>${index + 1}</td>
          <td>${team.pair_name}</td>
          <td>${team.played}</td>
          <td>${team.wins}</td>
          <td>${team.losses}</td>
          <td><strong>${team.points}</strong></td>
        </tr>
      `;
    });

    html += `</tbody></table></div>`;
    container.innerHTML += html;
  });
}
