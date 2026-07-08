const API_URL = "https://puntorank-backend.onrender.com";

const params = new URLSearchParams(window.location.search);
const leagueId = params.get("id");

loadLeague();

async function loadLeague() {

    const res = await fetch(`${API_URL}/public/leagues/${leagueId}`);
    const data = await res.json();

    renderHeader(data.league);

    renderStandings(data.standings);

    renderMatches(data.matches);

}

function renderHeader(league){

    document.getElementById("leagueHeader").innerHTML=`

        <h1>${league.name}</h1>

        <p>

            ${league.club_name}

            •

            ${league.category}

            •

            ${league.gender}

        </p>

        <p>

            Estado:

            <strong>${league.status}</strong>

        </p>

    `;

}
function renderStandings(rows){

    const container=document.getElementById("standingsContainer");

    container.innerHTML="";

    const groups={};

    rows.forEach(r=>{

        if(!groups[r.group_name])
            groups[r.group_name]=[];

        groups[r.group_name].push(r);

    });

    Object.keys(groups).forEach(group=>{

        let html=`

            <h3>${group}</h3>

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

        groups[group].forEach((team,index)=>{

            html+=`

            <tr>

                <td>${index+1}</td>

                <td>${team.pair_name}</td>

                <td>${team.played}</td>

                <td>${team.wins}</td>

                <td>${team.losses}</td>

                <td><b>${team.points}</b></td>

            </tr>

            `;

        });

        html+="</tbody></table>";

        container.innerHTML+=html;

    });

}

function renderMatches(matches) {
  const container = document.getElementById("matchesContainer");
  container.innerHTML = "";

  if (!matches || matches.length === 0) {
    container.innerHTML = `<div class="meta">Aún no hay partidos generados.</div>`;
    return;
  }

  const groups = {};

  matches.forEach(m => {
    const group = m.group_name || "Grupo único";
    const round = m.round_number || "Sin ronda";

    if (!groups[group]) groups[group] = {};
    if (!groups[group][round]) groups[group][round] = [];

    groups[group][round].push(m);
  });

  Object.keys(groups).forEach(groupName => {
    container.innerHTML += `<h3>${groupName}</h3>`;

    Object.keys(groups[groupName]).forEach(roundNumber => {
      let html = `<div class="card"><h4>Ronda ${roundNumber}</h4>`;

      groups[groupName][roundNumber].forEach(match => {
        const scheduled = match.scheduled_at
          ? new Date(match.scheduled_at).toLocaleString("es-CL")
          : "Fecha por definir";

        const court = match.court || "Cancha por definir";

        const winner =
          match.winner_pair_id === match.pair_a_id
            ? match.pair_a_name
            : match.winner_pair_id === match.pair_b_id
              ? match.pair_b_name
              : null;

        html += `
          <div class="match-card" style="border:1px solid #e5e7eb; border-radius:14px; padding:14px; margin:12px 0;">
            <div class="meta">${scheduled} · ${court}</div>

            <div style="margin-top:8px;">
              <strong>${match.pair_a_name}</strong>
              <div class="meta">vs</div>
              <strong>${match.pair_b_name}</strong>
            </div>

            <div style="margin-top:8px;">
              ${
                match.score
                  ? `<span class="badge">Resultado: ${match.score}</span>`
                  : `<span class="meta">Resultado pendiente</span>`
              }
            </div>

            ${
              winner
                ? `<div class="meta" style="margin-top:6px;">Ganador: <strong>${winner}</strong></div>`
                : ""
            }
          </div>
        `;
      });

      html += `</div>`;
      container.innerHTML += html;
    });
  });
}
