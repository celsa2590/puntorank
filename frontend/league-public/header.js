export function renderHeader(league) {
  document.getElementById("leagueHeader").innerHTML = `
    <h1>${league.name}</h1>
    <div class="meta">
      ${league.club_name || "Club no asignado"} · ${league.category || ""} · ${league.gender || ""}
    </div>
    <p>Estado: <strong>${league.status}</strong></p>
  `;
}
