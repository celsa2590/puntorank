export function renderSummary(data) {
  const league = data.league;
  const pairsCount = data.pairs?.length || 0;
  const matchesCount = data.matches?.length || 0;
  const completed = data.matches?.filter(m => m.status === "completed").length || 0;

  document.getElementById("summary").innerHTML = `
    <div class="card">
      <h2>Resumen</h2>
      <p><strong>Club:</strong> ${league.club_name || "-"}</p>
      <p><strong>Categoría:</strong> ${league.category || "-"}</p>
      <p><strong>Género:</strong> ${league.gender || "-"}</p>
      <p><strong>Formato:</strong> ${league.format || "-"}</p>
      <p><strong>Parejas:</strong> ${pairsCount}</p>
      <p><strong>Partidos:</strong> ${completed}/${matchesCount} jugados</p>
    </div>
  `;
}
