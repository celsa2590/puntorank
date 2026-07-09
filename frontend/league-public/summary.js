export function renderSummary(data) {
  const league = data.league;
  const pairsCount = data.pairs?.length || 0;
  const matchesCount = data.matches?.length || 0;
  const completed = data.matches?.filter(m => m.status === "completed").length || 0;

  const progress = matchesCount > 0
    ? Math.round((completed / matchesCount) * 100)
    : 0;

  document.getElementById("summary").innerHTML = `
    <div class="card">
      <h2>Resumen</h2>

      <div class="grid">
        <div>
          <p><strong>Club:</strong> ${league.club_name || "-"}</p>
          <p><strong>Categoría:</strong> ${league.category || "-"}</p>
          <p><strong>Género:</strong> ${league.gender || "-"}</p>
          <p><strong>Formato:</strong> ${league.format || "-"}</p>
        </div>

        <div>
          <p><strong>Estado:</strong> <span class="badge">${league.status || "-"}</span></p>
          <p><strong>Parejas:</strong> ${pairsCount}</p>
          <p><strong>Partidos:</strong> ${completed}/${matchesCount} jugados</p>
          <p><strong>Avance:</strong> ${progress}%</p>
        </div>
      </div>

      <div style="margin-top:18px;">
        <div style="height:12px; background:#e5e7eb; border-radius:999px; overflow:hidden;">
          <div style="height:100%; width:${progress}%; background:#18a957; border-radius:999px;"></div>
        </div>
      </div>
    </div>
  `;
}
