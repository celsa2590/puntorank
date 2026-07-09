export function renderStatistics(data) {
  const matches = data.matches || [];
  const pairs = data.pairs || [];
  const completed = matches.filter(m => m.status === "completed").length;

  document.getElementById("stats").innerHTML = `
    <div class="card">
      <h2>Estadísticas</h2>
      <p><strong>Parejas inscritas:</strong> ${pairs.length}</p>
      <p><strong>Partidos jugados:</strong> ${completed}</p>
      <p><strong>Partidos pendientes:</strong> ${matches.length - completed}</p>
      <p class="meta">Más estadísticas próximamente.</p>
    </div>
  `;
}
