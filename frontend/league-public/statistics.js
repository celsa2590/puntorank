export function renderStatistics(data) {
  const matches = data.matches || [];
  const pairs = data.pairs || [];

  const completed = matches.filter(m => m.status === "completed").length;
  const pending = matches.length - completed;

  const groups = new Set(pairs.map(p => p.group_name || "Grupo único"));

  const completion = matches.length > 0
    ? Math.round((completed / matches.length) * 100)
    : 0;

  const matchesWithScore = matches.filter(m => m.score);

  document.getElementById("stats").innerHTML = `
    <div class="grid">
      <div class="card">
        <h2>${pairs.length}</h2>
        <div class="meta">Parejas inscritas</div>
      </div>

      <div class="card">
        <h2>${groups.size}</h2>
        <div class="meta">Grupos</div>
      </div>

      <div class="card">
        <h2>${completed}</h2>
        <div class="meta">Partidos jugados</div>
      </div>

      <div class="card">
        <h2>${pending}</h2>
        <div class="meta">Partidos pendientes</div>
      </div>

      <div class="card">
        <h2>${completion}%</h2>
        <div class="meta">Avance de la liga</div>
      </div>

      <div class="card">
        <h2>${matchesWithScore.length}</h2>
        <div class="meta">Resultados cargados</div>
      </div>
    </div>

    <div class="card">
      <h2>Próximamente</h2>
      <p class="meta">
        Aquí podremos mostrar rachas, games ganados, sets ganados, partidos más disputados
        y estadísticas destacadas de la liga.
      </p>
    </div>
  `;
}
