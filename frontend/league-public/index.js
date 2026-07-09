import { renderHeader } from "./header.js";
import { renderSummary } from "./summary.js";
import { renderStandings } from "./standings.js";
import { renderMatches } from "./matches.js";
import { renderParticipants } from "./participants.js";
import { renderStatistics } from "./statistics.js";

const API_URL = "https://puntorank-backend.onrender.com";

const params = new URLSearchParams(window.location.search);
const leagueId = params.get("id");

loadLeague();

async function loadLeague() {
  try {
    const response = await fetch(`${API_URL}/public/leagues/${leagueId}`);
    const data = await response.json();

    if (!response.ok) {
      document.getElementById("leagueHeader").innerHTML =
        `<h2>No se pudo cargar la liga</h2>`;
      return;
    }

    renderHeader(data.league);

    try { renderSummary(data); } 
    catch (e) { console.error("Error summary", e); }

    try { renderStandings(data.standings || []); } 
    catch (e) { console.error("Error standings", e); }

    try { renderMatches(data.matches || []); } 
    catch (e) { console.error("Error matches", e); }

    try { renderParticipants(data.pairs || []); } 
    catch (e) { console.error("Error participants", e); }

    try { renderStatistics(data); } 
    catch (e) { console.error("Error statistics", e); }

  } catch (error) {
    console.error(error);
    document.getElementById("leagueHeader").innerHTML =
      `<h2>Error cargando la liga</h2>`;
  }
}
