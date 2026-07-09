import { renderHeader } from "./header.js";
import { renderSummary } from "./summary.js";
import { renderStandings } from "./standings.js";
import { renderMatches } from "./matches.js";
import { renderParticipants } from "./participants.js";
import { renderStatistics } from "./statistics.js";
import { showTab } from "./tabs.js";
import { initTabs } from "./tabs.js";

const API_URL = "https://puntorank-backend.onrender.com";


const params = new URLSearchParams(window.location.search);
const leagueId = params.get("id");

loadLeague();

async function loadLeague() {
  const response = await fetch(`${API_URL}/public/leagues/${leagueId}`);
  const data = await response.json();

  if (!response.ok) {
    document.getElementById("leagueHeader").innerHTML =
      `<h2>No se pudo cargar la liga</h2>`;
    return;
  }

  renderHeader(data.league);
  renderSummary(data);
  renderStandings(data.standings);
  renderMatches(data.matches);
  renderParticipants(data.pairs);
  renderStatistics(data);
  
  initTabs();
}

