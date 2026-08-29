/* Fleet live dashboard: consumes WebSocket snapshots and renders map + charts. */
"use strict";

const MAX_POINTS = 240; // ~4 min of 1 Hz ticks per vehicle
const COLORS = ["#58a6ff", "#f78166", "#3fb950", "#bc8cff", "#e3b341"];

const socket = new WebSocket(
  `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
);
socket.onopen = () => setConn("on", "live");
socket.onclose = () => setConn("off", "disconnected — retrying…");
socket.onerror = () => setConn("off", "connection error");
socket.onmessage = (ev) => handleSnapshot(JSON.parse(ev.data));

function setConn(state, label) {
  const el = document.getElementById("conn");
  el.className = `conn ${state}`;
  el.textContent = label;
}

// ---- Map ------------------------------------------------------------------
const map = L.map("map", { attributionControl: false });
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 18,
}).addTo(map);
map.setView([-6.55, 107.1], 8);

const markers = {};
let boundsFitted = false;

// ---- Charts ---------------------------------------------------------------
function makeChart(canvasId, unit) {
  return new Chart(document.getElementById(canvasId), {
    type: "line",
    data: { labels: [], datasets: [] },
    options: {
      animation: false,
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "nearest", intersect: false },
      parsing: false,
      scales: {
        x: {
          type: "linear",
          ticks: { color: "#8b949e", maxTicksLimit: 6, callback: (v) => shortTime(v) },
          grid: { color: "#2a313b" },
        },
        y: {
          beginAtZero: unit !== "°C",
          ticks: { color: "#8b949e" },
          grid: { color: "#2a313b" },
        },
      },
      plugins: {
        legend: { labels: { color: "#e6edf3", boxWidth: 10 } },
        elements: { point: { radius: 0 }, line: { tension: 0.25 } },
      },
    },
  });
}

const speedChart = makeChart("speedChart", "km/h");
const tempChart = makeChart("tempChart", "°C");

const history = {}; // id -> {t:[], speed:[], temp:[]}
const lastUpdate = {};

// Stable per-vehicle palette: a color is assigned on first sight and never
// changes, so markers, chips and chart lines always agree.
const palette = {};
let nextColor = 0;

function colorFor(id) {
  if (!(id in palette)) palette[id] = COLORS[nextColor++ % COLORS.length];
  return palette[id];
}

function shortTime(ms) {
  return new Date(ms).toLocaleTimeString([], {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function pushPoint(id, t, speed, temp) {
  if (!history[id]) history[id] = { t: [], speed: [], temp: [] };
  const h = history[id];
  h.t.push(t);
  h.speed.push(speed);
  h.temp.push(temp);
  if (h.t.length > MAX_POINTS) {
    h.t.shift();
    h.speed.shift();
    h.temp.shift();
  }
}

function refreshChart(chart, key, unitText) {
  const ids = Object.keys(history).sort();
  chart.data.datasets = ids.map((id) => ({
    label: id,
    data: history[id].t.map((t, i) => ({ x: t, y: history[id][key][i] })),
    borderColor: colorFor(id),
    backgroundColor: colorFor(id),
    borderWidth: 1.5,
  }));
  chart.update("none");
  void unitText;
}

// ---- Snapshot handling ----------------------------------------------------
function handleSnapshot(snap) {
  if (!snap || snap.type !== "snapshot") return;
  setConn("on", `live · ${snap.ts}`);
  const chips = document.getElementById("chips");
  const now = Date.parse(snap.ts);

  for (const v of snap.vehicles) {
    // Marker
    if (!markers[v.id]) {
      markers[v.id] = L.circleMarker([v.lat, v.lon], {
        radius: 9,
        weight: 2,
        color: "#fff",
        fillColor: colorFor(v.id),
        fillOpacity: 0.95,
      }).addTo(map).bindTooltip(v.id);
    }
    markers[v.id].setLatLng([v.lat, v.lon]);
    markers[v.id].setTooltipContent(
      `<b>${v.id}</b><br>${v.speed_kmh.toFixed(1)} km/h · ${v.engine_temp_c.toFixed(0)} °C`
    );

    // History
    pushPoint(v.id, now, v.speed_kmh, v.engine_temp_c);
    lastUpdate[v.id] = now;
  }

  if (!boundsFitted && snap.vehicles.length) {
    map.fitBounds(L.latLngBounds(snap.vehicles.map((v) => [v.lat, v.lon])).pad(0.4));
    boundsFitted = true;
  }

  // Chips
  chips.innerHTML = snap.vehicles
    .map((v) => {
      const c = colorFor(v.id);
      return `<span class="chip"><span class="dot" style="background:${c}"></span>` +
        `${v.id} <b>${v.speed_kmh.toFixed(0)}</b> km/h</span>`;
    })
    .join("");

  refreshChart(speedChart, "speed");
  refreshChart(tempChart, "temp");
}
