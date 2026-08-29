"""Generate docs/demo.gif from pipeline data in Cassandra (host :9042).

Usage:
    # default: newest contiguous 12-minute window
    python scripts/make_demo_gif.py

    # explicit UTC window (useful to pick one where vehicles are driving)
    WINDOW_START="2026-08-25 16:19" WINDOW_END="2026-08-25 16:31" python scripts/make_demo_gif.py

Deps: pip install matplotlib pillow cassandra-driver
"""
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

VEHICLES = ["veh_0001", "veh_0002"]
COLORS = {"veh_0001": "#58a6ff", "veh_0002": "#f78166"}
MINUTES = 12
FRAMES = 72
FPS = 12

BG, PANEL, GRID, TEXT, MUTED = "#0e1116", "#161b22", "#2a313b", "#e6edf3", "#8b949e"

cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
session = cluster.connect()

data = defaultdict(list)  # vehicle -> [(ts, lat, lon, speed)]
t1 = None
WIN = (os.environ.get("WINDOW_START"), os.environ.get("WINDOW_END"))
for v in VEHICLES:
    if all(WIN):
        q = (f"SELECT event_time, lat, lon, speed_kmh FROM fleet_telemetry.telemetry_by_vehicle_time "
             f"WHERE vehicle_id='{v}' AND event_time >= '{WIN[0].replace(' ', 'T')}:00+0000' "
             f"AND event_time < '{WIN[1].replace(' ', 'T')}:00+0000'")
    else:
        q = (f"SELECT event_time, lat, lon, speed_kmh FROM fleet_telemetry.telemetry_by_vehicle_time "
             f"WHERE vehicle_id='{v}' LIMIT {MINUTES * 120}")
    rows = session.execute(SimpleStatement(q), timeout=30)
    pts = [(r.event_time, r.lat, r.lon, float(r.speed_kmh)) for r in rows]
    pts.sort(key=lambda p: p[0])
    if pts:
        t1 = max(t1 or pts[-1][0], pts[-1][0])
    data[v] = pts
    print(v, len(pts), "points", pts[0][0], "->", pts[-1][0])
cluster.shutdown()

# Keep only the contiguous window right before the newest sample, so the
# animation never spans pipeline downtime gaps.
if not all(WIN):
    t1 = t1.replace(tzinfo=timezone.utc)
    t0 = t1 - timedelta(minutes=MINUTES)
    for v in VEHICLES:
        data[v] = [p for p in data[v] if t0 <= p[0].replace(tzinfo=timezone.utc) <= t1]
        print(v, "in window:", len(data[v]))
        assert len(data[v]) > FRAMES * 3, f"not enough fresh data for {v}; is the stack running?"

if os.environ.get("WINDOW_START"):  # explicit demo window (UTC, "YYYY-MM-DD HH:MM")
    from datetime import datetime as _dt
    w0 = _dt.strptime(os.environ["WINDOW_START"], "%Y-%m-%d %H:%M")
    w1 = _dt.strptime(os.environ["WINDOW_END"], "%Y-%m-%d %H:%M")
    for v in VEHICLES:
        data[v] = [p for p in data[v] if w0 <= p[0] <= w1]
        assert len(data[v]) > FRAMES * 3, f"window too sparse for {v}"
    t0, t1 = w0, w1
else:
    t0 = min(data[v][0][0] for v in VEHICLES if data[v])
    t1 = max(data[v][-1][0] for v in VEHICLES if data[v])
bounds = {v: (min(p[1] for p in data[v]), max(p[1] for p in data[v]),
              min(p[2] for p in data[v]), max(p[2] for p in data[v])) for v in VEHICLES}

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": PANEL, "savefig.facecolor": BG,
    "text.color": TEXT, "axes.edgecolor": GRID, "axes.labelcolor": MUTED,
    "xtick.color": MUTED, "ytick.color": MUTED, "font.size": 10,
})

fig = plt.figure(figsize=(11, 5.6), dpi=90)
ax_maps = [fig.add_axes([0.05, 0.10, 0.28, 0.76]),
           fig.add_axes([0.36, 0.10, 0.28, 0.76])]
ax_spd = fig.add_axes([0.70, 0.15, 0.26, 0.70])
for ax, v in zip(ax_maps, VEHICLES):
    ax.set_title(f"{v} — live map", color=COLORS[v], fontsize=11, loc="left", pad=8)
ax_spd.set_title("Speed (km/h)", color=TEXT, fontsize=12, loc="left", pad=10)

def draw(frame):
    frac = (frame + 1) / FRAMES
    cutoff = t0 + (t1 - t0) * frac
    for ax in ax_maps:
        ax.clear()
        ax.set_facecolor(PANEL)
        ax.set_xticks([])
        ax.set_yticks([])
    ax_spd.clear()
    ax_spd.set_facecolor(PANEL)
    ax_spd.grid(color=GRID, lw=0.5)
    ymin = min(p[3] for v in VEHICLES for p in data[v]) * 0.9
    ymax = max(p[3] for v in VEHICLES for p in data[v]) * 1.1
    ax_spd.set_ylim(ymin, ymax)
    ax_spd.set_xlim(t0, t1)
    ax_spd.set_xticks([t0 + (t1 - t0) * f for f in (0, 0.5, 1.0)])
    ax_spd.set_xticklabels([f"{t:%H:%M}" for t in [t0, t0 + (t1 - t0) / 2, t1]])
    ax_spd.yaxis.tick_right()
    ax_spd.yaxis.set_label_position("right")
    ax_spd.set_ylabel("km/h")

    for v, ax in zip(VEHICLES, ax_maps):
        c = COLORS[v]
        pts = data[v]
        past = [p for p in pts if p[0] <= cutoff]
        if not past:
            continue
        lons = [p[2] for p in pts]
        lats = [p[1] for p in pts]
        span = 0.02  # ~2 km view that follows the vehicle, nav-style
        ax.plot(lons, lats, color=c, lw=1, alpha=0.2, zorder=1)
        ax.plot([p[2] for p in past], [p[1] for p in past], color=c, lw=2.4, zorder=2)
        cur = past[-1]
        ax.set_xlim(cur[2] - span, cur[2] + span)
        ax.set_ylim(cur[1] - span, cur[1] + span)
        ax.scatter([cur[2]], [cur[1]], s=150, color=c, edgecolors="white",
                   linewidths=1.8, zorder=3)
        ax.annotate(f"{cur[3]:.0f} km/h", (cur[2], cur[1]), textcoords="offset points",
                    xytext=(10, -4), color=TEXT, fontsize=9, fontweight="bold")
        ax_spd.plot([p[0] for p in past], [p[3] for p in past], color=c, lw=1.6, label=v)
        ax_spd.annotate(f"{cur[3]:.0f}", (cur[0], cur[3]), color=c,
                        textcoords="offset points", xytext=(4, 2), fontsize=9)
    ax_spd.legend(loc="upper left", framealpha=0, fontsize=8,
                  labelcolor=MUTED)

    fig.text(0.05, 0.945, "IoT fleet telemetry — Kafka -> Beam -> Cassandra/Redis",
             color=TEXT, fontsize=13, fontweight="bold")
    fig.text(0.05, 0.915, f"recorded live · {cutoff:%Y-%m-%d %H:%M:%SZ}",
             color=MUTED, fontsize=9)

anim = FuncAnimation(fig, draw, frames=FRAMES, interval=1000 / FPS)
anim.save(Path(__file__).resolve().parent.parent / "docs" / "demo.gif", writer=PillowWriter(fps=FPS))
print("saved docs/demo.gif")
