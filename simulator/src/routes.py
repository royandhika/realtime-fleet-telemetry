"""Route catalog — real Indonesian corridors as polylines.

Each route is a sequence of `RouteNode`s with a lat/lon, road class, and
an explicit posted speed limit. The simulator interpolates between
consecutive nodes using great-circle distance, advancing the vehicle
along the polyline at its current speed.

Coordinates are approximate lat/lon of recognizable intersections so a
local reviewer can spot the corridor on a map. They are not survey-grade.
"""

from __future__ import annotations

from models import RoadClass, Route, RouteNode

# Speed limit helper — most nodes inherit from the road class, but
# intersections / bridge approaches get an explicit override.
def _n(
    lat: float,
    lon: float,
    road_class: RoadClass,
    speed_limit: int | None = None,
) -> RouteNode:
    """Build a route node, falling back to the road-class default limit."""
    return RouteNode(
        lat=lat,
        lon=lon,
        road_class=road_class,
        speed_limit_kmh=speed_limit if speed_limit is not None
        else _ROAD_CLASS_DEFAULT[road_class],
    )


_ROAD_CLASS_DEFAULT = {
    RoadClass.TOLL: 100,
    RoadClass.ARTERIAL: 60,
    RoadClass.LOCAL: 40,
    RoadClass.RESIDENTIAL: 20,
}


# ---------------------------------------------------------------------------
# Jakarta corridors
# ---------------------------------------------------------------------------

# Jl. Sudirman → Jl. Gajah Mada → Kota Tua (the CBD spine)
JKT_SUDIRMAN_KOTA = Route(
    route_id="jkt_sudirman_kota",
    name="Sudirman – Harmoni – Kota Tua",
    city="Jakarta",
    nodes=(
        _n(-6.2240, 106.8014, RoadClass.ARTERIAL),       # Senayan (GBK)
        _n(-6.2197, 106.8070, RoadClass.ARTERIAL),       # Senayan Plaza
        _n(-6.2135, 106.8050, RoadClass.ARTERIAL),        # Bendungan Hilir
        _n(-6.2085, 106.8165, RoadClass.ARTERIAL),       # Karet
        _n(-6.2018, 106.8192, RoadClass.ARTERIAL),       # Sudirman (Dukuh Atas)
        _n(-6.1982, 106.8324, RoadClass.ARTERIAL),       # Gondangdia
        _n(-6.1861, 106.8265, RoadClass.LOCAL, 40),      # Tanah Abang → Arteri
        _n(-6.1735, 106.8270, RoadClass.LOCAL, 40),      # Harmoni
        _n(-6.1667, 106.8238, RoadClass.LOCAL, 40),      # Glodok
        _n(-6.1351, 106.8132, RoadClass.RESIDENTIAL, 25),# Kota Tua
    ),
)

# Jl. Gatot Subroto → Kuningan → Senayan loop (a CBD ring)
JKT_GATOT_KUNINGAN = Route(
    route_id="jkt_gatot_kuningan",
    name="Gatot Subroto – Kuningan – Semanggi loop",
    city="Jakarta",
    nodes=(
        _n(-6.2240, 106.8014, RoadClass.ARTERIAL),       # Senayan
        _n(-6.2200, 106.7970, RoadClass.ARTERIAL),       # Ratu Plaza
        _n(-6.2105, 106.7960, RoadClass.ARTERIAL),       # Tegal Ropo
        _n(-6.2020, 106.8085, RoadClass.ARTERIAL),       # Slipi
        _n(-6.1925, 106.8100, RoadClass.LOCAL, 40),       # Kebon Jeruk
        _n(-6.2055, 106.8200, RoadClass.ARTERIAL),       # Gatot Subroto slip road
        _n(-6.2220, 106.8270, RoadClass.ARTERIAL),       # Kuningan junction
        _n(-6.2250, 106.8285, RoadClass.ARTERIAL),       # Semanggi
        _n(-6.2240, 106.8014, RoadClass.ARTERIAL),       # back to Senayan
    ),
)


# ---------------------------------------------------------------------------
# Bandung corridors
# ---------------------------------------------------------------------------

# Cihampelas → Dipatiukur → Wastukencana (city corridor; ends near Alun-Alun)
BDG_CIHAMPELAS_BRAGA = Route(
    route_id="bdg_cihampelas_braga",
    name="Cihampelas – Dipatiukur – Braga – Alun-Alun",
    city="Bandung",
    nodes=(
        _n(-6.8932, 107.6111, RoadClass.LOCAL, 40),       # Cihampelas Walk
        _n(-6.8917, 107.6120, RoadClass.LOCAL, 40),       # upper Cihampelas
        _n(-6.8905, 107.6130, RoadClass.LOCAL, 40),       # Cihampelas curve
        _n(-6.8875, 107.6142, RoadClass.LOCAL, 40),       # Dipatiukur (Dago mouth)
        _n(-6.9010, 107.6110, RoadClass.LOCAL, 35),       # Jl. Merdeka
        _n(-6.9180, 107.6109, RoadClass.RESIDENTIAL, 25),# Wastukencana
        _n(-6.9217, 107.6087, RoadClass.RESIDENTIAL, 25),# Asia Afrika
        _n(-6.9217, 107.6084, RoadClass.RESIDENTIAL, 25),# Braga
        _n(-6.9215, 107.6046, RoadClass.RESIDENTIAL, 25),# Alun-Alun
    ),
)

# Pasteur → Cileunyi toll onramp corridor (a bit faster, semi-arterial)
BDG_PASTEUR_CILEUNYI = Route(
    route_id="bdg_pasteur_cileunyi",
    name="Pasteur – Karang Setia – Cileunyi toll gate",
    city="Bandung",
    nodes=(
        _n(-6.8932, 107.6111, RoadClass.LOCAL, 40),       # Cihampelas start
        _n(-6.9184, 107.5930, RoadClass.ARTERIAL),        # Jl. Padjadjaran
        _n(-6.9253, 107.5783, RoadClass.ARTERIAL),        # Pasteur
        _n(-6.9300, 107.6085, RoadClass.ARTERIAL),        # Binuang
        _n(-6.9270, 107.6405, RoadClass.ARTERIAL),        # Soekarno Hatta
        _n(-6.9200, 107.6870, RoadClass.TOLL),            # Cileunyi toll gate
    ),
)


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

ROUTES: tuple[Route, ...] = (
    JKT_SUDIRMAN_KOTA,
    JKT_GATOT_KUNINGAN,
    BDG_CIHAMPELAS_BRAGA,
    BDG_PASTEUR_CILEUNYI,
)


def routes_for_city(city: str) -> tuple[Route, ...]:
    """Return routes whose city matches the given home city."""
    return tuple(r for r in ROUTES if r.city == city)


def route_by_id(route_id: str) -> Route | None:
    for r in ROUTES:
        if r.route_id == route_id:
            return r
    return None