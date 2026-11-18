"""Curated city coordinates monitored by SkyPulse."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MonitoredCity:
    """Metadata for a monitored global city."""

    city_id: str
    city_name: str
    country: str
    region: str
    latitude: float
    longitude: float
    timezone: str


MONITORED_CITIES: tuple[MonitoredCity, ...] = (
    MonitoredCity("tacna-pe", "Tacna", "Peru", "LATAM", -18.0146, -70.2536, "America/Lima"),
    MonitoredCity("lima-pe", "Lima", "Peru", "LATAM", -12.0464, -77.0428, "America/Lima"),
    MonitoredCity("bogota-co", "Bogota", "Colombia", "LATAM", 4.7110, -74.0721, "America/Bogota"),
    MonitoredCity(
        "santiago-cl", "Santiago", "Chile", "LATAM", -33.4489, -70.6693, "America/Santiago"
    ),
    MonitoredCity(
        "cdmx-mx", "Mexico City", "Mexico", "LATAM", 19.4326, -99.1332, "America/Mexico_City"
    ),
    MonitoredCity(
        "nyc-us",
        "New York",
        "United States",
        "North America",
        40.7128,
        -74.0060,
        "America/New_York",
    ),
    MonitoredCity("madrid-es", "Madrid", "Spain", "Europe", 40.4168, -3.7038, "Europe/Madrid"),
    MonitoredCity(
        "london-gb", "London", "United Kingdom", "Europe", 51.5074, -0.1278, "Europe/London"
    ),
    MonitoredCity("tokyo-jp", "Tokyo", "Japan", "Asia", 35.6762, 139.6503, "Asia/Tokyo"),
    MonitoredCity(
        "sydney-au", "Sydney", "Australia", "Oceania", -33.8688, 151.2093, "Australia/Sydney"
    ),
)
