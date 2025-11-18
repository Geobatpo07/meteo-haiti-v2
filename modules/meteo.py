# -*- coding: utf-8 -*-
# ../modules/meteo.py

import requests
import polars as pl


# =========================================================
# API ENDPOINTS
# =========================================================

LIVE_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


# =========================================================
# MÉTÉO LIVE BASIQUE
# =========================================================

def get_weather(lat: float, lon: float):
    """
    Récupère météo live horaire :
      - température
      - précipitation
      - vent
    Retour brut JSON (pour usage simple)
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": "America/Port-au-Prince",
    }

    try:
        r = requests.get(LIVE_URL, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERREUR] get_weather({lat}, {lon}) → {e}")
        return None


# =========================================================
# MÉTÉO LIVE LÉGÈRE (pour carte : température + weather_code)
# =========================================================

def get_city_current(lat: float, lon: float):
    """
    Retour ultra-léger pour la carte :
      - temp
      - weather_code
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,weather_code",
        "timezone": "auto",
    }

    try:
        r = requests.get(LIVE_URL, params=params, timeout=12)
        r.raise_for_status()
        return r.json().get("current", {})
    except Exception as e:
        print(f"[ERREUR] get_city_current({lat}, {lon}) → {e}")
        return None


# =========================================================
# MÉTÉO LIVE PREMIUM (alertes + humidité + vent)
# =========================================================

def get_live_weather(lat: float, lon: float):
    """
    Appel complet :
      - temp, humidité, pluie
      - vent
      - weather_code
      - alertes météo
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation,"
            "wind_speed_10m,"
            "weather_code"
        ),
        "timezone": "auto",
        "alerts": "true",
    }

    try:
        r = requests.get(LIVE_URL, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERREUR] get_live_weather({lat}, {lon}) → {e}")
        return None


# =========================================================
# ARCHIVE HISTORIQUE — VERSION POLARS (ultra rapide)
# =========================================================

def get_meteo_data(id_ville: int, lat: float, lon: float, year: int) -> pl.DataFrame | None:
    """
    Récupère les données climatiques ANNUELLES (archives Open-Meteo).
    Retour : Polars DataFrame
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "daily": (
            "temperature_2m_max,"
            "temperature_2m_min,"
            "precipitation_sum,"
            "relative_humidity_2m_mean,"
            "windspeed_10m_max"
        ),
        "timezone": "auto",
    }

    try:
        r = requests.get(ARCHIVE_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[ERREUR] Ville={id_ville}, année={year} → {e}")
        return None

    if "daily" not in data or data["daily"] is None:
        print(f"[INFO] Pas de données pour ville={id_ville} année={year}")
        return None

    daily = data["daily"]

    # Construction Polars (beaucoup plus performant que Pandas)
    return pl.DataFrame({
        "date": daily["time"],
        "temp_min": daily["temperature_2m_min"],
        "temp_max": daily["temperature_2m_max"],
        "humidite": daily["relative_humidity_2m_mean"],
        "precipitation": daily["precipitation_sum"],
        "vent": daily["windspeed_10m_max"],
    }).with_columns(
        pl.lit(id_ville).alias("id_ville")
    )

import requests

def get_historical_weather(lat, lon, start, end):
    """
    Télécharge l'historique météo entre start et end via Open-Meteo.
    start, end : 'YYYY-MM-DD'
    """

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "wind_speed_10m_max",
            "relative_humidity_2m_max"
        ],
        "timezone": "America/Port-au-Prince"
    }

    r = requests.get(LIVE_URL, params=params)
    r.raise_for_status()
    return r.json()
