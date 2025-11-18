# python
# -*- coding: utf-8 -*-
# HaïtiMétéo+ — Carte météorologique premium (version Polars FIXED & STABLE)

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import logging
import concurrent.futures
import numpy as np
import pandas as pd
import polars as pl
import streamlit as st
import pydeck as pdk

from modules.storage import read_villes
from modules.meteo import get_live_weather

logging.basicConfig(level=logging.INFO)


WEATHER_ICONS = {
    0: "☀️", 1: "🌤️", 2: "⛅", 3: "☁️",
    45: "🌫️", 48: "🌫️",
    51: "🌦️", 53: "🌦️", 55: "🌧️",
    61: "🌧️", 63: "🌧️", 65: "🌧️",
    71: "❄️", 73: "❄️", 75: "❄️",
    95: "⛈️", 96: "⛈️", 99: "⛈️",
}


def render():
    st.title("Carte météorologique – HaïtiMétéo+")
    st.write("Visualisation avancée : heatmap, vent, icônes météo, températures et styles personnalisés.")
    st.markdown("---")

    style_choice = st.radio(
        "Style de carte :",
        ["Clair", "Sombre", "Satellite"],
        horizontal=True
    )

    MAP_STYLES = {
        "Clair": "light",
        "Sombre": "dark",
        "Satellite": "satellite"
    }

    map_style = MAP_STYLES[style_choice]

    villes = read_villes()
    if villes.is_empty():
        st.error("Aucune ville n’a été trouvée.")
        return

    st.info("📡 Récupération de la météo en temps réel…")

    # Collecter les villes en liste pour itération parallèle
    villes_list = list(villes.iter_rows(named=True))

    rows = []

    def fetch_for_ville(v):
        try:
            data = get_live_weather(v["latitude"], v["longitude"])
            cur = data.get("current", {}) or {}
            return {
                "ville": v["ville"],
                "lat": float(v["latitude"]),
                "lon": float(v["longitude"]),
                "temp": _safe_float(cur.get("temperature_2m")),
                "hum": _safe_float(cur.get("relative_humidity_2m")),
                "precip": _safe_float(cur.get("precipitation")),
                "vent": _safe_float(cur.get("wind_speed_10m")),
                "wcode": int(cur.get("weather_code", 0) or 0),
                "icon": str(WEATHER_ICONS.get(cur.get("weather_code", 0), "🌡️")),
            }
        except Exception as e:
            logging.exception("Erreur récupération météo pour %s", v.get("ville"))
            # Ne pas masquer l'erreur : on retourne un état identifiable
            return {
                "ville": v["ville"],
                "lat": float(v["latitude"]),
                "lon": float(v["longitude"]),
                "temp": None,
                "hum": None,
                "precip": 0.0,
                "vent": 0.0,
                "wcode": 0,
                "icon": "⚠️",
                "error": str(e)
            }

    # Taille du pool adaptative (limite raisonnable)
    max_workers = min(20, max(2, len(villes_list)))
    with st.spinner("Récupération parallèle des données météo..."):
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(fetch_for_ville, v): v for v in villes_list}
            for fut in concurrent.futures.as_completed(futures):
                res = fut.result()
                if res.get("error"):
                    st.warning(f"Erreur pour {res['ville']}: {res['error']}")
                rows.append(res)

    # Utiliser pandas directement (on convertit ensuite en polars si besoin)
    df_pd = pd.DataFrame(rows)

    # Nettoyage ciblé
    df_pd["icon"] = df_pd["icon"].fillna("⚠️").astype(str)

    df_pd["precip"] = pd.to_numeric(df_pd["precip"], errors="coerce").fillna(0.0)
    df_pd["vent"] = pd.to_numeric(df_pd["vent"], errors="coerce").fillna(0.0)

    # temp/hum : garder NaN si absent (ne pas remplacer par 0)
    df_pd["temp"] = pd.to_numeric(df_pd["temp"], errors="coerce")
    df_pd["hum"] = pd.to_numeric(df_pd["hum"], errors="coerce")

    # wcode : int sûr (remplacer NaN par 0)
    df_pd["wcode"] = pd.to_numeric(df_pd.get("wcode", 0), errors="coerce").fillna(0).astype(int)

    df_pd["lat"] = df_pd["lat"].astype(float)
    df_pd["lon"] = df_pd["lon"].astype(float)

    # Génération vectorisée des couleurs (numpy) : évite map_elements Polars
    temps = df_pd["temp"]
    t_clamped = temps.clip(lower=-5, upper=40)
    r = np.rint(((t_clamped + 5) / 45.0) * 255).astype("int32")
    r = np.where(temps.isna(), 150, r)
    g = np.full(len(df_pd), 90, dtype="int32")
    g = np.where(temps.isna(), 150, g)
    b = 255 - r
    b = np.where(temps.isna(), 150, b)
    a = np.full(len(df_pd), 220, dtype="int32")
    a = np.where(temps.isna(), 180, a)
    df_pd["color"] = np.stack([r, g, b, a], axis=1).tolist()

    # Layers PyDeck
    layer_temp = pdk.Layer(
        "ScatterplotLayer",
        data=df_pd,
        get_position="[lon, lat]",
        get_color="color",
        get_radius=42000,
        pickable=True
    )

    layer_heat = pdk.Layer(
        "HeatmapLayer",
        data=df_pd,
        get_position="[lon, lat]",
        get_weight="precip",
        radiusPixels=60,
        threshold=0.2
    )

    text_layer = pdk.Layer(
        "TextLayer",
        data=df_pd,
        get_position="[lon, lat]",
        get_text="icon",
        get_size=28,
        get_color=[255, 255, 255],
        pickable=True
    )

    wind_layer = pdk.Layer(
        "ArrowLayer",
        data=df_pd,
        get_position="[lon, lat]",
        get_direction="[1, 0, 0]",
        get_length="vent / 3",
        get_color=[255, 255, 0],
        pickable=False
    )

    view_state = pdk.ViewState(
        latitude=float(df_pd["lat"].mean()),
        longitude=float(df_pd["lon"].mean()),
        zoom=7
    )

    st.pydeck_chart(
        pdk.Deck(
            map_style=map_style,
            initial_view_state=view_state,
            layers=[layer_heat, layer_temp, text_layer, wind_layer],
            tooltip={
                "html": "<b>{ville}</b><br>"
                        "🌡 Température : {temp} °C<br>"
                        "💧 Humidité : {hum}%<br>"
                        "🌧 Pluie : {precip} mm<br>"
                        "💨 Vent : {vent} km/h<br>"
                        "Code météo : {wcode}",
                "style": {"color": "white"}
            }
        )
    )

    st.markdown("---")
    st.subheader("Tableau récapitulatif (Live)")
    st.dataframe(
        df_pd[["ville", "temp", "hum", "precip", "vent", "wcode"]],
        use_container_width=True
    )


def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None
