# -*- coding: utf-8 -*-
# ../app/views/page_live.py
# HaïtiMétéo+ — Page Météo en direct (Premium • Stable • Polars)

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import polars as pl
import pydeck as pdk

from modules.storage import save_weather, read_villes
from modules.meteo import get_live_weather


# ----------------------------
# Codes météo + description
# ----------------------------
WEATHER_DESC = {
    0:  ("☀️", "Ciel dégagé"),
    1:  ("🌤️", "Principalement clair"),
    2:  ("⛅", "Partiellement nuageux"),
    3:  ("☁️", "Couvert"),
    45: ("🌫️", "Brouillard"),
    48: ("🌫️", "Brouillard givrant"),
    51: ("🌦️", "Bruine légère"),
    53: ("🌦️", "Bruine modérée"),
    55: ("🌧️", "Bruine forte"),
    61: ("🌧️", "Pluie légère"),
    63: ("🌧️", "Pluie modérée"),
    65: ("🌧️", "Pluie forte"),
    71: ("❄️", "Neige légère"),
    73: ("❄️", "Neige modérée"),
    75: ("❄️", "Neige forte"),
    95: ("⛈️", "Orage"),
    96: ("⛈️", "Orage + grêle légère"),
    99: ("⛈️", "Orage + grêle forte"),
}


def safe_float(x):
    """Convertit proprement en float ou renvoie None."""
    try:
        return float(x) if x is not None else None
    except:
        return None


def render():
    st.title("🌤️ Météo en direct – HaïtiMétéo+")
    st.write("Conditions météo actuelles, alertes officielles, localisation et enregistrement automatique.")

    st.markdown("---")

    # ----------------------------
    # Charger les villes
    # ----------------------------
    villes = read_villes()
    choice = st.selectbox("Ville :", villes["ville"].to_list())

    row = villes.filter(pl.col("ville") == choice).row(0)
    lat = float(row[2])
    lon = float(row[3])

    st.markdown("---")

    # ----------------------------
    # Bouton mise à jour
    # ----------------------------
    if not st.button("🔄 Actualiser maintenant"):
        st.info("Cliquez sur le bouton pour récupérer la météo en direct.")
        return

    with st.spinner("Connexion à Open-Meteo..."):
        try:
            data = get_live_weather(lat, lon)
            current = data.get("current", {})
        except Exception as e:
            st.error(f"❌ Erreur API : {e}")
            return

    # ----------------------------
    # Extraction sécurisée
    # ----------------------------
    temp = safe_float(current.get("temperature_2m"))
    hum = safe_float(current.get("relative_humidity_2m"))
    rain = safe_float(current.get("precipitation"))
    wind = safe_float(current.get("wind_speed_10m"))
    code = int(current.get("weather_code", 0))

    icon, label = WEATHER_DESC.get(code, ("🌡️", "Condition inconnue"))

    # ----------------------------
    # Enregistrer l’observation
    # ----------------------------
    try:
        save_weather(choice, temp or 0, rain or 0, wind or 0)
        st.success("Observations enregistrées dans l’historique ✔")
    except Exception as e:
        st.warning(f"⚠️ Erreur lors de l’enregistrement : {e}")

    st.markdown("---")

    # ----------------------------
    # Carte
    # ----------------------------
    st.subheader("📍 Position")

    df_map = pl.DataFrame({"lat": [lat], "lon": [lon]}).to_pandas()

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_map,
        get_position="[lon, lat]",
        get_radius=50000,
        get_color=[0, 122, 255, 200],
        pickable=True,
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=7),
            map_style="light",
        )
    )

    st.markdown("---")

    # ----------------------------
    # Conditions actuelles
    # ----------------------------
    st.subheader(f"🌡️ Conditions actuelles — {choice}")

    colA, colB = st.columns([1, 2])

    with colA:
        st.markdown(
            f"""
            <div style="font-size:80px;text-align:center">{icon}</div>
            <h2 style="text-align:center;margin-top:-10px">{label}</h2>
            """,
            unsafe_allow_html=True,
        )

    with colB:
        st.metric("Température", f"{temp:.1f} °C" if temp is not None else "—")
        st.metric("Humidité", f"{hum:.0f} %" if hum is not None else "—")
        st.metric("Précipitations", f"{rain:.1f} mm" if rain is not None else "—")
        st.metric("Vent", f"{wind:.1f} km/h" if wind is not None else "—")

    st.markdown("---")

    # ----------------------------
    # Alertes météo
    # ----------------------------
    st.subheader("🚨 Alertes météo")

    alerts = data.get("alerts", {})

    if alerts and alerts.get("alert"):
        for a in alerts["alert"]:
            with st.expander(f"⚠️ {a.get('event', 'Alerte')}"):
                st.write(f"**Début :** {a.get('onset', '—')}")
                st.write(f"**Fin :** {a.get('ends', '—')}")
                st.write(f"**Niveau :** {a.get('severity', '—')}")
                st.write(f"**Description :** {a.get('description', '—')}")
    else:
        st.success("Aucune alerte active 👍")

    st.markdown("---")

    # ----------------------------
    # Debug
    # ----------------------------
    with st.expander("🔍 Données brutes API"):
        st.json(data)
