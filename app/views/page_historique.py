# -*- coding: utf-8 -*-
# ../app/views/page_historique.py
# HaïtiMétéo+ — Page Historique (7 jours → 30 jours)

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import polars as pl
import sqlite3
from datetime import date, timedelta
from modules.storage import read_villes


# -------------------------------------------
# Utilitaire : curseur SQLite → DataFrame Polars
# -------------------------------------------
def _pl_from_cursor(cursor):
    cols = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    dicts = [dict(zip(cols, r)) for r in rows]
    return pl.DataFrame(dicts)


# -------------------------------------------
# Chargement historique avec limite 30 jours
# -------------------------------------------
@st.cache_data(ttl=600)
def load_history(ville_id: int, start: str, end: str):
    with sqlite3.connect("data/meteo_haiti.sqlite") as conn:
        cursor = conn.execute(
            """
            SELECT date, temp_min, temp_max, humidite, precipitation, vent
            FROM meteo_archive
            WHERE id_ville = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (ville_id, start, end),
        )
        return _pl_from_cursor(cursor)


def render():
    st.title("Historique récent – HaïtiMétéo+")

    st.write("""
Consultez l’évolution de la météo récente pour une ville.
**Période maximale : 30 jours.**  
Une analyse simple, rapide et orientée “tendances”.
""")

    st.markdown("---")

    # -------------------------------------------
    # Sélection ville
    # -------------------------------------------
    villes = read_villes()
    if villes.is_empty():
        st.error("Aucune ville disponible.")
        return

    choices = villes["ville"].to_list()
    ville_choice = st.selectbox("Ville :", choices)

    row = villes.filter(pl.col("ville") == ville_choice)
    if row.is_empty():
        st.error("Ville introuvable.")
        return

    ville_id = int(row["id"][0])

    # -------------------------------------------
    # Sélection période (max 30 jours)
    # -------------------------------------------
    today = date.today()
    default_start = today - timedelta(days=7)
    min_start = today - timedelta(days=30)

    col1, col2 = st.columns(2)

    start_date = col1.date_input(
        "Date de début",
        value=default_start,
        min_value=min_start,
        max_value=today,
    )

    end_date = col2.date_input(
        "Date de fin",
        value=today,
        min_value=min_start,
        max_value=today,
    )

    if start_date > end_date:
        st.error("❌ La date de début doit être antérieure à la date de fin.")
        return

    if (end_date - start_date).days > 30:
        st.error("❌ La période ne peut pas dépasser 30 jours.")
        return

    st.markdown("---")

    # -------------------------------------------
    # Chargement données
    # -------------------------------------------
    df = load_history(ville_id, str(start_date), str(end_date))

    if df.is_empty():
        st.warning("Aucune donnée disponible pour cette période.")
        return

    # Convertir colonnes
    df = df.with_columns([
        pl.col("date").str.to_date(),
        pl.col("temp_min").cast(pl.Float64),
        pl.col("temp_max").cast(pl.Float64),
        pl.col("humidite").cast(pl.Float64),
        pl.col("precipitation").cast(pl.Float64),
        pl.col("vent").cast(pl.Float64),
    ])

    # -------------------------------------------
    # Graphiques
    # -------------------------------------------
    st.subheader("🌡️ Températures")
    st.line_chart(
        df.select(["date", "temp_min", "temp_max"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("🌧️ Précipitation")
    st.area_chart(
        df.select(["date", "precipitation"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("💧 Humidité")
    st.line_chart(
        df.select(["date", "humidite"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("💨 Vent")
    st.line_chart(
        df.select(["date", "vent"])
          .to_pandas()
          .set_index("date")
    )

    st.markdown("---")

    # -------------------------------------------
    # Tableau
    # -------------------------------------------
    st.subheader("📋 Données complètes")
    st.dataframe(df.to_pandas(), use_container_width=True)

    st.markdown("---")

    # -------------------------------------------
    # Statistiques récentes
    # -------------------------------------------
    stats = df.select([
        pl.col("temp_min").mean().alias("min_avg"),
        pl.col("temp_max").mean().alias("max_avg"),
        pl.col("humidite").mean().alias("hum_avg"),
        pl.col("vent").mean().alias("vent_avg"),
    ]).to_dicts()[0]

    st.subheader("📊 Statistiques (période sélectionnée)")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Temp. min (moy.)", f"{stats['min_avg']:.1f} °C")
    c2.metric("Temp. max (moy.)", f"{stats['max_avg']:.1f} °C")
    c3.metric("Humidité (moy.)", f"{stats['hum_avg']:.1f} %")
    c4.metric("Vent (moy.)", f"{stats['vent_avg']:.1f} km/h")
