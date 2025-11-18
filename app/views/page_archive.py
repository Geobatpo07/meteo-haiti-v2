# -*- coding: utf-8 -*-
# ../app/views/page_archive.py
# HaïtiMétéo+ — Page Archives (version Polars ultra-optimisée)

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import streamlit as st
import polars as pl
import sqlite3
from datetime import date
import math
from modules.storage import read_villes

# python
def _pl_from_cursor(cursor):
    """
    Convertit le résultat d'un curseur sqlite3 en Polars DataFrame.
    Gère le cas sans lignes en retournant un DataFrame vide avec les colonnes.
    """
    cols = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    if not rows:
        return pl.DataFrame({c: [] for c in cols})
    # Construire une liste de dicts pour garantir les noms de colonnes
    dicts = [dict(zip(cols, row)) for row in rows]
    return pl.DataFrame(dicts)

@st.cache_data(ttl=600)
def get_date_bounds(ville_id: int):
    with sqlite3.connect("data/meteo_haiti.sqlite") as conn:
        cursor = conn.execute(
            """
            SELECT MIN(date) AS min_d, MAX(date) AS max_d
            FROM meteo_archive
            WHERE id_ville = ?
            """,
            (ville_id,),
        )
        return _pl_from_cursor(cursor)

@st.cache_data(ttl=600)
def load_archive(ville_id: int, start_str: str, end_str: str):
    with sqlite3.connect("data/meteo_haiti.sqlite") as conn:
        cursor = conn.execute(
            """
            SELECT date, temp_min, temp_max, humidite, precipitation, vent
            FROM meteo_archive
            WHERE id_ville = ?
              AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (ville_id, start_str, end_str),
        )
        return _pl_from_cursor(cursor)

def _to_date(val):
    if val is None:
        return None
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return date.fromisoformat(val)
        except Exception:
            return None
    return None

def _fmt_metric(v, unit):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "N/A"
    return f"{v:.1f} {unit}"

def render():
    st.title("Archives météorologiques – HaïtiMété+")

    st.write("""
Analysez les données climatiques historiques sur toute Haïti.
Sélectionnez une ville et choisissez librement votre plage temporelle.
""")

    st.markdown("---")

    # ------------------------------------------------------
    # Sélection de la ville
    # ------------------------------------------------------
    villes = read_villes()  # Polars
    if villes.is_empty():
        st.error("Aucune ville disponible. Vérifiez la source `read_villes`.")
        return

    villes_list = villes["ville"].to_list()
    ville_choice = st.selectbox("Ville :", villes_list)
    # sécuriser la récupération de l'id
    sel = villes.filter(pl.col("ville") == ville_choice)
    if sel.is_empty():
        st.error("Ville sélectionnée introuvable.")
        return
    ville_id = int(sel["id"][0])

    # ------------------------------------------------------
    # Récupération bornes MIN/MAX via cursor SQL (avec cache)
    # ------------------------------------------------------
    bounds = get_date_bounds(ville_id)
    if bounds.is_empty():
        st.warning("Aucune donnée historique disponible pour cette ville.")
        return

    min_date_db = bounds["min_d"][0]
    max_date_db = bounds["max_d"][0]

    default_start = _to_date(min_date_db)
    default_end = _to_date(max_date_db)

    if default_start is None or default_end is None:
        st.warning("Les bornes de dates sont invalides dans la base.")
        return

    col1, col2 = st.columns(2)
    start_date = col1.date_input(
        "Date de début",
        value=default_start,
        min_value=default_start,
        max_value=default_end,
    )
    end_date = col2.date_input(
        "Date de fin",
        value=default_end,
        min_value=default_start,
        max_value=default_end,
    )

    if start_date > end_date:
        st.error("❌ La date de début doit être antérieure à la date de fin.")
        return

    st.markdown("---")

    # ------------------------------------------------------
    # Chargement optimisé via cursor + Polars (cached)
    # ------------------------------------------------------
    df = load_archive(ville_id, str(start_date), str(end_date))
    if df.is_empty():
        st.warning("Aucune donnée disponible pour cette période.")
        return

    # Normaliser la date
    df = df.with_columns(pl.col("date").str.to_date().alias("date"))

    # Coercion des colonnes numériques
    numeric_cols = ["temp_min", "temp_max", "humidite", "precipitation", "vent"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.with_columns(pl.col(c).cast(pl.Float64))

    # ------------------------------------------------------
    # Visualisations
    # ------------------------------------------------------
    st.subheader("Évolution des températures")
    st.line_chart(
        df.select(["date", "temp_min", "temp_max"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("Précipitations")
    st.area_chart(
        df.select(["date", "precipitation"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("Humidité")
    st.line_chart(
        df.select(["date", "humidite"])
          .to_pandas()
          .set_index("date")
    )

    st.subheader("Vent")
    st.line_chart(
        df.select(["date", "vent"])
          .to_pandas()
          .set_index("date")
    )

    st.markdown("---")

    # ------------------------------------------------------
    # Tableau
    # ------------------------------------------------------
    st.subheader("Tableau complet")
    st.dataframe(df.to_pandas(), use_container_width=True)

    # ------------------------------------------------------
    # Statistiques (robustes)
    # ------------------------------------------------------
    stats = df.select([
        pl.col("temp_min").mean().alias("min_avg"),
        pl.col("temp_max").mean().alias("max_avg"),
        pl.col("humidite").mean().alias("hum_avg"),
        pl.col("vent").mean().alias("vent_avg"),
    ]).to_dicts()[0]

    st.subheader("Statistiques rapides")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Temp. min (moy.)", _fmt_metric(stats.get("min_avg"), "°C"))
    col2.metric("Temp. max (moy.)", _fmt_metric(stats.get("max_avg"), "°C"))
    col3.metric("Humidité (moy.)", _fmt_metric(stats.get("hum_avg"), "%"))
    col4.metric("Vent (moy.)", _fmt_metric(stats.get("vent_avg"), "km/h"))
