# -*- coding: utf-8 -*-
import datetime
import sqlite3
import time
from pathlib import Path

import polars as pl
from modules.meteo import get_meteo_data
from modules.utils import load_yaml

DB_PATH = Path("data/meteo_haiti.sqlite")


# ---------------------------------------------------------
# CONNEXION
# ---------------------------------------------------------
def connect_db():
    return sqlite3.connect(DB_PATH)


# ---------------------------------------------------------
# INITIALISATION DES TABLES
# ---------------------------------------------------------
def init_db():
    conn = connect_db()
    cur = conn.cursor()

    # Table villes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS villes (
            id INTEGER PRIMARY KEY,
            nom TEXT,
            latitude REAL,
            longitude REAL
        );
    """)

    # Table météo archive
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meteo_archive (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            id_ville INTEGER,
            date TEXT,
            temp_min REAL,
            temp_max REAL,
            humidite REAL,
            precipitation REAL,
            vent REAL,
            FOREIGN KEY(id_ville) REFERENCES villes(id)
        );
    """)

    # Table météo live
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meteo_live (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ville TEXT,
            timestamp TEXT,
            temperature REAL,
            precipitation REAL,
            vent REAL
        );
    """)

    # Indexs pour accélérer Polars + SQLite
    cur.execute("CREATE INDEX IF NOT EXISTS idx_archive_ville_date ON meteo_archive (id_ville, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_archive_date ON meteo_archive (date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_archive_ville ON meteo_archive (id_ville);")

    conn.commit()
    conn.close()


# ---------------------------------------------------------
# SYNCHRONISATION YAML → TABLE VILLES
# ---------------------------------------------------------
def sync_villes_from_yaml():
    config = load_yaml("data/config.yaml")
    yaml_villes = config["villes"]

    conn = connect_db()
    cur = conn.cursor()

    # Récupérer les IDs déjà existants
    cur.execute("SELECT id FROM villes")
    existing_ids = {row[0] for row in cur.fetchall()}

    for v in yaml_villes:
        if v["id"] not in existing_ids:
            cur.execute("""
                INSERT INTO villes (id, nom, latitude, longitude)
                VALUES (?, ?, ?, ?)
            """, (v["id"], v["nom"], v["latitude"], v["longitude"]))

    conn.commit()
    conn.close()

    print("✔ Synchronisation de la table `villes` terminée.")


# ---------------------------------------------------------
# LECTURE DES VILLES (POLARS)
# ---------------------------------------------------------
def read_villes() -> pl.DataFrame:
    conn = connect_db()
    df = pl.read_database(
        "SELECT id, nom AS ville, latitude, longitude FROM villes",
        connection=conn
    )
    conn.close()
    return df


# ---------------------------------------------------------
# INSERTION ARCHIVE (Polars → SQLite)
# ---------------------------------------------------------
def insert_dataframe(table: str, df: pl.DataFrame):
    """
    Polars → SQLite (oblige conversion en pandas pour écrire).
    """
    conn = connect_db()
    df.to_pandas().to_sql(table, conn, if_exists="append", index=False)
    conn.close()


def insert_meteo_data(start_year: int, end_year: int, wait_seconds: float = 1.0):
    villes = read_villes()  # Polars

    for row in villes.iter_rows(named=True):
        ville_id = row["id"]
        ville_nom = row["ville"]
        lat = row["latitude"]
        lon = row["longitude"]

        for year in range(start_year, end_year + 1):
            print(f"⏳ Traitement : {ville_nom} - {year}")

            df = get_meteo_data(
                id_ville=ville_id,
                lat=lat,
                lon=lon,
                year=year
            )

            if df is not None:
                insert_dataframe("meteo_archive", df)

            time.sleep(wait_seconds)

    print("\n✔ Données climatiques insérées dans `meteo_archive`")


# ---------------------------------------------------------
# MÉTÉO LIVE (Streamlit)
# ---------------------------------------------------------
def save_weather(ville: str, temp: float, precip: float, wind: float):
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO meteo_live (ville, timestamp, temperature, precipitation, vent)
        VALUES (?, ?, ?, ?, ?)
    """, (ville, datetime.datetime.now().isoformat(), temp, precip, wind))

    conn.commit()
    conn.close()


def load_history(ville: str) -> pl.DataFrame:
    conn = connect_db()
    df = pl.read_database(
        "SELECT timestamp, temperature, precipitation, vent "
        "FROM meteo_live WHERE ville = ? ORDER BY timestamp",
        connection=conn,
        params=[ville]
    )
    conn.close()
    return df

def save_history(ville_id, daily_json):
    """
    daily_json contient :
    - time
    - temperature_2m_min
    - temperature_2m_max
    - precipitation_sum
    - relative_humidity_2m_max
    - wind_speed_10m_max
    """

    df = pl.DataFrame({
        "date": daily_json["time"],
        "temp_min": daily_json["temperature_2m_min"],
        "temp_max": daily_json["temperature_2m_max"],
        "precipitation": daily_json["precipitation_sum"],
        "humidite": daily_json["relative_humidity_2m_max"],
        "vent": daily_json["wind_speed_10m_max"],
        "id_ville": [ville_id] * len(daily_json["time"])
    })

    with sqlite3.connect("data/meteo_haiti.sqlite") as conn:
        df.write_database(
            table_name="meteo_archive",
            connection=conn,
            if_exists="append"
        )