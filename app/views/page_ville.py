# -*- coding: utf-8 -*-
# HaïtiMétéo+ — Page Gestion des Villes

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import streamlit as st
import yaml
import pandas as pd
import pydeck as pdk

from modules.utils import load_yaml
from modules.storage import sync_villes_from_yaml, read_villes

CONFIG_PATH = "data/config.yaml"


# ------------------------------------------------------
# Trouver le prochain ID disponible
# ------------------------------------------------------
def next_available_id(villes):
    ids = sorted([v["id"] for v in villes])
    for i in range(1, 9999):
        if i not in ids:
            return i
    return ids[-1] + 1


# ------------------------------------------------------
# Page principale
# ------------------------------------------------------
def render():
    st.title("Gestion des villes – HaïtiMétéo+")

    config = load_yaml(CONFIG_PATH)
    villes_config = config.get("villes", [])

    df_config = pd.DataFrame(villes_config)

    st.markdown("---")
    st.subheader("📍 Liste des villes enregistrées")

    # ------------------------------------------------------
    # Recherche / filtre
    # ------------------------------------------------------
    search = st.text_input("Rechercher une ville", placeholder="Nom ou ID...").lower()

    if search:
        df_display = df_config[
            df_config["nom"].str.lower().str.contains(search)
            | df_config["id"].astype(str).str.contains(search)
        ]
    else:
        df_display = df_config

    st.dataframe(df_display, use_container_width=True)

    st.markdown("---")

    # ======================================================
    # 🗺️ Visualisation sur Carte
    # ======================================================
    if not df_config.empty:
        st.subheader("Carte des villes")

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_config,
            get_position="[longitude, latitude]",
            get_color="[0, 122, 255, 180]",
            get_radius=50000,
            pickable=True
        )

        view_state = pdk.ViewState(
            latitude=df_config["latitude"].mean(),
            longitude=df_config["longitude"].mean(),
            zoom=6.8
        )

        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip={"text": "{nom}\n({latitude}, {longitude})"}
            )
        )

    st.markdown("---")

    # ======================================================
    # ➕ Ajouter une ville
    # ======================================================
    st.subheader("➕ Ajouter une ville")

    new_col1, new_col2 = st.columns(2)

    suggested_id = next_available_id(villes_config)

    nom = new_col1.text_input("Nom de la ville")
    id_ville = new_col2.number_input("ID", min_value=1, step=1, value=suggested_id)

    lat = st.number_input("Latitude", format="%.6f")
    lon = st.number_input("Longitude", format="%.6f")

    if st.button("Enregistrer la nouvelle ville"):
        if not nom or lat == 0 or lon == 0:
            st.error("Veuillez remplir *tous* les champs.")
            return

        if any(v["id"] == int(id_ville) for v in villes_config):
            st.error("Cet ID existe déjà.")
            return

        if any(v["nom"].lower() == nom.lower() for v in villes_config):
            st.error("Une ville portant déjà ce nom existe.")
            return

        villes_config.append({
            "id": int(id_ville),
            "nom": nom,
            "latitude": float(lat),
            "longitude": float(lon)
        })

        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            yaml.safe_dump({"villes": villes_config}, f, allow_unicode=True)

        sync_villes_from_yaml()
        st.success("Ville ajoutée avec succès 🎉")
        st.experimental_rerun()

    st.markdown("---")

    # ======================================================
    # ✏️ Modifier une ville
    # ======================================================
    st.subheader("✏️ Modifier une ville existante")

    if df_config.empty:
        st.info("Aucune ville à modifier.")
    else:
        ville_to_edit = st.selectbox(
            "Sélectionnez la ville à modifier",
            df_config["nom"].tolist()
        )

        selected = df_config[df_config["nom"] == ville_to_edit].iloc[0]

        edit_id = st.number_input("ID", min_value=1, value=int(selected["id"]))
        edit_nom = st.text_input("Nom", value=selected["nom"])
        edit_lat = st.number_input("Latitude", format="%.6f", value=float(selected["latitude"]))
        edit_lon = st.number_input("Longitude", format="%.6f", value=float(selected["longitude"]))

        if st.button("Mettre à jour"):
            # Validation
            if any(v["id"] == int(edit_id) and v["nom"] != ville_to_edit for v in villes_config):
                st.error("Un autre enregistrement utilise déjà cet ID.")
                return

            if any(v["nom"].lower() == edit_nom.lower() and v["nom"] != ville_to_edit for v in villes_config):
                st.error("Un autre enregistrement utilise déjà ce nom.")
                return

            # Mise à jour
            for v in villes_config:
                if v["nom"] == ville_to_edit:
                    v["id"] = int(edit_id)
                    v["nom"] = edit_nom
                    v["latitude"] = float(edit_lat)
                    v["longitude"] = float(edit_lon)

            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                yaml.safe_dump({"villes": villes_config}, f, allow_unicode=True)

            sync_villes_from_yaml()
            st.success("Ville mise à jour ✔")
            st.experimental_rerun()

    st.markdown("---")

    # ======================================================
    # ❌ Suppression
    # ======================================================
    st.subheader("🗑️ Supprimer une ville")

    if not df_config.empty:
        ville_to_delete = st.selectbox(
            "Choisissez la ville à supprimer",
            df_config["nom"].tolist(),
            key="delete_select"
        )

        if st.button("Supprimer définitivement"):
            villes_config = [v for v in villes_config if v["nom"] != ville_to_delete]

            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                yaml.safe_dump({"villes": villes_config}, f, allow_unicode=True)

            sync_villes_from_yaml()
            st.success("Ville supprimée 🗑️")
            st.experimental_rerun()
