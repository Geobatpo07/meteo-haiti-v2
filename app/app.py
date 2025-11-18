import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st

st.set_page_config(
    page_title="HaïtiMétéo+",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------------------
# SIDEBAR : MENU PERSONNALISÉ
# ------------------------------

st.sidebar.title("🌤️ HaïtiMétéo+")
st.sidebar.markdown("### Tableau de bord climatologique")

menu = st.sidebar.radio(
    "Navigation",
    [
        "Accueil",
        "Météo en direct",
        "Historique Live",
        "Archives météorologiques",
        "Carte des villes",
        "Gestion des villes"
    ]
)

# ------------------------------
# ROUTEUR
# ------------------------------

if menu == "Accueil":
    st.title("HaïtiMétéo+")
    st.subheader("Plateforme moderne d’analyse météorologique pour Haïti")
    st.write("""
Bienvenue dans **HaïtiMétéo+**, votre tableau de bord centralisé pour explorer, analyser et surveiller les données climatiques d’Haïti.
Utilisez le menu de gauche pour naviguer entre les sections.
""")

elif menu == "Météo en direct":
    import views.page_live as page
    page.render()

elif menu == "Historique Live":
    import views.page_historique as page
    page.render()

elif menu == "Archives météorologiques":
    import views.page_archive as page
    page.render()

elif menu == "Carte des villes":
    import views.page_map as page
    page.render()

elif menu == "Gestion des villes":
    import views.page_ville as page
    page.render()
