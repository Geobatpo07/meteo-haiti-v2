import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
from tqdm import tqdm
from modules.storage import (
    init_db,
    sync_villes_from_yaml,
    read_villes,
    insert_dataframe,
    connect_db
)
from modules.meteo import get_meteo_data


# ---------------------------------------------------------
# OUTILS
# ---------------------------------------------------------

def ensure_data_folder():
    """Créer le dossier data/ si absent."""
    if not os.path.exists("data"):
        os.makedirs("data")
        print("📁 Dossier 'data' créé.")


def reset_database_if_requested(force_reset: bool):
    """Supprime la base SQLite si l'utilisateur demande --force."""
    db_file = "data/meteo_haiti.sqlite"

    if force_reset and os.path.exists(db_file):
        os.remove(db_file)
        print("🗑 Base SQLite supprimée (option --force).")


# ---------------------------------------------------------
# COLLECTE ARCHIVE AVEC TQDM
# ---------------------------------------------------------

def run_collection(start_year: int, end_year: int, pause: float, villes_filtrees: list | None):
    """
    Collecte 2010–2020 avec barre de progression.
    S'insère dans meteo_archive.
    """

    villes = read_villes()

    # Filtrage par noms de ville si demandé
    if villes_filtrees:
        villes = villes[villes["ville"].isin(villes_filtrees)]
        print(f"🎯 Filtre appliqué : {len(villes)} villes sélectionnées.")

    if villes.empty:
        print("❌ Aucune ville sélectionnée. Abandon.")
        return

    total_taches = len(villes) * (end_year - start_year + 1)

    nb_annees = end_year - start_year + 1

    print(f"📊 Collecte pour {len(villes)} villes • {start_year} → {end_year}")
    print(f"Total estimé : {nb_annees} années à télécharger\n")

    pbar = tqdm(total=total_taches, desc="📥 Collecte", unit="année")

    for _, ville in villes.iterrows():
        nom = ville["ville"]

        for year in range(start_year, end_year + 1):

            df = get_meteo_data(
                id_ville=ville["id"],
                lat=ville["latitude"],
                lon=ville["longitude"],
                year=year
            )

            if df is not None:
                insert_dataframe("meteo_archive", df)

            pbar.update(1)
            pbar.set_postfix(ville=nom, année=year)

    pbar.close()

    print("\n🎉 Collecte terminée ! Données insérées dans `meteo_archive`.")


# ---------------------------------------------------------
# CLI AVANCÉ (ARGPARSE)
# ---------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Collecte des données météo archivées (Open-Meteo).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--start",
        type=int,
        default=2010,
        help="Année de début"
    )

    parser.add_argument(
        "--end",
        type=int,
        default=2020,
        help="Année de fin"
    )

    parser.add_argument(
        "--pause",
        type=float,
        default=1.0,
        help="Pause (secondes) entre les appels API"
    )

    parser.add_argument(
        "--villes",
        nargs="+",
        help="Liste de villes à collecter (ex: --villes 'Port-au-Prince' 'Cap-Haïtien')"
    )

    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Ne pas synchroniser les villes depuis config.yaml"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Supprime la base SQLite avant de commencer"
    )

    return parser.parse_args()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

if __name__ == "__main__":
    print("=== 📡 COLLECTE DES DONNÉES ARCHIVÉES (Open-Meteo) ===")

    args = parse_arguments()

    ensure_data_folder()
    reset_database_if_requested(args.force)

    print("🔧 Initialisation de la base...")
    init_db()

    if not args.no_sync:
        print("🌍 Synchronisation des villes (config.yaml)...")
        sync_villes_from_yaml()
    else:
        print("⏭ Synchronisation villes ignorée (--no-sync)")

    print("⏳ Démarrage de la collecte...\n")

    run_collection(
        start_year=args.start,
        end_year=args.end,
        pause=args.pause,
        villes_filtrees=args.villes
    )
