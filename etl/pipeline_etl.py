"""
pipeline_etl.py
Orchestrateur ETL complet - Partie 1.
Projet ADREA - ImmoPro France SAS

Flux :
    4 CSV sources
        -> Extraction (pandas par chunks)
        -> Transformation T01 à T10
        -> Séparation PASS / QUARANTAINE / REJET
        -> Chargement DuckDB (PASS uniquement)
        -> Export CSV quarantaine + rejets
        -> Rapport qualité avant / après

Usage :
    uv run python etl/pipeline_etl.py

Stratégie grands volumes :
    - source_agences_flux.csv (148 MB, 820 000 lignes) : lecture par chunks
      de CHUNK_SIZE lignes pour ne jamais saturer la RAM. Chaque chunk est
      transformé et accumulé dans une liste, puis concaténé une seule fois
      avant le géocodage BAN (qui lui-même traite par batches de 5 000).
    - Les référentiels COG et FANTOIR sont chargés une seule fois en cache.
    - Le cache BAN JSON évite de rappeler l'API sur les relances pipeline.
"""

import time
from pathlib import Path

import duckdb
import pandas as pd

from etl.ban_client import (
    analyser_distribution_scores,
    geocoder_batch,
    separer_flux,
)
from etl.enrichissement import dedupliquer, joindre_fantoir
from etl.transform import (
    appliquer_detection_complement,
    appliquer_expansion_abreviations,
    appliquer_nettoyage_cp,
    appliquer_normalisation_villes,
    appliquer_parsing_flux,
    appliquer_separation_num_voie,
    charger_cog_insee,
    normaliser_casse,
)

SOURCES_DIR = Path("sources")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

DUCKDB_PATH = OUTPUT_DIR / "adrea_etl.duckdb"
CHUNK_SIZE = 50_000

# ---------------------------------------------------------------------------
# Schéma cible DuckDB (Livrable 3)
# ---------------------------------------------------------------------------

DDL_REFERENTIEL = """
CREATE TABLE IF NOT EXISTS referentiel_adresses (
    addr_ref_id     VARCHAR PRIMARY KEY,
    source_id       VARCHAR NOT NULL,
    source_fichier  VARCHAR NOT NULL,
    num_voie        VARCHAR,
    type_voie       VARCHAR,
    nom_voie        VARCHAR,
    complement      VARCHAR,
    code_postal     CHAR(5)   NOT NULL,
    code_insee      CHAR(5),
    libelle_commune VARCHAR,
    code_rivoli     VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    score_ban       DOUBLE,
    qualite         CHAR(1),
    result_label    VARCHAR,
    source_ids      VARCHAR,
    dt_creation     TIMESTAMP DEFAULT NOW()
)
"""


# ---------------------------------------------------------------------------
# Étape 1 : Extraction
# ---------------------------------------------------------------------------

def extraire_crm() -> pd.DataFrame:
    """
    Lit source_crm_contacts.csv par chunks et retourne un DataFrame unifié.
    Renomme les colonnes vers le schéma interne du pipeline.
    """
    print("\n  Extraction CRM...")
    chunks = []
    for chunk in pd.read_csv(
        SOURCES_DIR / "source_crm_contacts.csv",
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    ):
        chunk = chunk.rename(columns={
            "addr_id": "source_id",
            "addr_line_1": "adresse_ligne1",
            "addr_line_2": "adresse_ligne2",
            "city": "ville_src",
            "postal_code": "cp_src",
            "country_cd": "pays_src",
            "geocode_lat": "lat_src",
            "geocode_lng": "lng_src",
        })
        chunk["source_fichier"] = "source_crm_contacts.csv"
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(f"  CRM : {len(df):,} lignes extraites")
    return df


def extraire_sap() -> pd.DataFrame:
    """
    Lit source_sap_partenaires.csv et retourne un DataFrame unifié.
    SAP n'a pas de coordonnées GPS -> lat_src et lng_src seront None.
    """
    print("\n  Extraction SAP...")
    chunks = []
    for chunk in pd.read_csv(
        SOURCES_DIR / "source_sap_partenaires.csv",
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    ):
        # Construire adresse_ligne1 depuis les colonnes SAP
        chunk["adresse_ligne1"] = (
            chunk.get("STREET", pd.Series("", index=chunk.index)).fillna("")
            + " "
            + chunk.get("STREET2", pd.Series("", index=chunk.index)).fillna("")
        ).str.strip()

        chunk = chunk.rename(columns={
            "PARTNER_ID": "source_id",
            "CITY1": "ville_src",
            "POST_CODE1": "cp_src",
            "COUNTRY": "pays_src",
        })
        chunk["lat_src"] = None
        chunk["lng_src"] = None
        chunk["source_fichier"] = "source_sap_partenaires.csv"
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(f"  SAP : {len(df):,} lignes extraites")
    return df


def extraire_portail() -> pd.DataFrame:
    """Lit Source_portail_biens.csv et retourne un DataFrame unifié."""
    print("\n  Extraction Portail...")
    chunks = []
    for chunk in pd.read_csv(
        SOURCES_DIR / "Source_portail_biens.csv",
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    ):
        # Construire adresse_ligne1 depuis num + voie
        chunk["adresse_ligne1"] = (
            chunk.get("adresse_num", pd.Series("", index=chunk.index)).fillna("")
            + " "
            + chunk.get("adresse_voie", pd.Series("", index=chunk.index)).fillna("")
        ).str.strip()

        chunk = chunk.rename(columns={
            "bien_id": "source_id",
            "ville": "ville_src",
            "code_postal": "cp_src",
            "latitude": "lat_src",
            "longitude": "lng_src",
        })
        chunk["pays_src"] = "FR"
        chunk["source_fichier"] = "Source_portail_biens.csv"
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(f"  Portail : {len(df):,} lignes extraites")
    return df


def extraire_flux() -> pd.DataFrame:
    """
    Lit source_agences_flux.csv par chunks (148 MB).
    Applique T02 (parsing adresse_brute) directement pendant la lecture
    pour ne pas conserver deux fois les données en mémoire.
    """
    print("\n  Extraction Flux agences (148 MB - lecture par chunks)...")
    chunks = []
    for chunk in pd.read_csv(
        SOURCES_DIR / "source_agences_flux.csv",
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
    ):
        # T02 appliqué immédiatement sur chaque chunk
        chunk = appliquer_parsing_flux(chunk)

        # Construire adresse_ligne1 depuis les colonnes parsées
        chunk["adresse_ligne1"] = (
            chunk.get("num_voie_p", pd.Series("", index=chunk.index)).fillna("")
            + " "
            + chunk.get("type_voie_p", pd.Series("", index=chunk.index)).fillna("")
            + " "
            + chunk.get("nom_voie_p", pd.Series("", index=chunk.index)).fillna("")
        ).str.strip()

        chunk = chunk.rename(columns={
            "flux_id": "source_id",
            "commune": "ville_src",
            "code_postal": "cp_src",
            "lat_wgs84": "lat_src",
            "lng_wgs84": "lng_src",
        })
        chunk["pays_src"] = "FR"
        chunk["source_fichier"] = "source_agences_flux.csv"
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    print(f"  Flux : {len(df):,} lignes extraites")
    return df


# ---------------------------------------------------------------------------
# Étape 2 : Transformation T01 à T07
# ---------------------------------------------------------------------------

def transformer(df: pd.DataFrame, cog: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les règles T01 à T07 dans l'ordre sur un DataFrame source.

    T01 : Normalisation casse de adresse_ligne1
    T03 : Expansion des abréviations (appliquée après T06 sur type_voie)
    T04 : Nettoyage et validation du code postal
    T05 : Normalisation ville via COG INSEE
    T06 : Séparation numéro / libellé de voie
    T07 : Détection et isolation du complément d'adresse

    T02 est déjà appliqué pendant l'extraction pour les flux agences.
    """
    source = df["source_fichier"].iloc[0] if len(df) > 0 else "?"
    print(f"\n  Transformation T01-T07 : {source} ({len(df):,} lignes)")

    # T01 - Normalisation casse
    df["adresse_ligne1"] = df["adresse_ligne1"].fillna("").astype(str).apply(normaliser_casse)

    # T04 - Nettoyage code postal (avant T05 pour avoir cp_clean)
    df = appliquer_nettoyage_cp(df, "cp_src")

    # T06 - Séparation numéro de voie et libellé
    df = appliquer_separation_num_voie(df, "adresse_ligne1", "num_voie", "libelle_voie")

    # T07 - Détection du complément d'adresse
    df = appliquer_detection_complement(df, "libelle_voie", "adresse_principale", "complement_adresse")

    # T03 - Expansion des abréviations sur le début du libellé de voie
    # On reconstruit un champ type_voie_brut à partir du premier mot du libellé
    df["type_voie_brut"] = df["adresse_principale"].fillna("").str.split().str[0].fillna("")
    df = appliquer_expansion_abreviations(df, "type_voie_brut")

    # Construire adresse_normalisee pour l'envoi à la BAN (T08)
    df["adresse_normalisee"] = (
        df["num_voie"].fillna("").str.strip()
        + " "
        + df["adresse_principale"].fillna("").str.strip()
    ).str.strip()

    # T05 - Normalisation ville
    df = appliquer_normalisation_villes(df, "ville_src", "cp_clean", cog)

    return df


# ---------------------------------------------------------------------------
# Étape 3 : Chargement DuckDB (Livrable 3)
# ---------------------------------------------------------------------------

def creer_base_duckdb() -> duckdb.DuckDBPyConnection:
    """Crée la base DuckDB et la table cible si elles n'existent pas."""
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute(DDL_REFERENTIEL)
    print(f"  Base DuckDB prête : {DUCKDB_PATH}")
    return con


def charger_dans_duckdb(
    con: duckdb.DuckDBPyConnection,
    df_pass: pd.DataFrame,
    source_fichier: str,
):
    """
    Charge le DataFrame PASS dans la table referentiel_adresses de DuckDB.
    Construit addr_ref_id depuis source_id + source_fichier pour garantir l'unicité.
    """
    if df_pass.empty:
        return

    # Construire le DataFrame de chargement avec le schéma exact de la table
    df_load = pd.DataFrame()
    df_load["addr_ref_id"] = (
        df_pass["source_id"].astype(str)
        + "_"
        + source_fichier.replace(".csv", "").replace("source_", "")[:10]
    )
    df_load["source_id"] = df_pass["source_id"].astype(str)
    df_load["source_fichier"] = source_fichier
    df_load["num_voie"] = df_pass.get("num_voie", "").fillna("").astype(str)
    df_load["type_voie"] = df_pass.get("type_voie_norm", "").fillna("").astype(str)
    df_load["nom_voie"] = df_pass.get("adresse_principale", "").fillna("").astype(str)
    df_load["complement"] = df_pass.get("complement_adresse", "").fillna("").astype(str)
    df_load["code_postal"] = df_pass.get("cp_clean", "").fillna("").astype(str)
    df_load["code_insee"] = df_pass.get("code_insee", "").fillna("").astype(str)
    df_load["libelle_commune"] = df_pass.get("libelle_commune", "").fillna("").astype(str)
    df_load["code_rivoli"] = df_pass.get("code_rivoli", "").fillna("").astype(str)

    # Coordonnées : priorité BAN, fallback source
    df_load["latitude"] = pd.to_numeric(df_pass.get("latitude_ban"), errors="coerce")
    df_load["longitude"] = pd.to_numeric(df_pass.get("longitude_ban"), errors="coerce")

    df_load["score_ban"] = pd.to_numeric(df_pass.get("result_score"), errors="coerce")
    df_load["qualite"] = df_pass.get("qualite", "D").fillna("D").astype(str)
    df_load["result_label"] = df_pass.get("result_label", "").fillna("").astype(str)
    df_load["source_ids"] = df_pass["source_id"].astype(str)

    con.register("df_load_temp", df_load)
    con.execute("""
        INSERT OR IGNORE INTO referentiel_adresses
        SELECT
            addr_ref_id, source_id, source_fichier,
            num_voie, type_voie, nom_voie, complement,
            code_postal, code_insee, libelle_commune, code_rivoli,
            latitude, longitude, score_ban, qualite, result_label,
            source_ids, NOW()
        FROM df_load_temp
    """)
    con.unregister("df_load_temp")
    print(f"  DuckDB : {len(df_load):,} adresses PASS chargées ({source_fichier})")


# ---------------------------------------------------------------------------
# Rapport qualité avant / après (Livrable 4)
# ---------------------------------------------------------------------------

def calculer_metriques_avant(sources: dict[str, pd.DataFrame]) -> dict:
    """Calcule les métriques de qualité sur les DataFrames sources bruts."""
    import re as _re
    regex_cp = _re.compile(r"^\d{5}$")

    nb_total = sum(len(df) for df in sources.values())
    nb_gps = 0
    nb_cp_valides = 0

    for nom, df in sources.items():
        # GPS
        lat_col = {"crm": "lat_src", "sap": None, "portail": "lat_src", "flux": "lat_src"}.get(nom)
        if lat_col and lat_col in df.columns:
            nb_gps += int(df[lat_col].notna().sum())

        # CP
        cp_col = "cp_src"
        if cp_col in df.columns:
            nb_cp_valides += int(
                df[cp_col].fillna("").astype(str).apply(
                    lambda v: bool(regex_cp.match(v.strip()))
                ).sum()
            )

    return {
        "nb_total": nb_total,
        "taux_gps_pct": round(nb_gps / nb_total * 100, 2) if nb_total > 0 else 0,
        "taux_cp_valides_pct": round(nb_cp_valides / nb_total * 100, 2) if nb_total > 0 else 0,
    }


def produire_rapport_final(
    con: duckdb.DuckDBPyConnection,
    metriques_avant: dict,
    nb_doublons_supprimes: int,
    repartition: dict,
    duree_secondes: float,
):
    """
    Livrable 4 - Rapport de qualité avant / après traitement.
    Affiche et exporte output/rapport_qualite_final.csv.
    """
    # Métriques après depuis DuckDB
    nb_apres = con.execute("SELECT COUNT(*) FROM referentiel_adresses").fetchone()[0]
    nb_gps_apres = con.execute(
        "SELECT COUNT(*) FROM referentiel_adresses WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchone()[0]
    nb_cp_apres = con.execute(
        "SELECT COUNT(*) FROM referentiel_adresses WHERE LENGTH(code_postal) = 5"
    ).fetchone()[0]

    taux_gps_apres = round(nb_gps_apres / nb_apres * 100, 2) if nb_apres > 0 else 0
    taux_cp_apres = round(nb_cp_apres / nb_apres * 100, 2) if nb_apres > 0 else 0

    largeur = 70
    print("\n" + "RAPPORT QUALITE FINAL - PROJET ADREA".center(largeur))
    print("-" * largeur)
    print(f"  Durée totale du pipeline : {duree_secondes:.1f}s")
    print()
    print(f"  {'Metrique':<40} {'Avant':>10} {'Apres':>10}")
    print("  " + "-" * 62)
    print(f"  {'Adresses totales':<40} {metriques_avant['nb_total']:>10,} {nb_apres:>10,}")
    print(f"  {'Taux GPS (%)':<40} {metriques_avant['taux_gps_pct']:>10} {taux_gps_apres:>10}")
    print(f"  {'Taux CP valides (%)':<40} {metriques_avant['taux_cp_valides_pct']:>10} {taux_cp_apres:>10}")
    print(f"  {'Doublons supprimes':<40} {'':>10} {nb_doublons_supprimes:>10,}")
    print()
    print("  Répartition des flux :")
    nb_total_flux = sum(repartition.values())
    for label, nb in repartition.items():
        pct = round(nb / nb_total_flux * 100, 1) if nb_total_flux > 0 else 0
        print(f"    {label:<20} {nb:>10,}  ({pct}%)")
    print("-" * largeur)

    # Export CSV du rapport
    rapport = {
        "metrique": [
            "nb_adresses_avant", "nb_adresses_apres_pass",
            "taux_gps_avant_pct", "taux_gps_apres_pct",
            "taux_cp_valides_avant_pct", "taux_cp_valides_apres_pct",
            "doublons_supprimes",
            "flux_pass", "flux_quarantaine", "flux_rejet",
            "duree_pipeline_secondes",
        ],
        "valeur": [
            metriques_avant["nb_total"], nb_apres,
            metriques_avant["taux_gps_pct"], taux_gps_apres,
            metriques_avant["taux_cp_valides_pct"], taux_cp_apres,
            nb_doublons_supprimes,
            repartition.get("PASS", 0),
            repartition.get("QUARANTAINE", 0),
            repartition.get("REJET", 0),
            round(duree_secondes, 1),
        ],
    }
    df_rapport = pd.DataFrame(rapport)
    df_rapport.to_csv(OUTPUT_DIR / "rapport_qualite_final.csv", index=False)
    print(f"\n  Rapport exporté : {OUTPUT_DIR / 'rapport_qualite_final.csv'}")


# ---------------------------------------------------------------------------
# Orchestrateur principal
# ---------------------------------------------------------------------------

def run_pipeline():
    """
    Exécute le pipeline ETL complet dans l'ordre :
        1. Extraction des 4 sources (T02 intégré pour les flux)
        2. Transformation T01-T07
        3. Géocodage BAN T08
        4. Enrichissement FANTOIR T09
        5. Déduplication Jaro-Winkler T10
        6. Séparation PASS / QUARANTAINE / REJET
        7. Chargement DuckDB
        8. Export quarantaine.csv + rejected.csv
        9. Rapport qualité final
    """
    debut_global = time.time()

    print("\nPIPELINE ETL ADREA - ImmoPro France SAS")
    print("=" * 60)

    # Chargement des référentiels (une seule fois)
    print("\nChargement des référentiels...")
    cog = charger_cog_insee()

    # Connexion DuckDB
    con = creer_base_duckdb()

    # Extraction des 4 sources
    print("\nETAPE 1 : EXTRACTION")
    print("-" * 40)
    df_crm = extraire_crm()
    df_sap = extraire_sap()
    df_portail = extraire_portail()
    df_flux = extraire_flux()

    sources_brutes = {
        "crm": df_crm,
        "sap": df_sap,
        "portail": df_portail,
        "flux": df_flux,
    }
    metriques_avant = calculer_metriques_avant(sources_brutes)

    nb_doublons_total = 0
    repartition_totale = {"PASS": 0, "QUARANTAINE": 0, "REJET": 0}

    # Traitement source par source
    for nom, df in sources_brutes.items():
        source_fichier = df["source_fichier"].iloc[0]
        print(f"\nETAPE 2-6 : TRANSFORMATION + BAN + FANTOIR + DEDUP [{source_fichier}]")
        print("-" * 60)

        # T01, T03, T04, T05, T06, T07
        df = transformer(df, cog)

        # T08 - Géocodage BAN
        print("\n  T08 Géocodage BAN...")
        df = geocoder_batch(df, col_id="source_id")
        analyser_distribution_scores(df)

        # T09 - Code RIVOLI FANTOIR
        df = joindre_fantoir(df)

        # T10 - Déduplication Jaro-Winkler
        df, df_doublons = dedupliquer(df)
        nb_doublons_total += len(df_doublons)

        if not df_doublons.empty:
            df_doublons.to_csv(
                OUTPUT_DIR / f"doublons_{nom}.csv",
                index=False,
            )

        # Séparation des flux
        df_pass, df_quarantaine, df_rejet = separer_flux(df)

        repartition_totale["PASS"] += len(df_pass)
        repartition_totale["QUARANTAINE"] += len(df_quarantaine)
        repartition_totale["REJET"] += len(df_rejet)

        # Chargement DuckDB (PASS uniquement)
        charger_dans_duckdb(con, df_pass, source_fichier)

        # Export quarantaine
        if not df_quarantaine.empty:
            chemin_quarantaine = OUTPUT_DIR / f"adresses_quarantaine_{nom}.csv"
            df_quarantaine.to_csv(chemin_quarantaine, index=False)
            print(f"  Quarantaine : {len(df_quarantaine):,} lignes -> {chemin_quarantaine}")

        # Export rejets
        if not df_rejet.empty:
            chemin_rejet = OUTPUT_DIR / f"adresses_rejetees_{nom}.csv"
            df_rejet.to_csv(chemin_rejet, index=False)
            print(f"  Rejets      : {len(df_rejet):,} lignes -> {chemin_rejet}")

    # Rapport final
    duree = time.time() - debut_global
    print("\nETAPE 7 : RAPPORT QUALITE FINAL")
    print("-" * 40)
    produire_rapport_final(
        con,
        metriques_avant,
        nb_doublons_total,
        repartition_totale,
        duree,
    )

    con.close()
    print(f"\nPipeline terminé en {duree:.1f}s. Base DuckDB : {DUCKDB_PATH}")


if __name__ == "__main__":
    run_pipeline()