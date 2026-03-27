"""
profiling.py
Livrable 1 - Extraction et profiling initial des 4 sources CSV.
Projet ADREA - ImmoPro France SAS

Usage :
    python etl/profiling.py

Stratégie grands volumes :
    Lecture par chunks (CHUNK_SIZE lignes à la fois) pour éviter de saturer
    la RAM sur source_agences_flux.csv (148 MB, 820 000 lignes).
    Les métriques sont accumulées chunk par chunk sans jamais charger
    le fichier entier en mémoire en une seule fois.
"""

import re
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

SOURCES_DIR = Path("sources")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

# Nombre de lignes lues par itération pour les grands fichiers
CHUNK_SIZE = 50_000

# Configuration par source : colonnes métier clés
SOURCE_CONFIG = {
    "source_crm_contacts.csv": {
        "id_col": "addr_id",
        "city_col": "city",
        "cp_col": "postal_code",
        "lat_col": "geocode_lat",
        "lng_col": "geocode_lng",
        # Les 3 colonnes les plus susceptibles de contenir des anomalies de valeur
        # Justification : city (abréviations, casse), postal_code (format), country_cd (hétérogène)
        "anomaly_cols": ["city", "postal_code", "country_cd"],
    },
    "source_sap_partenaires.csv": {
        "id_col": "PARTNER_ID",
        "city_col": "CITY1",
        "cp_col": "POST_CODE1",
        "lat_col": None,
        "lng_col": None,
        # CITY1 (noms libres SAP), POST_CODE1 (format variable), COUNTRY (codes non normalisés)
        "anomaly_cols": ["CITY1", "POST_CODE1", "COUNTRY"],
    },
    "Source_portail_biens.csv": {
        "id_col": "bien_id",
        "city_col": "ville",
        "cp_col": "code_postal",
        "lat_col": "latitude",
        "lng_col": "longitude",
        # ville (saisie libre), code_postal (format), dpe (valeurs hors norme possibles)
        "anomaly_cols": ["ville", "code_postal", "dpe"],
    },
    "source_agences_flux.csv": {
        "id_col": "flux_id",
        "city_col": "commune",
        "cp_col": "code_postal",
        "lat_col": "lat_wgs84",
        "lng_col": "lng_wgs84",
        # adresse_brute (champ monolithique non structuré), commune, code_postal
        "anomaly_cols": ["adresse_brute", "commune", "code_postal"],
    },
}

# Regex de validation d'un code postal français (exactement 5 chiffres)
REGEX_CP = re.compile(r"^\d{5}$")


def valider_cp(valeur: str) -> bool:
    """Retourne True si la valeur est un code postal français valide (5 chiffres)."""
    if not valeur or not isinstance(valeur, str):
        return False
    return bool(REGEX_CP.match(valeur.strip()))


def est_ville_normalisee(valeur: str) -> bool:
    """
    Heuristique de détection d'une ville normalisée :
    commence par une majuscule, ne contient que des lettres, espaces, tirets, apostrophes.
    Exclut les villes tout en majuscules, avec chiffres ou abrégées.
    """
    if not valeur or not isinstance(valeur, str):
        return False
    valeur = valeur.strip()
    if valeur != valeur.title() and valeur == valeur.upper():
        return False
    pattern = re.compile(r"^[A-ZÀ-Ÿ][a-zA-ZÀ-ÿ\s\-']+$")
    return bool(pattern.match(valeur))


def profiler_chunk(chunk: pd.DataFrame, config: dict, accumulateur: dict):
    """
    Accumule les métriques d'un chunk dans l'accumulateur.
    Appelée pour chaque chunk lors de la lecture par morceaux.
    """
    nb = len(chunk)
    accumulateur["nb_lignes"] += nb

    # Colonnes présentes dans ce chunk
    if accumulateur["nb_colonnes"] == 0:
        accumulateur["nb_colonnes"] = len(chunk.columns)

    # Comptage des valeurs nulles par colonne
    for col in chunk.columns:
        accumulateur["nulls_par_col"][col] = (
            accumulateur["nulls_par_col"].get(col, 0)
            + int(chunk[col].isnull().sum())
        )

    # Codes postaux valides
    cp_col = config["cp_col"]
    if cp_col in chunk.columns:
        accumulateur["cp_total"] += nb
        accumulateur["cp_valides"] += int(
            chunk[cp_col].fillna("").astype(str).apply(valider_cp).sum()
        )

    # Villes normalisées
    city_col = config["city_col"]
    if city_col in chunk.columns:
        accumulateur["city_total"] += nb
        accumulateur["city_normalisees"] += int(
            chunk[city_col].fillna("").astype(str).apply(est_ville_normalisee).sum()
        )

    # Coordonnées GPS renseignées
    lat_col = config["lat_col"]
    lng_col = config["lng_col"]
    if lat_col and lng_col and lat_col in chunk.columns and lng_col in chunk.columns:
        accumulateur["gps_total"] += nb
        accumulateur["gps_renseignes"] += int(
            chunk[[lat_col, lng_col]].notna().all(axis=1).sum()
        )

    # Top 10 fréquences des colonnes à anomalies
    for col in config["anomaly_cols"]:
        if col in chunk.columns:
            counter = Counter(chunk[col].fillna("(vide)").astype(str).tolist())
            accumulateur["freq_anomaly"][col] = (
                accumulateur["freq_anomaly"].get(col, Counter()) + counter
            )

    # Doublons intra-fichier sur l'identifiant principal
    id_col = config["id_col"]
    if id_col in chunk.columns:
        accumulateur["ids"].update(chunk[id_col].dropna().astype(str).tolist())


def lire_et_profiler(filename: str, config: dict) -> dict:
    """
    Lit un CSV source par chunks et calcule toutes les métriques de qualité.
    Retourne un dictionnaire de résultats consolidés.
    """
    filepath = SOURCES_DIR / filename
    if not filepath.exists():
        print(f"  ATTENTION : fichier introuvable -> {filepath}")
        return {}

    accumulateur = {
        "nb_lignes": 0,
        "nb_colonnes": 0,
        "nulls_par_col": {},
        "cp_total": 0,
        "cp_valides": 0,
        "city_total": 0,
        "city_normalisees": 0,
        "gps_total": 0,
        "gps_renseignes": 0,
        "freq_anomaly": {},
        "ids": Counter(),
    }

    print(f"  Lecture par chunks ({CHUNK_SIZE} lignes) : {filename}")
    nb_chunks = 0
    for chunk in pd.read_csv(
        filepath,
        chunksize=CHUNK_SIZE,
        dtype=str,
        low_memory=False,
        encoding="utf-8",
    ):
        profiler_chunk(chunk, config, accumulateur)
        nb_chunks += 1

    print(f"  {nb_chunks} chunks traités - {accumulateur['nb_lignes']:,} lignes lues")

    # Calcul des métriques finales
    nb_lignes = accumulateur["nb_lignes"]

    taux_cp = (
        round(accumulateur["cp_valides"] / accumulateur["cp_total"] * 100, 2)
        if accumulateur["cp_total"] > 0
        else 0.0
    )
    taux_cp_invalides_abs = accumulateur["cp_total"] - accumulateur["cp_valides"]

    taux_villes = (
        round(accumulateur["city_normalisees"] / accumulateur["city_total"] * 100, 2)
        if accumulateur["city_total"] > 0
        else 0.0
    )

    taux_gps = (
        round(accumulateur["gps_renseignes"] / accumulateur["gps_total"] * 100, 2)
        if accumulateur["gps_total"] > 0
        else 0.0
    )

    # Taux de doublons intra-fichier sur l'identifiant principal
    nb_ids_uniques = len(accumulateur["ids"])
    nb_ids_total = sum(accumulateur["ids"].values())
    taux_doublons = (
        round((nb_ids_total - nb_ids_uniques) / nb_ids_total * 100, 2)
        if nb_ids_total > 0
        else 0.0
    )

    # % de nulls global
    total_cellules = nb_lignes * accumulateur["nb_colonnes"]
    total_nulls = sum(accumulateur["nulls_par_col"].values())
    pct_null_global = (
        round(total_nulls / total_cellules * 100, 2) if total_cellules > 0 else 0.0
    )

    return {
        "fichier": filename,
        "nb_lignes": nb_lignes,
        "nb_colonnes": accumulateur["nb_colonnes"],
        "pct_null_global": pct_null_global,
        "nulls_par_col": accumulateur["nulls_par_col"],
        "taux_cp_valides_pct": taux_cp,
        "cp_invalides_abs": taux_cp_invalides_abs,
        "taux_villes_normalisees_pct": taux_villes,
        "taux_gps_pct": taux_gps,
        "taux_doublons_intra_pct": taux_doublons,
        "freq_anomaly": {
            col: counter.most_common(10)
            for col, counter in accumulateur["freq_anomaly"].items()
        },
    }


def afficher_tableau_comparatif(resultats: list[dict]):
    """Affiche un tableau comparatif des métriques de qualité entre les 4 sources."""
    largeur = 92
    print("\n" + "TABLEAU COMPARATIF DE QUALITE DES SOURCES".center(largeur))
    print("-" * largeur)

    entetes = [
        "Fichier source",
        "Lignes",
        "Cols",
        "Nulls %",
        "CP valides %",
        "Villes norm. %",
        "GPS %",
        "Doublons %",
    ]
    col_widths = [30, 10, 6, 9, 13, 15, 8, 12]

    header = "".join(h.ljust(w) for h, w in zip(entetes, col_widths))
    print(header)
    print("-" * largeur)

    for r in resultats:
        if not r:
            continue
        nom_court = r["fichier"].replace("source_", "").replace(".csv", "")[:28]
        ligne = (
            nom_court.ljust(col_widths[0])
            + f"{r['nb_lignes']:,}".ljust(col_widths[1])
            + str(r["nb_colonnes"]).ljust(col_widths[2])
            + f"{r['pct_null_global']}%".ljust(col_widths[3])
            + f"{r['taux_cp_valides_pct']}%".ljust(col_widths[4])
            + f"{r['taux_villes_normalisees_pct']}%".ljust(col_widths[5])
            + f"{r['taux_gps_pct']}%".ljust(col_widths[6])
            + f"{r['taux_doublons_intra_pct']}%".ljust(col_widths[7])
        )
        print(ligne)

    print("-" * largeur)


def afficher_nulls_par_colonne(resultat: dict):
    """Affiche le détail des valeurs nulles par colonne pour une source."""
    nb_lignes = resultat["nb_lignes"]
    print(f"\n  Nulls par colonne - {resultat['fichier']}")
    cols_avec_nulls = {
        col: n for col, n in resultat["nulls_par_col"].items() if n > 0
    }
    if not cols_avec_nulls:
        print("  Aucune valeur nulle détectée.")
        return
    for col, nb_nulls in sorted(cols_avec_nulls.items(), key=lambda x: -x[1]):
        pct = round(nb_nulls / nb_lignes * 100, 1)
        print(f"    {col:<35} {nb_nulls:>8,} nulls  ({pct}%)")


def afficher_top10_anomalies(resultat: dict, anomaly_cols_justification: dict):
    """
    Affiche les 10 valeurs les plus fréquentes pour les 3 colonnes à anomalies.
    Inclut la justification du choix des colonnes.
    """
    print(f"\n  Colonnes à anomalies - {resultat['fichier']}")
    for col, top10 in resultat["freq_anomaly"].items():
        justif = anomaly_cols_justification.get(col, "Colonne à fort taux d'anomalies.")
        print(f"\n    Colonne : {col}")
        print(f"    Pourquoi : {justif}")
        print(f"    Top 10 valeurs les plus fréquentes :")
        for rang, (valeur, nb) in enumerate(top10, 1):
            print(f"      {rang:>2}. {str(valeur):<45} {nb:>8,} occurrences")


def identifier_source_la_plus_mauvaise(resultats: list[dict]) -> str:
    """
    Identifie la source avec la qualité la plus mauvaise.
    Critère composite : faible taux CP valides, faible taux GPS, fort taux nulls.
    """
    scores = []
    for r in resultats:
        if not r:
            continue
        score = (
            (100 - r["taux_cp_valides_pct"]) * 0.4
            + (100 - r["taux_gps_pct"]) * 0.3
            + r["pct_null_global"] * 0.2
            + r["taux_doublons_intra_pct"] * 0.1
        )
        scores.append((r["fichier"], score))
    scores.sort(key=lambda x: -x[1])
    return scores[0][0] if scores else "N/A"


# Justifications textuelles pour chaque colonne à anomalies
JUSTIFICATIONS = {
    "city": "Saisie libre dans le CRM : abréviations, casse incohérente (paris 14, PARIS14EME), arrondissements intégrés.",
    "postal_code": "Format non contraint : espaces parasites (75 014), trop longs (750014), vides.",
    "country_cd": "Non normalisé : FR, FRA, France, france, F coexistent dans la même colonne.",
    "CITY1": "Saisie SAP non contrainte : majuscules systématiques, variantes orthographiques.",
    "POST_CODE1": "Champ alphanumérique SAP : peut contenir des codes étrangers ou des formats non français.",
    "COUNTRY": "Code pays SAP issu de référentiels internes hétérogènes.",
    "ville": "Portail web : saisie libre par les agences, mélange de casses et variantes.",
    "code_postal": "Champ numérique ou texte selon la source, peut contenir des valeurs hors norme.",
    "dpe": "Valeurs DPE non normalisées : NC, vide, lettres hors A-G, saisies libres.",
    "adresse_brute": "Champ monolithique non structuré : toute l'adresse collée sans séparation, formats variables.",
    "commune": "Extrait de adresse_brute par regex : parsing imparfait, communes manquantes ou mal découpées.",
}


def main():
    print("\nPROFILING INITIAL - Projet ADREA - ImmoPro France SAS")
    print("Lecture des 4 sources CSV par chunks de", CHUNK_SIZE, "lignes\n")

    resultats = []

    for filename, config in SOURCE_CONFIG.items():
        print(f"Profiling : {filename}")
        resultat = lire_et_profiler(filename, config)
        resultats.append(resultat)

    # Tableau comparatif des métriques
    afficher_tableau_comparatif(resultats)

    # Détail nulls par colonne pour chaque source
    print("\n\nDETAIL DES VALEURS NULLES PAR COLONNE")
    for r in resultats:
        if r:
            afficher_nulls_par_colonne(r)

    # Top 10 valeurs pour les colonnes à anomalies
    print("\n\nANALYSE DES COLONNES A ANOMALIES (Top 10 valeurs)")
    for r in resultats:
        if r:
            afficher_top10_anomalies(r, JUSTIFICATIONS)

    # Identification de la source la plus problématique
    pire_source = identifier_source_la_plus_mauvaise(resultats)
    print(f"\n\nSOURCE LA PLUS PROBLEMATIQUE : {pire_source}")
    print(
        "Raison : score composite basé sur taux CP invalides (40%), "
        "absence GPS (30%), nulls (20%), doublons (10%)."
    )
    print(
        "source_agences_flux.csv est attendu comme le plus problématique : "
        "champ adresse_brute non structuré, 820 000 lignes, formats hétérogènes."
    )

    # Problème spécifique source_agences_flux
    print("\nPROBLEME SPECIFIQUE source_agences_flux.csv")
    print(
        "Le champ 'adresse_brute' contient toute l'adresse dans un seul texte "
        "sans structure (numéro, type voie, nom voie, CP, commune mélangés). "
        "Il faut parser ce champ par regex avant toute autre transformation (règle T02). "
        "De plus, le fichier de 148 MB / 820 000 lignes nécessite "
        "un traitement impérativement par chunks pour tenir en RAM."
    )

    # Export du rapport en CSV
    rapport = [
        {
            "fichier": r["fichier"],
            "nb_lignes": r["nb_lignes"],
            "nb_colonnes": r["nb_colonnes"],
            "pct_null_global": r["pct_null_global"],
            "taux_cp_valides_pct": r["taux_cp_valides_pct"],
            "cp_invalides_abs": r["cp_invalides_abs"],
            "taux_villes_normalisees_pct": r["taux_villes_normalisees_pct"],
            "taux_gps_pct": r["taux_gps_pct"],
            "taux_doublons_intra_pct": r["taux_doublons_intra_pct"],
        }
        for r in resultats
        if r
    ]
    df_rapport = pd.DataFrame(rapport)
    output_path = OUTPUT_DIR / "rapport_qualite_initial.csv"
    df_rapport.to_csv(output_path, index=False)
    print(f"\nRapport exporté : {output_path}")


if __name__ == "__main__":
    main()