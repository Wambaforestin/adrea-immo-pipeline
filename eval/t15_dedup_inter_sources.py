"""
t15_dedup_inter_sources.py
Exercice 3 — T15 : Déduplication inter-sources CRM vs PORTAIL.
Projet ADREA — ImmoPro France SAS

T10 (cours) détectait les doublons à l'intérieur d'une même source.
T15 détecte les doublons ENTRE sources différentes.
Le même bien peut apparaître dans le CRM et sur le Portail avec des
orthographes légèrement différentes.

Usage :
    uv run python eval/t15_dedup_inter_sources.py
"""

import unicodedata
from pathlib import Path

import argparse
import duckdb
import pandas as pd
from rapidfuzz.distance import JaroWinkler

DUCKDB_PATH_DEFAUT = Path("output/adrea_etl.duckdb")
TABLE = "referentiel_adresses"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db",
        default=str(DUCKDB_PATH_DEFAUT),
        help="Chemin vers la base DuckDB (défaut : output/adrea_etl.duckdb)",
    )
    parser.add_argument(
        "--table",
        default=TABLE,
        help="Nom de la table cible (défaut : referentiel_adresses)",
    )
    return parser.parse_args()


SEUIL_JARO = 0.92


def sans_accents(texte: str) -> str:
    """Supprime les accents pour comparaison robuste."""
    n = unicodedata.normalize("NFD", texte)
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


# ---------------------------------------------------------------------------
# 3.1 Pré-filtrage SQL — paires partageant CP + numéro de voie
# ---------------------------------------------------------------------------


def prefiltrage_sql(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Identifie les paires d'adresses inter-sources partageant le même
    code_postal ET le même num_voie.

    Le pré-filtrage SQL réduit drastiquement l'espace de recherche avant
    la comparaison fuzzy coûteuse (O(n²) dans le groupe).

    Retourne un DataFrame des paires candidates.
    """
    print("\n  3.1 — Pré-filtrage SQL : paires CP + num_voie identiques")

    # On compte d'abord les paires inter-sources
    nb_paires = con.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT a.source_id AS id_a, b.source_id AS id_b
            FROM {TABLE} a
            JOIN {TABLE} b
              ON  a.code_postal = b.code_postal
              AND a.num_voie    = b.num_voie
              AND a.num_voie   != ''
              AND a.source_fichier != b.source_fichier
              AND a.source_id < b.source_id
        )
    """).fetchone()[0]

    print(
        f"  Paires candidates (même CP + même num_voie, sources différentes) : {nb_paires:,}"
    )

    # Afficher les noms de fichiers sources présents dans la table
    sources_presentes = con.execute(
        f"SELECT DISTINCT source_fichier FROM {TABLE}"
    ).fetchall()
    print(f"  Sources présentes dans DuckDB : {[r[0] for r in sources_presentes]}")

    # Récupérer un échantillon pour analyse
    df_paires = con.execute(f"""
        SELECT
            a.source_id      AS id_a,
            a.source_fichier AS source_a,
            a.num_voie       AS num_a,
            a.nom_voie       AS voie_a,
            a.code_postal    AS cp,
            b.source_id      AS id_b,
            b.source_fichier AS source_b,
            b.num_voie       AS num_b,
            b.nom_voie       AS voie_b,
            a.score_ban      AS score_a,
            b.score_ban      AS score_b
        FROM {TABLE} a
        JOIN {TABLE} b
          ON  a.code_postal = b.code_postal
          AND a.num_voie    = b.num_voie
          AND a.num_voie   != ''
          AND a.source_fichier != b.source_fichier
          AND a.source_id < b.source_id
        LIMIT 1000
    """).df()

    return df_paires


# ---------------------------------------------------------------------------
# 3.2 Déduplication fuzzy CRM vs PORTAIL
# ---------------------------------------------------------------------------


def t15_dedup_inter_sources(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    T15 — Déduplication inter-sources entre CRM et PORTAIL uniquement.

    Algorithme :
        1. Séparation des adresses CRM et PORTAIL
        2. Partitionnement par code_postal (blocking) pour limiter O(n²)
        3. Dans chaque groupe CP : compare CRM vs PORTAIL uniquement
           (pas CRM vs CRM, ni PORTAIL vs PORTAIL)
        4. Jaro-Winkler >= 0.92 sur le nom_voie normalisé (sans accents)
        5. Vérification que les num_voie sont identiques (évite faux positifs)
        6. Conserve la fiche avec le meilleur score BAN

    Paramètre :
        df : DataFrame avec colonnes source_fichier, code_postal, num_voie,
             nom_voie, score_ban, source_id

    Retourne (df_dedup, df_doublons_detectes).

    Faux positifs possibles :
        Deux adresses différentes dans la même rue avec le même numéro mais
        des compléments distincts (ex: 15 Rue de la Paix Bât A et Bât B).
        Mitigation : ajouter une vérification sur le complement si disponible.
    """
    crm_mask = df["source_fichier"].str.contains("crm", case=False, na=False)
    portail_mask = df["source_fichier"].str.contains("portail", case=False, na=False)

    df_crm = df[crm_mask].copy()
    df_portail = df[portail_mask].copy()

    print(
        f"\n  T15 — CRM : {len(df_crm):,} adresses | PORTAIL : {len(df_portail):,} adresses"
    )

    if df_crm.empty or df_portail.empty:
        print("  Aucune donnée CRM ou PORTAIL disponible.")
        return df, pd.DataFrame()

    doublons_detectes = []
    indices_supprimer = set()

    groupes_cp = set(df_crm["code_postal"].dropna()) & set(
        df_portail["code_postal"].dropna()
    )
    print(f"  Groupes CP communs à analyser : {len(groupes_cp):,}")

    for cp in groupes_cp:
        groupe_crm = df_crm[df_crm["code_postal"] == cp]
        groupe_portail = df_portail[df_portail["code_postal"] == cp]

        for _, row_crm in groupe_crm.iterrows():
            voie_crm = sans_accents(str(row_crm.get("nom_voie") or "").lower().strip())
            num_crm = str(row_crm.get("num_voie") or "").strip()

            for _, row_portail in groupe_portail.iterrows():
                voie_portail = sans_accents(
                    str(row_portail.get("nom_voie") or "").lower().strip()
                )
                num_portail = str(row_portail.get("num_voie") or "").strip()

                # Vérification 1 : numéros de voie identiques (anti faux-positifs)
                if num_crm != num_portail or not num_crm:
                    continue

                # Vérification 2 : Jaro-Winkler sur le nom de voie
                if not voie_crm or not voie_portail:
                    continue

                score_jw = JaroWinkler.similarity(voie_crm, voie_portail)
                if score_jw < SEUIL_JARO:
                    continue

                # Doublon détecté : garder la fiche avec le meilleur score BAN
                score_ban_crm = float(row_crm.get("score_ban") or 0)
                score_ban_portail = float(row_portail.get("score_ban") or 0)

                if score_ban_crm >= score_ban_portail:
                    idx_supprimer = row_portail.name
                    id_garder = row_crm["source_id"]
                else:
                    idx_supprimer = row_crm.name
                    id_garder = row_portail["source_id"]

                indices_supprimer.add(idx_supprimer)
                doublons_detectes.append(
                    {
                        "id_crm": row_crm["source_id"],
                        "voie_crm": str(row_crm.get("nom_voie") or ""),
                        "id_portail": row_portail["source_id"],
                        "voie_portail": str(row_portail.get("nom_voie") or ""),
                        "num_voie": num_crm,
                        "code_postal": cp,
                        "score_jaro_winkler": round(score_jw, 4),
                        "score_ban_crm": score_ban_crm,
                        "score_ban_portail": score_ban_portail,
                        "fiche_conservee": id_garder,
                    }
                )

    df_doublons = pd.DataFrame(doublons_detectes)
    df_dedup = df.drop(index=list(indices_supprimer))

    return df_dedup, df_doublons


def afficher_resultats(df_doublons: pd.DataFrame):
    """Affiche les résultats de la déduplication inter-sources."""
    print(f"\n  Doublons inter-sources détectés : {len(df_doublons):,}")

    if df_doublons.empty:
        print("  Aucun doublon détecté sur cet échantillon.")
        return

    print("\n  Top 5 des paires les plus similaires :")
    print(f"  {'id_crm':<15} {'id_portail':<15} {'voie_crm':<25} {'score_jw':>9}")
    print(f"  {'-' * 68}")
    top5 = df_doublons.nlargest(5, "score_jaro_winkler")
    for _, row in top5.iterrows():
        print(
            f"  {str(row['id_crm']):<15} "
            f"{str(row['id_portail']):<15} "
            f"{str(row['voie_crm'])[:25]:<25} "
            f"{row['score_jaro_winkler']:>9.4f}"
        )

    print("""
  Analyse des faux positifs :
  Risque principal : deux logements différents au même numéro de rue
  (ex: 15 Rue de la Paix Bât A et 15 Rue de la Paix Bât B) peuvent
  avoir un Jaro-Winkler = 1.0 sur le nom de voie si les compléments
  ne sont pas comparés.
  Mitigation appliquée : vérification que num_voie est identique.
  Mitigation supplémentaire recommandée : comparer aussi le complement
  et rejeter les paires où les compléments sont différents et non vides.
    """)


def main():
    print("\nT15 — DÉDUPLICATION INTER-SOURCES — Projet ADREA — ImmoPro France SAS")

    if not DUCKDB_PATH.exists():
        print(f"  DuckDB introuvable ({DUCKDB_PATH}). Lancez d'abord le pipeline ETL.")
        return

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    # 3.1 Pré-filtrage SQL
    df_paires = prefiltrage_sql(con)

    # 3.2 Déduplication sur un sous-ensemble 500 CRM + 500 PORTAIL
    print("\n  3.2 — Déduplication fuzzy sur sous-ensemble (500 CRM + 500 PORTAIL)")
    # UNION ALL avec LIMIT : chaque branche doit être une sous-requête
    df_sample = con.execute(f"""
        SELECT * FROM (
            SELECT source_id, source_fichier, num_voie, nom_voie,
                   code_postal, score_ban
            FROM {TABLE}
            WHERE lower(source_fichier) LIKE '%crm%'
            LIMIT 500
        )
        UNION ALL
        SELECT * FROM (
            SELECT source_id, source_fichier, num_voie, nom_voie,
                   code_postal, score_ban
            FROM {TABLE}
            WHERE lower(source_fichier) LIKE '%portail%'
            LIMIT 500
        )
    """).df()

    con.close()

    df_dedup, df_doublons = t15_dedup_inter_sources(df_sample)
    afficher_resultats(df_doublons)

    if not df_doublons.empty:
        df_doublons.to_csv("output/doublons_inter_sources.csv", index=False)
        print("  Doublons exportés : output/doublons_inter_sources.csv")


if __name__ == "__main__":
    _args = parse_args()
    DUCKDB_PATH = Path(_args.db)
    TABLE = _args.table
    main()
