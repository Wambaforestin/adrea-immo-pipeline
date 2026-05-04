"""
mesure_avant.py
Exercice 1 — Mesure de qualité AVANT les transformations T11 à T16.
Projet ADREA — ImmoPro France SAS

Établit la baseline de qualité sur la table referentiel_adresses.
Tous les chiffres produits ici seront comparés dans mesure_apres.py.

Usage :
    uv run python eval/mesure_avant.py
"""

import duckdb
import pandas as pd
from pathlib import Path

DUCKDB_PATH = Path("output/adrea_etl.duckdb")
TABLE = "referentiel_adresses"


def connecter() -> duckdb.DuckDBPyConnection:
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"Base DuckDB introuvable : {DUCKDB_PATH}\n"
            "Lancez d'abord : uv run python -m etl.pipeline_etl"
        )
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def afficher_separateur(titre: str):
    print(f"\n{'=' * 65}")
    print(f"  {titre}")
    print("=" * 65)


def afficher_ligne(label: str, valeur, total: int = None):
    if total and isinstance(valeur, (int, float)):
        pct = round(valeur / total * 100, 2) if total > 0 else 0
        print(f"  {label:<50} {valeur:>8,}  ({pct}%)")
    else:
        print(f"  {label:<50} {str(valeur):>10}")


# ---------------------------------------------------------------------------
# 1.1 Métriques de baseline (10 pts)
# ---------------------------------------------------------------------------


def mesures_baseline(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Calcule les 10 métriques de baseline demandées par l'exercice 1.1.
    Retourne un dictionnaire pour réutilisation dans mesure_apres.py.
    """
    afficher_separateur("EXERCICE 1.1 — MÉTRIQUES DE BASELINE")

    metriques = {}

    # Nombre total de lignes
    nb_total = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    metriques["nb_total"] = nb_total
    afficher_ligne("Nombre total de lignes", nb_total)

    # % code_postal renseigné
    nb_cp = con.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE code_postal IS NOT NULL AND code_postal != ''"
    ).fetchone()[0]
    metriques["nb_cp_renseigne"] = nb_cp
    afficher_ligne("Lignes avec code_postal renseigné", nb_cp, nb_total)

    # % libelle_commune renseigné
    nb_commune = con.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE libelle_commune IS NOT NULL AND libelle_commune != ''"
    ).fetchone()[0]
    metriques["nb_commune"] = nb_commune
    afficher_ligne("Lignes avec libelle_commune renseignée", nb_commune, nb_total)

    # % num_voie non vide
    nb_num_voie = con.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE num_voie IS NOT NULL AND num_voie != ''"
    ).fetchone()[0]
    metriques["nb_num_voie"] = nb_num_voie
    afficher_ligne("Lignes avec num_voie non vide", nb_num_voie, nb_total)

    # % adresse_norm contenant au moins 3 mots
    # nom_voie correspond à adresse_principale dans notre schéma
    nb_3mots = con.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE array_length(string_split(trim(nom_voie), ' ')) >= 3
    """).fetchone()[0]
    metriques["nb_adresse_3mots"] = nb_3mots
    afficher_ligne("Lignes avec nom_voie >= 3 mots", nb_3mots, nb_total)

    # Nombre de code_postal distincts
    nb_cp_distincts = con.execute(
        f"SELECT COUNT(DISTINCT code_postal) FROM {TABLE} WHERE code_postal != ''"
    ).fetchone()[0]
    metriques["nb_cp_distincts"] = nb_cp_distincts
    afficher_ligne("Codes postaux distincts", nb_cp_distincts)

    # Top 10 des type_voie les plus fréquents
    print(f"\n  {'Top 10 des type_voie les plus fréquents':}")
    print(f"  {'-' * 50}")
    top_type_voie = con.execute(f"""
        SELECT type_voie, COUNT(*) AS nb
        FROM {TABLE}
        WHERE type_voie IS NOT NULL AND type_voie != ''
        GROUP BY type_voie
        ORDER BY nb DESC
        LIMIT 10
    """).fetchall()
    metriques["top_type_voie"] = top_type_voie
    for rang, (tv, nb) in enumerate(top_type_voie, 1):
        print(f"  {rang:>2}. {str(tv):<30} {nb:>8,}")

    # Lignes où nom_voie contient encore un chiffre
    # (numéro mal extrait par T06, resté dans le libellé)
    nb_chiffre_voie = con.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE regexp_matches(nom_voie, '\\d')
    """).fetchone()[0]
    metriques["nb_chiffre_dans_voie"] = nb_chiffre_voie
    afficher_ligne("nom_voie contenant encore un chiffre", nb_chiffre_voie, nb_total)

    # Lignes où nom_voie contient encore un point (abréviation non développée)
    nb_point_voie = con.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE nom_voie LIKE '%.%'
    """).fetchone()[0]
    metriques["nb_point_dans_voie"] = nb_point_voie
    afficher_ligne("nom_voie contenant encore un point", nb_point_voie, nb_total)

    # Lignes dont le CP ne correspond à aucune commune dans ref_hexasmal
    # On vérifie la cohérence CP vs libelle_commune : si libelle_commune est vide
    # alors que le CP est renseigné, c'est que le CP est inconnu du référentiel
    nb_cp_sans_commune = con.execute(f"""
        SELECT COUNT(*) FROM {TABLE}
        WHERE code_postal IS NOT NULL
          AND code_postal != ''
          AND (libelle_commune IS NULL OR libelle_commune = '')
    """).fetchone()[0]
    metriques["nb_cp_sans_commune"] = nb_cp_sans_commune
    afficher_ligne(
        "CP renseigné mais commune absente (CP inconnu)", nb_cp_sans_commune, nb_total
    )

    return metriques


# ---------------------------------------------------------------------------
# 1.2 Identification des problèmes résiduels (10 pts)
# ---------------------------------------------------------------------------


def problemes_residuels(con: duckdb.DuckDBPyConnection):
    """
    Affiche 20 exemples de lignes problématiques que T01-T10 ne corrige pas.
    Classées en 4 catégories distinctes.
    """
    afficher_separateur("EXERCICE 1.2 — PROBLÈMES RÉSIDUELS (20 exemples)")

    # Catégorie 1 : num_voie contient l'adresse complète (T06 a échoué)
    # Cause : adresse_ligne1 commençait par un type de voie sans numéro
    # -> T06 met tout dans num_voie au lieu de libelle_voie
    print(
        "\n  CATÉGORIE 1 — num_voie aberrant (numéro > 4 chiffres ou contient des lettres en tête)"
    )
    print(f"  {'source_id':<15} {'num_voie':<20} {'nom_voie':<30}")
    print(f"  {'-' * 67}")
    rows = con.execute(f"""
        SELECT source_id, num_voie, nom_voie, source_fichier
        FROM {TABLE}
        WHERE num_voie IS NOT NULL
          AND (length(regexp_replace(num_voie, '[^0-9]', '', 'g')) > 4
               OR regexp_matches(num_voie, '^[A-Za-z]'))
        LIMIT 5
    """).fetchall()
    for r in rows:
        # Problème : T06 extrait des numéros aberrants (>9999 ou commençant par lettre)
        print(f"  {str(r[0]):<15} {str(r[1]):<20} {str(r[2])[:30]:<30}")

    # Catégorie 2 : adresse_principale vide (T07 a trop découpé)
    # Cause : le mot-clé de complément était en début de libellé (ex: "Résidence les Pins")
    # -> T07 isole tout comme complément, ne laisse rien dans adresse_principale
    print(
        "\n  CATÉGORIE 2 — nom_voie vide (T07 a isolé tout le libellé comme complément)"
    )
    print(f"  {'source_id':<15} {'nom_voie':<15} {'complement':<35}")
    print(f"  {'-' * 67}")
    rows = con.execute(f"""
        SELECT source_id, nom_voie, complement, source_fichier
        FROM {TABLE}
        WHERE (nom_voie IS NULL OR nom_voie = '')
          AND complement IS NOT NULL AND complement != ''
        LIMIT 5
    """).fetchall()
    for r in rows:
        # Problème : T07 trop agressif sur les mots-clés en tête d'adresse
        print(f"  {str(r[0]):<15} {str(r[1]):<15} {str(r[2])[:35]:<35}")

    # Catégorie 3 : score BAN très bas malgré CP valide
    # Cause : adresse synthétique ou mal formée après T01-T07
    # -> la BAN ne reconnaît pas l'adresse envoyée
    print("\n  CATÉGORIE 3 — Score BAN faible (< 0.3) avec CP valide")
    print(f"  {'source_id':<15} {'nom_voie':<30} {'cp':<8} {'score':>7}")
    print(f"  {'-' * 62}")
    rows = con.execute(f"""
        SELECT source_id, nom_voie, code_postal, score_ban, source_fichier
        FROM {TABLE}
        WHERE score_ban < 0.3
          AND code_postal IS NOT NULL AND length(code_postal) = 5
          AND nom_voie IS NOT NULL AND nom_voie != ''
        ORDER BY score_ban ASC
        LIMIT 5
    """).fetchall()
    for r in rows:
        # Problème : T03 n'a pas développé toutes les abréviations, ou
        # T01 a mal normalisé la casse -> BAN ne reconnaît pas
        print(f"  {str(r[0]):<15} {str(r[1])[:30]:<30} {str(r[2]):<8} {r[3]:>7.3f}")

    # Catégorie 4 : CP valide mais libelle_commune vide
    # Cause : le CP n'existe pas dans Hexasmal (DOM/TOM, CEDEX, CP fictif)
    # -> T05 n'a trouvé aucune correspondance dans le COG
    print(
        "\n  CATÉGORIE 4 — CP valide mais commune non normalisée (absent du COG/Hexasmal)"
    )
    print(f"  {'source_id':<15} {'code_postal':<14} {'libelle_commune':<25} {'source'}")
    print(f"  {'-' * 67}")
    rows = con.execute(f"""
        SELECT source_id, code_postal, libelle_commune, source_fichier
        FROM {TABLE}
        WHERE code_postal IS NOT NULL AND length(code_postal) = 5
          AND (libelle_commune IS NULL OR libelle_commune = '')
        LIMIT 5
    """).fetchall()
    for r in rows:
        # Problème : CP de CEDEX, zone industrielle ou DOM/TOM non couvert par Hexasmal
        print(f"  {str(r[0]):<15} {str(r[1]):<14} {str(r[2]):<25} {str(r[3])}")

    print("\n  SYNTHÈSE DES 4 CATÉGORIES DE PROBLÈMES RÉSIDUELS :")
    print("  1. num_voie aberrant    : T06 extrait des numéros >9999 ou alphanum")
    print("  2. nom_voie vide        : T07 isole des types de voie comme compléments")
    print("  3. Score BAN faible     : adresse mal reconstruite après T01-T07")
    print("  4. CP sans commune      : CP hors référentiel Hexasmal (CEDEX, DOM/TOM)")


def sauvegarder_baseline(metriques: dict):
    """Exporte la baseline en CSV pour comparaison dans mesure_apres.py."""
    df = pd.DataFrame(
        [
            {"metrique": k, "valeur_avant": v}
            for k, v in metriques.items()
            if isinstance(v, (int, float))
        ]
    )
    path = Path("output/baseline_avant.csv")
    df.to_csv(path, index=False)
    print(f"\n  Baseline exportée : {path}")


def main():
    print("\nMESURE QUALITÉ AVANT — Projet ADREA — ImmoPro France SAS")

    con = connecter()
    metriques = mesures_baseline(con)
    problemes_residuels(con)
    sauvegarder_baseline(metriques)
    con.close()

    print("\nBaseline établie. Lancez maintenant :")
    print("  uv run python eval/transformations_v2.py")


if __name__ == "__main__":
    main()
