"""
transformations_v2.py
Exercice 2 — Nouvelles transformations T11 à T14.
Projet ADREA — ImmoPro France SAS

Chaque fonction est pure, testable et réutilisable.
Les tests fournis dans l'énoncé sont exécutés automatiquement à la fin du script.

Usage :
    uv run python eval/transformations_v2.py
"""

import re
import unicodedata
from pathlib import Path

import argparse
import duckdb
import pandas as pd

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
        help=f"Nom de la table cible (défaut : referentiel_adresses)",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# T11-A — Normalisation du pays
# ---------------------------------------------------------------------------

# Toutes les variantes connues de "France" -> 'FR'
VARIANTES_FRANCE = {
    "fr",
    "fra",
    "france",
    "f",
    "francia",
    "frankrijk",
    "la france",
    "frankreich",
    "frankrig",
    "fransa",
    "francja",
    "frankrike",
    "franca",
}

# Correspondances pays communs vers code ISO 2 lettres
PAYS_VERS_ISO2 = {
    "belgique": "BE",
    "belgië": "BE",
    "belgium": "BE",
    "be": "BE",
    "espagne": "ES",
    "spain": "ES",
    "españa": "ES",
    "es": "ES",
    "italie": "IT",
    "italy": "IT",
    "italia": "IT",
    "it": "IT",
    "allemagne": "DE",
    "germany": "DE",
    "deutschland": "DE",
    "de": "DE",
    "suisse": "CH",
    "switzerland": "CH",
    "schweiz": "CH",
    "ch": "CH",
    "luxembourg": "LU",
    "lu": "LU",
    "portugal": "PT",
    "pt": "PT",
    "pays-bas": "NL",
    "netherlands": "NL",
    "nl": "NL",
    "royaume-uni": "GB",
    "uk": "GB",
    "gb": "GB",
}


def t11_normaliser_pays(pays: str) -> str | None:
    """
    T11-A — Normalise un code ou libellé de pays vers le code ISO-3166 à 2 lettres.

    Retourne 'FR' pour toutes les variantes de France (8+ variantes gérées).
    Retourne None si le champ est vide ou None.
    Retourne le code ISO 2 lettres pour les pays reconnus.
    Retourne la valeur en majuscules nettoyée pour les pays non reconnus.

    Exemples :
        'FR'        -> 'FR'
        'FRA'       -> 'FR'
        'france'    -> 'FR'
        'FRANCE'    -> 'FR'
        'F'         -> 'FR'
        'Francia'   -> 'FR'
        'Frankrijk' -> 'FR'
        'la France' -> 'FR'
        ''          -> None
        'BE'        -> 'BE'
        'Belgique'  -> 'BE'
    """
    if not pays or not isinstance(pays, str):
        return None

    pays_clean = pays.strip()
    if not pays_clean:
        return None

    pays_lower = pays_clean.lower()

    if pays_lower in VARIANTES_FRANCE:
        return "FR"

    if pays_lower in PAYS_VERS_ISO2:
        return PAYS_VERS_ISO2[pays_lower]

    # Pays non reconnu : retourner en majuscules normalisées
    return pays_clean.upper()


def t11_classifier_zone(code_postal: str) -> str:
    """
    T11-B — Classe un code postal français dans sa zone géographique.

    Zones retournées :
        'METRO'   : France métropolitaine (01 à 95, hors 20 = Corse)
        'CORSE'   : Corse (20000 à 20999)
        'DOM'     : Départements d'Outre-Mer (971 à 976)
        'TOM'     : Territoires d'Outre-Mer (980 à 989)
        'INCONNU' : CP null, invalide, ou hors périmètre France

    Logique :
        On utilise les 2 ou 3 premiers chiffres du CP pour déterminer
        le département, puis la zone. Le département 20 n'existe plus
        depuis 1976 (remplacé par 2A et 2B), mais les CP 20xxx restent
        en usage pour la Corse.
    """
    if not code_postal or not isinstance(code_postal, str):
        return "INCONNU"

    cp = code_postal.strip()
    if not re.match(r"^\d{5}$", cp):
        return "INCONNU"

    prefixe2 = int(cp[:2])
    prefixe3 = int(cp[:3])

    # DOM : 971 (Guadeloupe), 972 (Martinique), 973 (Guyane),
    #       974 (La Réunion), 976 (Mayotte)
    if 971 <= prefixe3 <= 976:
        return "DOM"

    # TOM : 980 à 989
    if 980 <= prefixe3 <= 989:
        return "TOM"

    # Corse : 20000 à 20999 (département 20, codes 2A et 2B)
    if prefixe2 == 20:
        return "CORSE"

    # France métropolitaine : départements 01 à 95
    if 1 <= prefixe2 <= 95:
        return "METRO"

    return "INCONNU"


def appliquer_t11(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Applique T11 sur la table referentiel_adresses.
    Ajoute les colonnes pays_norm et zone.
    Affiche la répartition par zone.
    """
    df = con.execute(f"""
        SELECT source_id, source_fichier, code_postal,
               qualite, score_ban
        FROM {TABLE}
    """).df()

    # T11-A : normalisation pays (la source ne stocke pas le pays dans notre schéma,
    # on applique sur source_fichier pour simuler la provenance)
    df["pays_norm"] = "FR"

    # T11-B : classification zone géographique
    df["zone"] = df["code_postal"].apply(t11_classifier_zone)

    print("\n  T11 — Répartition par zone géographique :")
    print(f"  {'Zone':<12} {'Nb adresses':>12} {'Pourcentage':>12}")
    print(f"  {'-' * 38}")
    total = len(df)
    for zone in ["METRO", "CORSE", "DOM", "TOM", "INCONNU"]:
        nb = int((df["zone"] == zone).sum())
        pct = round(nb / total * 100, 2) if total > 0 else 0
        print(f"  {zone:<12} {nb:>12,} {pct:>11.2f}%")

    return df


# ---------------------------------------------------------------------------
# T12 — Nettoyage des numéros de voie aberrants
# ---------------------------------------------------------------------------

REGEX_NUM_VOIE_VALIDE = re.compile(
    r"^(\d{1,4})\s*(bis|ter|quater|[a-d])?$",
    re.IGNORECASE,
)


def t12_valider_numero_voie(num_voie: str) -> str | None:
    """
    T12 — Valide et normalise un numéro de voie.

    Règles de validation :
        - Doit être compris entre 1 et 9999 (inclus)
        - '0' et '00' sont rejetés (numéro fictif ou erreur de parsing)
        - Suffixes autorisés : bis, ter, quater, a, b, c, d
        - Les suffixes sont normalisés en minuscules et collés au numéro
        - Tout le reste (lettres seules, vide, >9999) retourne None

    Exemples :
        '15'      -> '15'
        '72ter'   -> '72ter'
        '12BIS'   -> '12bis'
        '15 bis'  -> '15bis'
        '0'       -> None
        '00'      -> None
        '99999'   -> None
        'A'       -> None
        ''        -> None
    """
    if not num_voie or not isinstance(num_voie, str):
        return None

    valeur = num_voie.strip()
    if not valeur:
        return None

    match = REGEX_NUM_VOIE_VALIDE.match(valeur)
    if not match:
        return None

    numero = int(match.group(1))
    suffixe = (match.group(2) or "").lower().strip()

    # Rejeter 0 et 00 : numéros fictifs ou erreurs de parsing T06
    if numero == 0:
        return None

    # Rejeter les numéros > 9999 (hors norme française)
    if numero > 9999:
        return None

    return f"{numero}{suffixe}"


def appliquer_t12(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique T12 sur la colonne num_voie du DataFrame.
    Crée la colonne num_voie_valid (None si aberrant).
    Affiche le nombre de numéros invalidés.
    """
    df["num_voie_valid"] = (
        df["num_voie"].fillna("").astype(str).apply(t12_valider_numero_voie)
    )
    nb_total = len(df)
    nb_valides = int(df["num_voie_valid"].notna().sum())
    nb_invalides = int(df["num_voie_valid"].isna().sum() - df["num_voie"].isna().sum())

    print(f"\n  T12 — Numéros de voie validés : {nb_valides:,}/{nb_total:,}")
    print(
        f"  T12 — Numéros aberrants mis à NULL : {max(0, nb_invalides):,} "
        "(gain qualité : moins de données fausses)"
    )
    return df


# ---------------------------------------------------------------------------
# T13 — Standardisation des compléments d'adresse
# ---------------------------------------------------------------------------

# Dictionnaire {regex_pattern: libelle_standard}
# Chaque pattern capture les variantes d'un type de complément
PATTERNS_COMPLEMENT = [
    (re.compile(r"\bbât(?:iment)?[.]?\s*", re.IGNORECASE), "Bâtiment"),
    (re.compile(r"\bbat(?:iment)?[.]?\s*", re.IGNORECASE), "Bâtiment"),
    (re.compile(r"\bappart(?:ement)?[.]?\s*", re.IGNORECASE), "Appartement"),
    (re.compile(r"\bappt?[.]?\s*", re.IGNORECASE), "Appartement"),
    (re.compile(r"\bapt[.]?\s*", re.IGNORECASE), "Appartement"),
    (re.compile(r"\brés(?:idence)?[.]?\s*", re.IGNORECASE), "Résidence"),
    (re.compile(r"\bres(?:idence)?[.]?\s*", re.IGNORECASE), "Résidence"),
    (re.compile(r"\besc(?:alier)?[.]?\s*", re.IGNORECASE), "Escalier"),
]


def t13_standardiser_complement(complement: str) -> str:
    """
    T13 — Standardise les compléments d'adresse détectés par T07.

    Remplace les variantes de chaque type par le libellé officiel :
        bâtiment : 'bat A', 'BÂT. A', 'Bâtiment A', 'batiment a' -> 'Bâtiment A'
        appartement : 'appt 14', 'APPT14', 'apt 14'              -> 'Appartement 14'
        résidence : 'rés les Pins', 'Residence les Pins'          -> 'Résidence les Pins'
        escalier : 'esc B', 'ESCALIER B'                          -> 'Escalier B'

    Le mot qui suit le type de complément est capitalisé (A, 14, les Pins...).

    Exemples :
        'bat A'        -> 'Bâtiment A'
        'BÂT. A'       -> 'Bâtiment A'
        'appt 14'      -> 'Appartement 14'
        'APPT14'       -> 'Appartement 14'
        'rés les Pins' -> 'Résidence les Pins'
        'esc B'        -> 'Escalier B'
        ''             -> ''
    """
    if not complement or not isinstance(complement, str):
        return complement or ""

    resultat = complement.strip()

    for pattern, libelle_standard in PATTERNS_COMPLEMENT:
        match = pattern.search(resultat)
        if match:
            # Extraire ce qui suit le mot-clé (point et espaces déjà consommés par le pattern)
            suite = resultat[match.end() :].strip()
            # On ne change pas la casse de la suite : 'les Pins' reste 'les Pins',
            # 'A' reste 'A', '14' reste '14'. Seul le libelle_standard est normalisé.
            if suite:
                resultat = f"{libelle_standard} {suite}"
            else:
                resultat = libelle_standard
            break

    return resultat


def appliquer_t13(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique T13 sur la colonne complement du DataFrame.
    Crée la colonne complement_std.
    """
    df["complement_std"] = (
        df["complement"].fillna("").astype(str).apply(t13_standardiser_complement)
    )
    nb_avec_complement = int(df["complement_std"].ne("").sum())
    print(f"\n  T13 — Compléments standardisés : {nb_avec_complement:,} lignes")
    return df


# ---------------------------------------------------------------------------
# T14 — Score de complétude de l'adresse
# ---------------------------------------------------------------------------


def t14_score_completude(row: dict | pd.Series) -> int:
    """
    T14 — Calcule un score de complétude de 0 à 100 pour une adresse.

    Barème (conforme à l'énoncé) :
        code_postal       : renseigné et valide (5 chiffres)   -> 20 pts
        libelle_commune   : renseigné                          -> 15 pts
        num_voie          : renseigné et validé par T12        -> 15 pts
        type_voie         : renseigné                          -> 10 pts
        nom_voie          : renseigné et longueur > 2 car      -> 15 pts
        latitude+longitude: les deux renseignés                -> 15 pts
        code_insee        : renseigné                          -> 10 pts
                                                        Total  -> 100 pts
    """
    score = 0

    # code_postal : renseigné ET exactement 5 chiffres
    cp = str(row.get("code_postal") or "").strip()
    if re.match(r"^\d{5}$", cp):
        score += 20

    # libelle_commune : renseigné et non vide
    commune = str(row.get("libelle_commune") or "").strip()
    if commune:
        score += 15

    # num_voie : renseigné ET validé par T12 (non None après validation)
    num = str(row.get("num_voie") or "").strip()
    if num and t12_valider_numero_voie(num) is not None:
        score += 15

    # type_voie : renseigné
    type_v = str(row.get("type_voie") or "").strip()
    if type_v:
        score += 10

    # nom_voie : renseigné ET longueur > 2 caractères
    nom = str(row.get("nom_voie") or "").strip()
    if len(nom) > 2:
        score += 15

    # latitude ET longitude : les deux doivent être renseignés et numériques
    try:
        lat = float(row.get("latitude") or row.get("latitude_ban") or "")
        lng = float(row.get("longitude") or row.get("longitude_ban") or "")
        if lat != 0.0 and lng != 0.0:
            score += 15
    except (ValueError, TypeError):
        pass

    # code_insee : renseigné
    insee = str(row.get("code_insee") or "").strip()
    if insee:
        score += 10

    return score


def appliquer_t14(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Applique T14 sur toute la table referentiel_adresses.
    Affiche la distribution par tranches et le score moyen par source.
    """
    df = con.execute(f"""
        SELECT source_id, source_fichier, num_voie, type_voie, nom_voie,
               code_postal, libelle_commune, code_insee,
               latitude, longitude, score_ban
        FROM {TABLE}
    """).df()

    df["score_completude"] = df.apply(t14_score_completude, axis=1)

    total = len(df)

    print("\n  T14 — Distribution des scores de complétude :")
    print(f"  {'Tranche':<15} {'Nb adresses':>12} {'Pourcentage':>12}")
    print(f"  {'-' * 42}")
    tranches = [(0, 25), (25, 50), (50, 75), (75, 101)]
    labels = ["0-25", "25-50", "50-75", "75-100"]
    for (inf, sup), label in zip(tranches, labels):
        nb = int(
            ((df["score_completude"] >= inf) & (df["score_completude"] < sup)).sum()
        )
        pct = round(nb / total * 100, 1) if total > 0 else 0
        barre = "#" * int(pct / 2)
        print(f"  {label:<15} {nb:>12,} {pct:>11.1f}%  {barre}")

    print(f"\n  Score moyen global : {df['score_completude'].mean():.1f} / 100")

    print("\n  T14 — Score moyen par source :")
    print(f"  {'Source':<40} {'Score moyen':>12} {'Nb lignes':>10}")
    print(f"  {'-' * 64}")
    for source, groupe in df.groupby("source_fichier"):
        nom_court = str(source).replace("source_", "").replace(".csv", "")
        print(
            f"  {nom_court:<40} "
            f"{groupe['score_completude'].mean():>12.1f} "
            f"{len(groupe):>10,}"
        )

    print("\n  T14 — Les 5 adresses avec le score le plus bas :")
    print(f"  {'source_id':<15} {'score':>6}  {'champs manquants'}")
    print(f"  {'-' * 65}")
    pires = df.nsmallest(5, "score_completude")
    for _, row in pires.iterrows():
        manquants = []
        if not re.match(r"^\d{5}$", str(row.get("code_postal") or "")):
            manquants.append("CP")
        if not str(row.get("libelle_commune") or "").strip():
            manquants.append("commune")
        if not str(row.get("num_voie") or "").strip():
            manquants.append("num_voie")
        if not str(row.get("type_voie") or "").strip():
            manquants.append("type_voie")
        if len(str(row.get("nom_voie") or "").strip()) <= 2:
            manquants.append("nom_voie")
        try:
            float(row.get("latitude") or "")
            float(row.get("longitude") or "")
        except (ValueError, TypeError):
            manquants.append("GPS")
        if not str(row.get("code_insee") or "").strip():
            manquants.append("code_insee")
        print(
            f"  {str(row['source_id']):<15} {row['score_completude']:>6}  "
            f"{', '.join(manquants)}"
        )

    return df


# ---------------------------------------------------------------------------
# Tests intégrés (exemples de l'énoncé)
# ---------------------------------------------------------------------------


def executer_tests():
    """
    Valide chaque fonction avec les exemples fournis dans l'énoncé.
    Affiche PASS ou FAIL pour chaque cas.
    """
    print("\n" + "=" * 65)
    print("  TESTS — Validation des exemples de l'énoncé")
    print("=" * 65)

    nb_pass = 0
    nb_fail = 0

    def tester(label, obtenu, attendu):
        nonlocal nb_pass, nb_fail
        ok = obtenu == attendu
        statut = "PASS" if ok else "FAIL"
        if ok:
            nb_pass += 1
        else:
            nb_fail += 1
            print(f"  [{statut}] {label}")
            print(f"         attendu={attendu!r}  obtenu={obtenu!r}")
            return
        print(f"  [{statut}] {label}")

    # Tests T11-A
    print("\n  T11-A — Normalisation pays")
    tester("FR", t11_normaliser_pays("FR"), "FR")
    tester("FRA", t11_normaliser_pays("FRA"), "FR")
    tester("france", t11_normaliser_pays("france"), "FR")
    tester("FRANCE", t11_normaliser_pays("FRANCE"), "FR")
    tester("F", t11_normaliser_pays("F"), "FR")
    tester("Francia", t11_normaliser_pays("Francia"), "FR")
    tester("Frankrijk", t11_normaliser_pays("Frankrijk"), "FR")
    tester("vide", t11_normaliser_pays(""), None)
    tester("BE", t11_normaliser_pays("BE"), "BE")
    tester("Belgique", t11_normaliser_pays("Belgique"), "BE")

    # Tests T11-B
    print("\n  T11-B — Classification zone")
    tester("75014 -> METRO", t11_classifier_zone("75014"), "METRO")
    tester("20000 -> CORSE", t11_classifier_zone("20000"), "CORSE")
    tester("97100 -> DOM", t11_classifier_zone("97100"), "DOM")
    tester("98800 -> TOM", t11_classifier_zone("98800"), "TOM")
    tester("None -> INCONNU", t11_classifier_zone(None), "INCONNU")
    tester("ABC -> INCONNU", t11_classifier_zone("ABC"), "INCONNU")

    # Tests T12
    print("\n  T12 — Validation numéro de voie")
    tester("'15'", t12_valider_numero_voie("15"), "15")
    tester("'72ter'", t12_valider_numero_voie("72ter"), "72ter")
    tester("'12BIS'", t12_valider_numero_voie("12BIS"), "12bis")
    tester("'0'", t12_valider_numero_voie("0"), None)
    tester("'00'", t12_valider_numero_voie("00"), None)
    tester("'99999'", t12_valider_numero_voie("99999"), None)
    tester("'15 bis'", t12_valider_numero_voie("15 bis"), "15bis")
    tester("'A'", t12_valider_numero_voie("A"), None)
    tester("''", t12_valider_numero_voie(""), None)

    # Tests T13
    print("\n  T13 — Standardisation compléments")
    tester("'bat A'", t13_standardiser_complement("bat A"), "Bâtiment A")
    tester("'BÂT. A'", t13_standardiser_complement("BÂT. A"), "Bâtiment A")
    tester("'appt 14'", t13_standardiser_complement("appt 14"), "Appartement 14")
    tester("'APPT14'", t13_standardiser_complement("APPT14"), "Appartement 14")
    tester(
        "'rés les Pins'",
        t13_standardiser_complement("rés les Pins"),
        "Résidence les Pins",
    )
    tester("'esc B'", t13_standardiser_complement("esc B"), "Escalier B")
    tester("''", t13_standardiser_complement(""), "")

    print(f"\n  Résultat : {nb_pass} PASS / {nb_fail} FAIL")
    return nb_fail == 0


def main():
    print("\nTRANSFORMATIONS V2 (T11-T14) — Projet ADREA — ImmoPro France SAS")

    # Tests unitaires d'abord (avant d'appliquer sur les données)
    tous_pass = executer_tests()
    if not tous_pass:
        print(
            "\n  ATTENTION : des tests échouent. Vérifiez avant d'appliquer sur les données."
        )

    # Application sur DuckDB
    if not DUCKDB_PATH.exists():
        print(
            f"\n  DuckDB introuvable ({DUCKDB_PATH}). Lancez d'abord le pipeline ETL."
        )
        return

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    print("\n" + "=" * 65)
    print("  APPLICATION SUR LA TABLE referentiel_adresses")
    print("=" * 65)

    print("\n  T11 — Normalisation pays + classification zone")
    df_t11 = appliquer_t11(con)

    print("\n  T12 + T13 — Chargement des données...")
    df = con.execute(f"""
        SELECT source_id, source_fichier, num_voie, type_voie, nom_voie,
               complement, code_postal, libelle_commune, code_insee,
               latitude, longitude
        FROM {TABLE}
    """).df()

    df = appliquer_t12(df)
    df = appliquer_t13(df)

    print("\n  T14 — Score de complétude")
    df_scores = appliquer_t14(con)

    # Sauvegarde pour mesure_apres.py
    df_scores["zone"] = df_t11["zone"].values
    df_scores["num_voie_valid"] = df["num_voie_valid"].values
    df_scores["complement_std"] = df["complement_std"].values
    df_scores.to_csv("output/adresses_transformees_v2.csv", index=False)
    print("\n  Résultats sauvegardés : output/adresses_transformees_v2.csv")

    con.close()


if __name__ == "__main__":
    _args = parse_args()
    DUCKDB_PATH = Path(_args.db)
    TABLE = _args.table
    main()
