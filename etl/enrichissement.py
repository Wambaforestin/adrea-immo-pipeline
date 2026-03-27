"""
enrichissement.py
T09 - Code RIVOLI via le fichier FANTOIR (DGFiP)
T10 - Déduplication fuzzy Jaro-Winkler inter et intra-fichiers.
Projet ADREA - ImmoPro France SAS

Stratégie grands volumes :
    T09 : Le fichier FANTOIR est chargé une seule fois en mémoire dans un
          dictionnaire {(code_insee, libelle_voie_norm): code_rivoli}.
          La jointure se fait par lookup O(1) sans merge pandas sur 820 000 lignes.

    T10 : La comparaison par paires est limitée au périmètre d'un même code
          postal (partitionnement). Sans cela, comparer toutes les paires de
          1 274 000 adresses serait O(n²) = ~800 milliards de comparaisons.
          Avec le partitionnement par CP, chaque groupe contient en moyenne
          ~250 adresses -> coût raisonnable.
"""

import re
import unicodedata
from pathlib import Path

import pandas as pd
from rapidfuzz.distance import JaroWinkler

REFERENTIELS_DIR = Path("referentiels")

SEUIL_JARO_WINKLER = 0.92

# Cache FANTOIR chargé une seule fois
_fantoir_cache: dict | None = None


# ---------------------------------------------------------------------------
# T09 - Code RIVOLI FANTOIR
# ---------------------------------------------------------------------------

def normaliser_libelle_voie_fantoir(libelle: str) -> str:
    """
    Normalise un libellé de voie pour la jointure FANTOIR.

    Transformations :
        - Passage en minuscules
        - Suppression des accents (unicodedata)
        - Suppression des caractères non alphanumériques (sauf espaces)
        - Suppression des articles et prépositions en début de libellé
          car le FANTOIR les supprime dans son indexation

    Exemple :
        "Rue de la République" -> "republique"
        "Avenue des Marronniers" -> "marronniers"
    """
    if not libelle or not isinstance(libelle, str):
        return ""

    # Minuscules
    s = libelle.strip().lower()

    # Suppression des accents
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")

    # Suppression des caractères non alphanumériques (conserve espaces)
    s = re.sub(r"[^a-z0-9\s]", " ", s)

    # Suppression des mots vides en tête (type de voie + articles)
    mots_vides = {
        "rue", "avenue", "boulevard", "impasse", "allee", "chemin",
        "route", "square", "place", "passage", "voie", "residence",
        "hameau", "lotissement", "domaine", "sentier", "cite",
        "de", "du", "des", "le", "la", "les", "l", "d",
        "en", "au", "aux", "sur", "sous", "par",
    }
    mots = s.split()
    mots_filtres = [m for m in mots if m not in mots_vides]

    return " ".join(mots_filtres).strip()


def charger_fantoir() -> dict:
    """
    Charge le fichier FANTOIR dans un dictionnaire de lookup.

    Format FANTOIR DGFiP :
        Fichier fixe (non CSV) ou CSV selon la version téléchargée.
        On supporte les deux formats courants :
            - CSV avec colonnes : code_commune, libelle_voie, code_rivoli
            - Fichier fixe FANTOIR (positions fixes par ligne)

    Retourne {(code_insee, libelle_norm): code_rivoli}.
    Met en cache pour éviter les relectures.
    """
    global _fantoir_cache
    if _fantoir_cache is not None:
        return _fantoir_cache

    fantoir_path = REFERENTIELS_DIR / "fantoir.csv"
    if not fantoir_path.exists():
        print(
            f"  ATTENTION : {fantoir_path} introuvable. "
            "Téléchargez FANTOIR sur "
            "https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/"
        )
        _fantoir_cache = {}
        return _fantoir_cache

    try:
        df = pd.read_csv(fantoir_path, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  ATTENTION : impossible de lire {fantoir_path} -> {e}")
        _fantoir_cache = {}
        return _fantoir_cache

    # Détecter les colonnes utiles (noms variables selon la source)
    cols = {c.upper().strip(): c for c in df.columns}

    col_insee = None
    col_libelle = None
    col_rivoli = None

    for candidat in ("CODE_COMMUNE", "CODE_INSEE", "CODECOMMUNE", "INSEE"):
        if candidat in cols:
            col_insee = cols[candidat]
            break

    for candidat in ("LIBELLE_VOIE", "LIBELLE", "NOM_VOIE", "NOMVOIE"):
        if candidat in cols:
            col_libelle = cols[candidat]
            break

    for candidat in ("CODE_RIVOLI", "RIVOLI", "CODE_VOIE", "CODEVOIE"):
        if candidat in cols:
            col_rivoli = cols[candidat]
            break

    if not all([col_insee, col_libelle, col_rivoli]):
        print(
            f"  ATTENTION : colonnes FANTOIR non reconnues. "
            f"Colonnes trouvées : {list(df.columns)}"
        )
        _fantoir_cache = {}
        return _fantoir_cache

    lookup = {}
    for _, row in df.iterrows():
        insee = str(row[col_insee]).strip().zfill(5)
        libelle = str(row[col_libelle]).strip()
        rivoli = str(row[col_rivoli]).strip()
        cle = (insee, normaliser_libelle_voie_fantoir(libelle))
        lookup[cle] = rivoli

    _fantoir_cache = lookup
    print(f"  FANTOIR chargé : {len(lookup):,} voies indexées")
    return _fantoir_cache


def joindre_fantoir(df: pd.DataFrame) -> pd.DataFrame:
    """
    T09 - Enrichit le DataFrame avec le code RIVOLI issu du fichier FANTOIR.

    Prérequis : les colonnes 'code_insee' et 'libelle_voie' doivent être présentes
    (produites par T05 et T06).

    Crée la colonne 'code_rivoli'. Valeur vide si la voie n'est pas trouvée dans
    le FANTOIR (cas fréquents : ZAC, ZI, voies privées -> risque R2 du cahier).
    """
    fantoir = charger_fantoir()

    if not fantoir:
        df["code_rivoli"] = ""
        return df

    def lookup_rivoli(row) -> str:
        insee = str(row.get("code_insee", "")).strip().zfill(5)
        libelle = str(row.get("libelle_voie", "")).strip()
        cle = (insee, normaliser_libelle_voie_fantoir(libelle))
        return fantoir.get(cle, "")

    df["code_rivoli"] = df.apply(lookup_rivoli, axis=1)

    nb_total = len(df)
    nb_trouves = int(df["code_rivoli"].ne("").sum())
    print(
        f"  T09 FANTOIR : {nb_trouves:,}/{nb_total:,} codes RIVOLI trouvés "
        f"({round(nb_trouves/nb_total*100, 2)}%)"
    )
    return df


# ---------------------------------------------------------------------------
# T10 - Déduplication fuzzy Jaro-Winkler
# ---------------------------------------------------------------------------

def _construire_adresse_normalisee_dedup(row: pd.Series) -> str:
    """
    Construit une chaîne d'adresse normalisée pour la comparaison Jaro-Winkler.

    Concatène : num_voie + type_voie + libelle_voie (après T01-T06).
    On exclut le CP et la commune car ils sont déjà utilisés pour le partitionnement.
    Les accents sont supprimés pour que "General" et "Général" soient reconnus doublons.
    """
    parties = []
    for col in ("num_voie", "type_voie_norm", "libelle_voie"):
        valeur = str(row.get(col, "")).strip().lower()
        if valeur:
            valeur = unicodedata.normalize("NFD", valeur)
            valeur = "".join(c for c in valeur if unicodedata.category(c) != "Mn")
            parties.append(valeur)
    return " ".join(parties)


def _sans_accents(texte: str) -> str:
    """Supprime les accents d'une chaîne pour la comparaison fuzzy."""
    n = unicodedata.normalize("NFD", texte)
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


def trouver_doublons_dans_groupe(adresses: list[str]) -> list[tuple[int, int, float]]:
    """
    Trouve les paires de doublons dans une liste d'adresses normalisées.

    Compare toutes les paires (O(n²) dans le groupe, acceptable car les groupes
    par code postal sont petits - quelques centaines de lignes max).

    Les accents sont supprimés avant comparaison pour que "General" et "Général"
    soient reconnus comme doublons (robustesse R6 du cahier des charges).

    Retourne une liste de tuples (index_i, index_j, score) pour les paires
    dont le score Jaro-Winkler est >= SEUIL_JARO_WINKLER.
    """
    doublons = []
    n = len(adresses)
    adresses_norm = [_sans_accents(a.lower()) if a else "" for a in adresses]
    for i in range(n):
        for j in range(i + 1, n):
            if not adresses_norm[i] or not adresses_norm[j]:
                continue
            score = JaroWinkler.similarity(adresses_norm[i], adresses_norm[j])
            if score >= SEUIL_JARO_WINKLER:
                doublons.append((i, j, score))
    return doublons


def dedupliquer(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    T10 - Détecte et fusionne les doublons fuzzy par groupe de code postal.

    Algorithme :
        1. Partitionner le DataFrame par 'cp_clean' (partitionnement clé pour
           limiter la complexité O(n²) à l'intérieur de chaque groupe)
        2. Dans chaque groupe, construire une adresse normalisée pour la comparaison
        3. Comparer toutes les paires avec Jaro-Winkler
        4. Pour chaque groupe de doublons : conserver la fiche canonique
           (critère : score BAN le plus élevé, sinon la ligne la plus récente)
        5. Marquer les doublons éliminés dans un DataFrame séparé

    Retourne (df_dedup, df_doublons_elimines).

    Stratégie grands volumes :
        Le partitionnement par CP est indispensable. Sans lui, 1 274 000 adresses
        produiraient ~810 milliards de paires à comparer.
        Avec ~8 000 codes postaux distincts et ~160 adresses par CP en moyenne,
        on tombe à ~160²/2 * 8 000 = ~100 millions de comparaisons, faisable.
    """
    if "cp_clean" not in df.columns:
        print("  T10 : colonne cp_clean absente, déduplication ignorée.")
        return df, pd.DataFrame()

    # Construire la colonne de comparaison
    df = df.copy()
    df["_adresse_dedup"] = df.apply(_construire_adresse_normalisee_dedup, axis=1)

    indices_a_supprimer = set()
    doublons_log = []

    groupes = df.groupby("cp_clean", sort=False)
    nb_groupes = len(groupes)
    nb_doublons_total = 0

    for cp, groupe in groupes:
        if len(groupe) < 2:
            continue

        adresses = groupe["_adresse_dedup"].tolist()
        indices_groupe = groupe.index.tolist()

        paires = trouver_doublons_dans_groupe(adresses)
        if not paires:
            continue

        # Construire les groupes de doublons (union-find simplifié)
        # Chaque doublon trouvé relie deux indices
        groupes_doublons: list[set] = []
        for i, j, score in paires:
            idx_i = indices_groupe[i]
            idx_j = indices_groupe[j]
            # Chercher si l'un des deux indices est déjà dans un groupe existant
            groupe_existant = None
            for g in groupes_doublons:
                if idx_i in g or idx_j in g:
                    g.add(idx_i)
                    g.add(idx_j)
                    groupe_existant = g
                    break
            if groupe_existant is None:
                groupes_doublons.append({idx_i, idx_j})

        # Pour chaque groupe de doublons : garder la fiche canonique
        for groupe_idx in groupes_doublons:
            sous_df = df.loc[list(groupe_idx)]

            # Critère 1 : score BAN le plus élevé
            if "result_score" in sous_df.columns:
                idx_canonique = sous_df["result_score"].fillna(0).idxmax()
            else:
                idx_canonique = sous_df.index[0]

            # Marquer les autres comme doublons à supprimer
            for idx in groupe_idx:
                if idx != idx_canonique:
                    indices_a_supprimer.add(idx)
                    nb_doublons_total += 1
                    doublons_log.append({
                        "idx_supprime": idx,
                        "idx_canonique": idx_canonique,
                        "adresse_supprimee": df.loc[idx, "_adresse_dedup"],
                        "adresse_canonique": df.loc[idx_canonique, "_adresse_dedup"],
                        "cp": cp,
                    })

    print(
        f"  T10 Déduplication : {nb_doublons_total:,} doublons supprimés "
        f"sur {len(df):,} adresses "
        f"({round(nb_doublons_total/len(df)*100, 2)}%) "
        f"| {nb_groupes:,} groupes CP analysés"
    )

    df_dedup = df.drop(index=list(indices_a_supprimer)).drop(columns=["_adresse_dedup"])
    df_doublons = pd.DataFrame(doublons_log)

    return df_dedup, df_doublons