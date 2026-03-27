"""
transform.py
Livrable 2 - Règles de transformation T01 à T07.
Projet ADREA - ImmoPro France SAS

Chaque règle est une fonction pure et réutilisable.
Les fonctions qui s'appliquent colonne par colonne sont conçues pour
être appelées via df[col].apply(fonction) ou directement sur un DataFrame.

Stratégie grands volumes :
    Toutes les fonctions de transformation opèrent sur des valeurs scalaires
    ou des Series. Elles sont appelées depuis pipeline_etl.py qui gère
    la lecture par chunks pour ne jamais saturer la RAM.
"""

import re
from pathlib import Path

import pandas as pd
from rapidfuzz.distance import JaroWinkler

REFERENTIELS_DIR = Path("referentiels")

# ---------------------------------------------------------------------------
# T01 - Normalisation de la casse selon la typographie française des voies
# ---------------------------------------------------------------------------

# Mots qui restent en minuscule dans un libellé de voie (articles, prépositions)
MOTS_MINUSCULES = {
    "de", "du", "des", "le", "la", "les",
    "sur", "sous", "en", "par", "au", "aux",
    "et", "à", "l", "d",
}


def normaliser_casse(texte: str) -> str:
    """
    T01 - Normalise la casse d'un libellé de voie selon la typographie française.

    Règles :
    - Le premier mot prend toujours une majuscule initiale.
    - Les articles et prépositions (de, du, des, le, la...) restent en minuscule.
    - Tous les autres mots prennent une majuscule initiale.
    - La fonction .title() de Python ne fait pas ça correctement
      car elle met une majuscule après chaque apostrophe et tiret.

    Exemples :
        "RUE DU GENERAL DE GAULLE"  -> "Rue du Général de Gaulle"
        "AVENUE DE LA REPUBLIQUE"   -> "Avenue de la République"
        "impasse des lilas"         -> "Impasse des Lilas"
    """
    if not texte or not isinstance(texte, str):
        return texte

    texte = texte.strip()
    mots = texte.split()
    resultat = []

    for i, mot in enumerate(mots):
        mot_lower = mot.lower()
        if i == 0:
            # Le premier mot prend toujours une majuscule
            resultat.append(mot_lower.capitalize())
        elif mot_lower in MOTS_MINUSCULES:
            resultat.append(mot_lower)
        else:
            resultat.append(mot_lower.capitalize())

    return " ".join(resultat)


# ---------------------------------------------------------------------------
# T02 - Parsing du champ adresse_brute (source_agences_flux.csv)
# ---------------------------------------------------------------------------

# Dictionnaire des types de voie connus pour le parsing
TYPES_VOIE_CONNUS = [
    "impasse", "allée", "allee", "avenue", "boulevard", "chemin", "route",
    "rue", "square", "résidence", "residence", "hameau", "lotissement",
    "passage", "place", "quai", "villa", "voie", "domaine", "sentier",
    "cité", "cite", "ruelle", "esplanade", "promenade", "parvis",
]

# Regex du code postal français (5 chiffres consécutifs)
REGEX_CP_BRUT = re.compile(r"\b(\d{5})\b")

# Regex du numéro de voie en début de chaîne (ex: 12, 12bis, 72ter, 12B)
REGEX_NUM_VOIE = re.compile(r"^(\d+\s*(?:bis|ter|[a-zA-Z])?)[\s,]+", re.IGNORECASE)

# Regex du numéro de voie n'importe où dans la chaîne
REGEX_NUM_VOIE_MILIEU = re.compile(r"\b(\d+\s*(?:bis|ter|[a-zA-Z])?)\b", re.IGNORECASE)


def parser_adresse_brute(texte: str) -> dict:
    """
    T02 - Parse un champ adresse_brute monolithique et extrait les composants.

    Formats détectés (observés sur les données sources) :
        F1 : "86 Rue de la Liberté 78100 Saint-Germain-en-Laye"
             -> num en tête, type+nom voie, CP, commune en fin
        F2 : "Rue de la Liberté 86 78100 Saint-Germain-en-Laye"
             -> type voie en tête, num après le nom, CP, commune
        F3 : "86 Rue de la Liberté, 78100 Saint-Germain-en-Laye"
             -> idem F1 avec virgule comme séparateur
        F4 : "Saint-Germain-en-Laye (78100) 86 Rue de la Liberté"
             -> commune et CP en tête, adresse après
        F5 : "78100 Saint-Germain-en-Laye 86 Rue de la Liberté"
             -> CP, commune, puis adresse

    Stratégie : le code postal (5 chiffres) est le repère le plus stable.
    On l'extrait en premier, puis on reconstruit les parties gauche et droite.

    Retourne un dictionnaire avec les clés :
        num_voie, type_voie, nom_voie, code_postal, commune
        (valeurs vides "" si non trouvées)
    """
    resultat = {
        "num_voie": "",
        "type_voie": "",
        "nom_voie": "",
        "code_postal": "",
        "commune": "",
    }

    if not texte or not isinstance(texte, str):
        return resultat

    texte = texte.strip()

    # Étape 1 : extraire le code postal (repère le plus fiable)
    match_cp = REGEX_CP_BRUT.search(texte)
    if not match_cp:
        # Sans CP on tente quand même d'extraire le numéro de voie
        _extraire_num_et_voie(texte, resultat)
        return resultat

    code_postal = match_cp.group(1)
    resultat["code_postal"] = code_postal
    pos_cp = match_cp.start()

    # Étape 2 : tout ce qui est après le CP est potentiellement la commune
    partie_apres_cp = texte[match_cp.end():].strip().lstrip(",").strip()
    partie_avant_cp = texte[:pos_cp].strip().rstrip(",").strip()

    # Heuristique commune : si la partie après CP ressemble à un nom de ville
    # (pas de chiffres sauf arrondissement), c'est la commune
    if partie_apres_cp and not re.match(r"^\d+\s*(?:bis|ter)?", partie_apres_cp, re.IGNORECASE):
        resultat["commune"] = partie_apres_cp
        partie_adresse = partie_avant_cp
    else:
        # Format F4/F5 : commune avant le CP
        # On cherche si le début de partie_avant_cp ressemble à un nom de ville
        # en vérifiant si ça commence par une majuscule sans numéro
        if partie_avant_cp and not re.match(r"^\d", partie_avant_cp):
            # Chercher où commence la partie voie (type de voie connu)
            idx_type = _trouver_debut_type_voie(partie_avant_cp)
            if idx_type > 0:
                resultat["commune"] = partie_avant_cp[:idx_type].strip().rstrip(",")
                partie_adresse = partie_avant_cp[idx_type:].strip()
                if partie_apres_cp:
                    partie_adresse = partie_adresse + " " + partie_apres_cp
            else:
                resultat["commune"] = partie_apres_cp
                partie_adresse = partie_avant_cp
        else:
            resultat["commune"] = partie_apres_cp
            partie_adresse = partie_avant_cp

    # Étape 3 : extraire numéro, type et nom de voie depuis la partie adresse
    _extraire_num_et_voie(partie_adresse, resultat)

    return resultat


def _trouver_debut_type_voie(texte: str) -> int:
    """
    Retourne l'index où commence un type de voie connu dans le texte.
    Retourne 0 si le texte commence par un type de voie, -1 si non trouvé.
    """
    texte_lower = texte.lower()
    for type_voie in TYPES_VOIE_CONNUS:
        idx = texte_lower.find(type_voie)
        if idx >= 0:
            return idx
    return -1


def _extraire_num_et_voie(texte: str, resultat: dict):
    """
    Extrait le numéro de voie, le type de voie et le nom de voie depuis
    une chaîne d'adresse et les place dans le dictionnaire résultat.
    """
    if not texte:
        return

    texte = texte.strip()

    # Chercher le numéro de voie en début de chaîne
    match_num = REGEX_NUM_VOIE.match(texte)
    if match_num:
        resultat["num_voie"] = match_num.group(1).strip()
        reste = texte[match_num.end():].strip()
    else:
        reste = texte

    # Chercher le type de voie dans le reste
    reste_lower = reste.lower()
    for type_voie in TYPES_VOIE_CONNUS:
        if reste_lower.startswith(type_voie):
            resultat["type_voie"] = type_voie.capitalize()
            resultat["nom_voie"] = reste[len(type_voie):].strip()
            return

    # Si pas de numéro au début, chercher le type de voie n'importe où
    for type_voie in TYPES_VOIE_CONNUS:
        idx = reste_lower.find(type_voie)
        if idx >= 0:
            # Ce qui précède le type voie peut être le numéro
            avant = reste[:idx].strip()
            if not resultat["num_voie"] and avant:
                match_num2 = REGEX_NUM_VOIE_MILIEU.search(avant)
                if match_num2:
                    resultat["num_voie"] = match_num2.group(1).strip()
            resultat["type_voie"] = type_voie.capitalize()
            resultat["nom_voie"] = reste[idx + len(type_voie):].strip()
            return

    # Dernier recours : tout le reste est le nom de voie
    if reste:
        resultat["nom_voie"] = reste


def appliquer_parsing_flux(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique T02 sur le DataFrame des flux agences.
    Crée les colonnes num_voie, type_voie, nom_voie, code_postal_parse, commune_parse
    à partir du champ adresse_brute.

    Mesure et affiche le taux de parsing réussi (cible > 85%).
    """
    parsed = df["adresse_brute"].apply(parser_adresse_brute).apply(pd.Series)
    parsed.columns = [
        "num_voie_p", "type_voie_p", "nom_voie_p",
        "code_postal_p", "commune_p",
    ]
    df = pd.concat([df, parsed], axis=1)

    # Taux de parsing réussi : CP extrait ET type_voie non vide
    nb_total = len(df)
    nb_reussi = int(
        (df["code_postal_p"].ne("") & df["type_voie_p"].ne("")).sum()
    )
    taux = round(nb_reussi / nb_total * 100, 2) if nb_total > 0 else 0.0
    print(f"  T02 parsing adresse_brute : {nb_reussi:,}/{nb_total:,} = {taux}% (cible > 85%)")

    return df


# ---------------------------------------------------------------------------
# T03 - Expansion des abréviations de types de voie
# ---------------------------------------------------------------------------

ABBREV_VOIES = {
    "r.": "Rue",
    "r": "Rue",
    "rue": "Rue",
    "av.": "Avenue",
    "av": "Avenue",
    "ave": "Avenue",
    "bld": "Boulevard",
    "blvd": "Boulevard",
    "bd": "Boulevard",
    "bd.": "Boulevard",
    "boul.": "Boulevard",
    "imp.": "Impasse",
    "imp": "Impasse",
    "rte": "Route",
    "rte.": "Route",
    "all.": "Allée",
    "all": "Allée",
    "chem.": "Chemin",
    "chem": "Chemin",
    "sq.": "Square",
    "sq": "Square",
    "pl.": "Place",
    "pl": "Place",
    "res.": "Résidence",
    "rés.": "Résidence",
    "rés": "Résidence",
    "lot.": "Lotissement",
    "lot": "Lotissement",
    "ham.": "Hameau",
    "ham": "Hameau",
    "pass.": "Passage",
    "cité": "Cité",
    "cite": "Cité",
    "dom.": "Domaine",
    "dom": "Domaine",
}


def expand_abreviation_voie(type_voie: str) -> str:
    """
    T03 - Remplace une abréviation de type de voie par son libellé complet.

    La comparaison est insensible à la casse.
    Si l'abréviation n'est pas reconnue, retourne le type de voie tel quel
    après normalisation de casse (T01 appliqué).

    Exemples :
        "r."      -> "Rue"
        "av."     -> "Avenue"
        "bld"     -> "Boulevard"
        "Bld"     -> "Boulevard"
        "BLVD"    -> "Boulevard"
        "imp."    -> "Impasse"
        "rte"     -> "Route"
        "all."    -> "Allée"
        "chem."   -> "Chemin"
    """
    if not type_voie or not isinstance(type_voie, str):
        return type_voie
    cle = type_voie.strip().lower()
    return ABBREV_VOIES.get(cle, normaliser_casse(type_voie))


def appliquer_expansion_abreviations(df: pd.DataFrame, col_type_voie: str) -> pd.DataFrame:
    """
    Applique T03 sur la colonne de type de voie d'un DataFrame.
    Crée la colonne type_voie_norm à partir de col_type_voie.
    """
    df["type_voie_norm"] = df[col_type_voie].fillna("").astype(str).apply(
        expand_abreviation_voie
    )
    return df


# ---------------------------------------------------------------------------
# T04 - Nettoyage et validation du code postal
# ---------------------------------------------------------------------------

REGEX_CP_VALIDE = re.compile(r"^\d{5}$")


def nettoyer_code_postal(valeur: str) -> str:
    """
    T04 - Nettoie et valide un code postal français.

    Transformations appliquées dans l'ordre :
        1. Suppression des espaces et caractères non numériques
        2. Si le résultat a plus de 5 chiffres, on garde les 5 premiers
        3. Complétion à gauche avec des zéros si moins de 5 chiffres
        4. Validation finale : doit correspondre exactement à ^[0-9]{5}$
           -> retourne "" si invalide (la ligne ira en REJET)

    Exemples :
        "75 014"  -> "75014"   (espace supprimé)
        "750014"  -> "75001"   (tronqué aux 5 premiers chiffres)
        "75"      -> "00075"   (complété à gauche)
        "7501A"   -> ""        (invalide après nettoyage)
        ""        -> ""        (vide = invalide)
    """
    if not valeur or not isinstance(valeur, str):
        return ""

    # Supprimer tout ce qui n'est pas un chiffre
    chiffres = re.sub(r"[^\d]", "", valeur.strip())

    if not chiffres:
        return ""

    # Tronquer si trop long (plus de 5 chiffres)
    if len(chiffres) > 5:
        chiffres = chiffres[:5] # garder les 5 premiers chiffres. (ex: "750014" -> "75001", pas "00075")

    # Compléter à gauche avec des zéros si trop court
    chiffres = chiffres.zfill(5)

    # Validation finale
    if not REGEX_CP_VALIDE.match(chiffres):
        return ""

    return chiffres


def appliquer_nettoyage_cp(df: pd.DataFrame, col_cp: str) -> pd.DataFrame:
    """
    Applique T04 sur la colonne code postal d'un DataFrame.
    Crée la colonne cp_clean et cp_invalide (booléen).
    Affiche les comptages avant/après.
    """
    nb_avant = int(df[col_cp].fillna("").astype(str).apply(valider_cp_strict).sum())

    df["cp_clean"] = df[col_cp].fillna("").astype(str).apply(nettoyer_code_postal)
    df["cp_invalide"] = df["cp_clean"].eq("")

    nb_apres = int((~df["cp_invalide"]).sum())
    nb_total = len(df)
    nb_rejetes = int(df["cp_invalide"].sum())

    print(
        f"  T04 CP valides avant : {nb_avant:,}/{nb_total:,} "
        f"-> après : {nb_apres:,}/{nb_total:,} "
        f"| Rejets : {nb_rejetes:,}"
    )
    return df


def valider_cp_strict(valeur: str) -> bool:
    """Validation stricte sans correction : True si déjà au format ^[0-9]{5}$."""
    if not valeur or not isinstance(valeur, str):
        return False
    return bool(REGEX_CP_VALIDE.match(valeur.strip()))


# ---------------------------------------------------------------------------
# T05 - Normalisation de la ville via le COG INSEE 2024
# ---------------------------------------------------------------------------

# Cache du référentiel COG chargé une seule fois en mémoire
_cog_cache: pd.DataFrame | None = None


def _trouver_fichier_cog() -> Path | None:
    """
    Cherche le fichier COG INSEE dans referentiels/.
    Accepte n'importe quel nom contenant "communes" (communes2024.csv,
    communes2026.csv, communes.csv...).
    """
    for candidat in sorted(REFERENTIELS_DIR.glob("communes*.csv"), reverse=True):
        return candidat
    return None


def _trouver_fichier_hexasmal() -> Path | None:
    """
    Cherche le fichier Hexasmal dans referentiels/.
    Accepte : hexasmal.csv, 019HexaSmal.csv, HexaSmal.csv...
    Exclut le fichier enrichi (trop volumineux, >50 Mo).
    """
    candidats = list(REFERENTIELS_DIR.glob("*[Hh]exa*[Ss]mal*.csv"))
    candidats += list(REFERENTIELS_DIR.glob("hexasmal*.csv"))
    for chemin in candidats:
        # Ignorer le fichier enrichi (438 Mo) - on veut le fichier léger (1.6 Mo)
        if chemin.stat().st_size < 10_000_000:
            return chemin
    return None


def charger_cog_insee() -> pd.DataFrame:
    """
    Charge le COG INSEE et Hexasmal depuis referentiels/.

    Détection automatique du fichier COG (communes*.csv) et Hexasmal.
    Fonctionne avec COG 2024, 2025, 2026 sans changement de code.

    COG INSEE - colonnes utilisées :
        TYPECOM : filtre sur "COM" uniquement (exclut arrondissements, cantons)
        COM     : code INSEE commune (5 chiffres)
        LIBELLE : libellé officiel

    Hexasmal La Poste (019HexaSmal.csv) - colonnes utilisées :
        Code_commune_INSEE       : code INSEE pour la jointure avec COG
        Code_postal              : code postal (5 chiffres)
        Libellé_d_acheminement   : nom officiel d'acheminement La Poste
    """
    global _cog_cache
    if _cog_cache is not None:
        return _cog_cache

    cog_path = _trouver_fichier_cog()
    hexasmal_path = _trouver_fichier_hexasmal()

    if cog_path is None:
        print(
            "  ATTENTION : fichier COG INSEE introuvable dans referentiels/. "
            "Téléchargez communes*.csv sur https://www.insee.fr/fr/information/2560452"
        )
        return pd.DataFrame()

    print(f"  Chargement COG : {cog_path.name}")

    # Lecture du COG INSEE
    cog = pd.read_csv(cog_path, dtype=str, low_memory=False)

    # Filtrer sur TYPECOM = "COM" pour ne garder que les communes réelles
    # (exclut les arrondissements municipaux, les communes déléguées, etc.)
    if "TYPECOM" in cog.columns:
        cog = cog[cog["TYPECOM"] == "COM"].copy()

    # Détecter les colonnes code_insee et libelle
    col_mapping = {}
    for col in cog.columns:
        col_upper = col.upper().strip()
        if col_upper in ("COM", "CODECOM", "CODE_COM"):
            col_mapping["code_insee"] = col
        elif col_upper in ("LIBELLE", "NOM", "NCCENR", "LIBELLE_COM"):
            col_mapping["libelle"] = col

    if "code_insee" not in col_mapping or "libelle" not in col_mapping:
        print(f"  ATTENTION : colonnes COG non reconnues. Colonnes : {list(cog.columns)}")
        return pd.DataFrame()

    cog = cog.rename(columns={
        col_mapping["code_insee"]: "code_insee",
        col_mapping["libelle"]: "libelle_commune",
    })[["code_insee", "libelle_commune"]]

    cog["code_insee"] = cog["code_insee"].str.strip().str.zfill(5)
    print(f"  COG chargé : {len(cog):,} communes (TYPECOM=COM)")

    # Jointure avec Hexasmal pour avoir le lien CP <-> code INSEE
    if hexasmal_path is not None:
        print(f"  Chargement Hexasmal : {hexasmal_path.name}")

        # Le fichier 019HexaSmal.csv est encodé en ISO-8859-1 (latin-1).
        # La première ligne commence par "#" : pandas l'inclut dans le nom
        # de la première colonne -> on nettoie le "#" après lecture.
        hexa = pd.read_csv(
            hexasmal_path,
            dtype=str,
            sep=";",
            encoding="iso-8859-1",
            low_memory=False,
        )

        # Nettoyer le "#" éventuel en tête du nom de la première colonne
        hexa.columns = [c.lstrip("#").strip() for c in hexa.columns]

        # Colonnes réelles du fichier 019HexaSmal.csv :
        #   Code_commune_INSEE, Nom_de_la_commune, Code_postal,
        #   Libellé_d_acheminement, Ligne_5
        # On normalise pour la détection (accents, casse, underscores)
        import unicodedata as _ud
        def _norm_col(c):
            c = _ud.normalize("NFD", c)
            c = "".join(x for x in c if _ud.category(x) != "Mn")
            return c.upper().replace("_", "").replace(" ", "").strip()

        hexa_cols = {_norm_col(c): c for c in hexa.columns}

        cp_col    = hexa_cols.get("CODEPOSTAL")
        insee_col = hexa_cols.get("CODECOMMUNEINSEE")
        # Libellé_d_acheminement = nom officiel La Poste, fallback Nom_de_la_commune
        # Libellé_d_acheminement -> normalisé en "LIBELLEDACHEMINEMENT"
        # Nom_de_la_commune      -> normalisé en "NOMDELACOMMUNE"
        libelle_col = (
            hexa_cols.get("LIBELLEDACHEMINEMENT")
            or hexa_cols.get("NOMDELACOMMUNE")
            or hexa_cols.get("NOMCOMMUNE")
        )

        if not cp_col or not insee_col:
            print(
                f"  ATTENTION : colonnes Hexasmal non reconnues après nettoyage.\n"
                f"  Colonnes brutes : {list(hexa.columns)}\n"
                f"  Clés normalisées : {list(hexa_cols.keys())}"
            )

        if cp_col and insee_col:
            hexa = hexa.rename(columns={
                cp_col: "code_postal",
                insee_col: "code_insee_hexa",
            })
            hexa["code_postal"] = hexa["code_postal"].str.strip().str.zfill(5)
            hexa["code_insee_hexa"] = hexa["code_insee_hexa"].str.strip().str.zfill(5)

            # Joindre COG sur Hexasmal via code_insee pour récupérer LIBELLE officiel
            cog_enrichi = hexa.merge(
                cog,
                left_on="code_insee_hexa",
                right_on="code_insee",
                how="left",
            )[["code_postal", "code_insee_hexa", "libelle_commune"]].rename(
                columns={"code_insee_hexa": "code_insee"}
            )

            # Fallback : si la jointure COG échoue, utiliser le libellé Hexasmal
            if libelle_col:
                hexa_libelle = hexa[["code_postal", libelle_col]].rename(
                    columns={libelle_col: "libelle_hexa"}
                )
                cog_enrichi = cog_enrichi.merge(hexa_libelle, on="code_postal", how="left")
                cog_enrichi["libelle_commune"] = cog_enrichi["libelle_commune"].fillna(
                    cog_enrichi["libelle_hexa"]
                )
                cog_enrichi = cog_enrichi.drop(columns=["libelle_hexa"], errors="ignore")

            _cog_cache = cog_enrichi.drop_duplicates(subset=["code_postal", "code_insee"])
            print(f"  COG + Hexasmal : {len(_cog_cache):,} associations CP/commune prêtes")
            return _cog_cache

        print("  ATTENTION : colonnes CP ou INSEE non trouvées dans Hexasmal.")
        print(f"  Colonnes disponibles : {list(hexa.columns)}")

    # Sans Hexasmal : COG seul, jointure uniquement par code INSEE direct
    _cog_cache = cog
    print(f"  COG seul (sans Hexasmal) : {len(_cog_cache):,} communes")
    return _cog_cache


def normaliser_ville(ville_brute: str, cp_clean: str, cog: pd.DataFrame) -> tuple[str, str]:
    """
    T05 - Normalise un nom de ville via le COG INSEE 2024.

    Algorithme :
        1. Filtre le COG sur le code postal fourni
        2. S'il n'y a qu'une commune pour ce CP, retourne son libellé officiel
        3. S'il y en a plusieurs, sélectionne la plus proche par similarité
           Jaro-Winkler avec ville_brute
        4. Cas particuliers Paris/Lyon/Marseille : commune = ville principale,
           l'arrondissement est conservé dans la valeur retournée

    Retourne (libelle_commune_officiel, code_insee).
    Retourne ("", "") si le CP est inconnu du COG.
    """
    if cog.empty or not cp_clean or len(cp_clean) != 5:
        return "", ""

    # Cas particulier : arrondissements Paris (750xx), Lyon (6900x), Marseille (130xx)
    commune_principale = None
    if cp_clean.startswith("75") and cp_clean[2:].isdigit():
        commune_principale = "Paris"
    elif cp_clean.startswith("69") and cp_clean[2:].isdigit() and int(cp_clean) <= 69009:
        commune_principale = "Lyon"
    elif cp_clean.startswith("130") and int(cp_clean) <= 13016:
        commune_principale = "Marseille"

    if commune_principale:
        # Pour les arrondissements, code INSEE = 75056 / 69123 / 13055
        code_insee_principal = {"Paris": "75056", "Lyon": "69123", "Marseille": "13055"}
        return commune_principale, code_insee_principal[commune_principale]

    # Recherche dans le COG par code postal
    if "code_postal" in cog.columns:
        candidats = cog[cog["code_postal"] == cp_clean]
    else:
        return "", ""

    if candidats.empty:
        return "", ""

    if len(candidats) == 1:
        row = candidats.iloc[0]
        return str(row["libelle_commune"]), str(row.get("code_insee", ""))

    # Plusieurs communes pour ce CP : similarité Jaro-Winkler avec ville_brute
    if not ville_brute or not isinstance(ville_brute, str):
        row = candidats.iloc[0]
        return str(row["libelle_commune"]), str(row.get("code_insee", ""))

    ville_norm = ville_brute.strip().lower()
    meilleur_score = -1.0
    meilleure_commune = ""
    meilleur_insee = ""

    for _, row in candidats.iterrows():
        libelle = str(row["libelle_commune"]).lower()
        score = JaroWinkler.similarity(ville_norm, libelle)
        if score > meilleur_score:
            meilleur_score = score
            meilleure_commune = str(row["libelle_commune"])
            meilleur_insee = str(row.get("code_insee", ""))

    return meilleure_commune, meilleur_insee


def appliquer_normalisation_villes(
    df: pd.DataFrame,
    col_ville: str,
    col_cp_clean: str,
    cog: pd.DataFrame,
) -> pd.DataFrame:
    """
    Applique T05 sur un DataFrame.
    Crée les colonnes libelle_commune et code_insee.
    Affiche le nombre de villes normalisées avec succès.
    """
    if cog.empty:
        df["libelle_commune"] = ""
        df["code_insee"] = ""
        return df

    resultats = df.apply(
        lambda row: normaliser_ville(
            str(row[col_ville]) if pd.notna(row[col_ville]) else "",
            str(row[col_cp_clean]) if pd.notna(row[col_cp_clean]) else "",
            cog,
        ),
        axis=1,
    )
    df["libelle_commune"] = resultats.apply(lambda x: x[0])
    df["code_insee"] = resultats.apply(lambda x: x[1])

    nb_total = len(df)
    nb_normalises = int(df["libelle_commune"].ne("").sum())
    print(
        f"  T05 villes normalisées : {nb_normalises:,}/{nb_total:,} "
        f"({round(nb_normalises/nb_total*100, 2)}%)"
    )
    return df


# ---------------------------------------------------------------------------
# T06 - Séparation numéro de voie / libellé de voie
# ---------------------------------------------------------------------------

# Regex : numéro en début de chaîne, éventuellement suivi de bis/ter/lettre
REGEX_NUM_DEBUT = re.compile(
    r"^(\d+\s*(?:bis|ter|quater|[a-zA-Z])?)[\s,/-]+(.*)$",
    re.IGNORECASE,
)


def separer_numero_voie(adresse_ligne: str) -> tuple[str, str]:
    """
    T06 - Extrait le numéro de voie du libellé d'une adresse.

    Exemples :
        "15 Rue du Général de Gaulle" -> ("15", "Rue du Général de Gaulle")
        "72ter Rue de la Tour"        -> ("72ter", "Rue de la Tour")
        "12BIS Chemin Aristide Briand"-> ("12BIS", "Chemin Aristide Briand")
        "Rue sans numéro"             -> ("", "Rue sans numéro")

    Retourne (num_voie, libelle_voie).
    """
    if not adresse_ligne or not isinstance(adresse_ligne, str):
        return "", adresse_ligne or ""

    adresse_ligne = adresse_ligne.strip()
    match = REGEX_NUM_DEBUT.match(adresse_ligne)
    if match:
        num = match.group(1).strip()
        libelle = match.group(2).strip()
        return num, libelle

    return "", adresse_ligne


def appliquer_separation_num_voie(
    df: pd.DataFrame,
    col_adresse: str,
    col_num_cible: str = "num_voie",
    col_voie_cible: str = "libelle_voie",
) -> pd.DataFrame:
    """
    Applique T06 sur la colonne d'adresse d'un DataFrame.
    Crée les colonnes num_voie et libelle_voie.
    """
    resultats = df[col_adresse].fillna("").astype(str).apply(separer_numero_voie)
    df[col_num_cible] = resultats.apply(lambda x: x[0])
    df[col_voie_cible] = resultats.apply(lambda x: x[1])
    return df


# ---------------------------------------------------------------------------
# T07 - Détection et isolation des compléments d'adresse
# ---------------------------------------------------------------------------

# Mots-clés signalant un complément d'adresse (bâtiment, appartement, etc.)
# Ordonnés du plus spécifique au plus général pour éviter les faux positifs
MOTS_CLES_COMPLEMENT = [
    r"\bbât(?:iment)?\b",
    r"\bbat(?:iment)?\b",
    r"\bapt\b",
    r"\bappt\b",
    r"\bappartement\b",
    r"\brés(?:idence)?\b",
    r"\bres(?:idence)?\b",
    r"\besc(?:alier)?\b",
    r"\bentrée\b",
    r"\bentree\b",
    r"\btour\b",
    r"\bimm(?:euble)?\b",
    r"\bporte\b",
    r"\bboite\b",
    r"\bboîte\b",
    r"\bété\b",
    r"\blot\b",
    r"\bparc\b",
]

REGEX_COMPLEMENT = re.compile(
    "|".join(MOTS_CLES_COMPLEMENT),
    re.IGNORECASE,
)


def detecter_complement(adresse_ligne: str) -> tuple[str, str]:
    """
    T07 - Détecte et isole le complément d'adresse d'un libellé.

    Stratégie :
        On cherche la première occurrence d'un mot-clé de complément.
        Tout ce qui précède reste comme libellé de voie principal.
        Tout ce qui suit (y compris le mot-clé) devient le complément.

    Exemples :
        "Impasse des Lilas - Rés. les Pins Appt14"
            -> libellé : "Impasse des Lilas"
               complément : "Rés. les Pins Appt14"
        "12 Rue de la Paix Bât C"
            -> libellé : "12 Rue de la Paix"
               complément : "Bât C"
        "15 Avenue de la Gare"
            -> libellé : "15 Avenue de la Gare"
               complément : ""

    Retourne (libelle_sans_complement, complement).
    """
    if not adresse_ligne or not isinstance(adresse_ligne, str):
        return adresse_ligne or "", ""

    adresse_ligne = adresse_ligne.strip()
    match = REGEX_COMPLEMENT.search(adresse_ligne)

    if not match:
        return adresse_ligne, ""

    pos = match.start()

    # Nettoyage des séparateurs en fin de libellé (tiret, virgule, espace)
    libelle = adresse_ligne[:pos].strip().rstrip("-,/").strip()
    complement = adresse_ligne[pos:].strip()

    return libelle, complement


def appliquer_detection_complement(
    df: pd.DataFrame,
    col_adresse: str,
    col_libelle_cible: str = "adresse_principale",
    col_complement_cible: str = "complement_adresse",
) -> pd.DataFrame:
    """
    Applique T07 sur la colonne d'adresse d'un DataFrame.
    Crée les colonnes adresse_principale et complement_adresse.
    Affiche le nombre de lignes avec un complément détecté.
    """
    resultats = df[col_adresse].fillna("").astype(str).apply(detecter_complement)
    df[col_libelle_cible] = resultats.apply(lambda x: x[0])
    df[col_complement_cible] = resultats.apply(lambda x: x[1])

    nb_avec_complement = int(df[col_complement_cible].ne("").sum())
    print(
        f"  T07 compléments détectés : {nb_avec_complement:,}/{len(df):,} lignes "
        f"({round(nb_avec_complement/len(df)*100, 2)}%)"
    )
    return df