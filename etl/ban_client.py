"""
ban_client.py
T08 - Validation et géocodage via l'API Adresse officielle (BAN).
Projet ADREA - ImmoPro France SAS

Documentation API : https://adresse.data.gouv.fr/api-doc/adresse

Deux modes d'appel :
    - Unitaire  : valider une adresse unique (tests, debug)
    - Batch CSV : valider un bloc de lignes via POST /search/csv/
                  (endpoint officiel, traite jusqu'à 50 000 lignes par appel)

Stratégie grands volumes (R1 + R4 du cahier des charges) :
    - Cache local JSON : les résultats BAN sont persistés sur disque.
      Un addr_id déjà traité ne rappelle jamais l'API. Cela protège
      contre l'indisponibilité de l'API et évite de repayer le coût
      réseau sur les relances de pipeline.
    - Découpage en batches de BATCH_SIZE lignes (défaut 5 000) pour
      rester dans les limites de l'endpoint /search/csv/.
    - Retry exponentiel sur les erreurs réseau (3 tentatives, backoff x2).
    - Timeout configurable pour éviter les blocages.

Seuils de qualification (conformes au TP) :
    SEUIL_PASS        >= 0.7  -> qualité A, chargé dans DuckDB
    SEUIL_QUARANTAINE >= 0.5  -> qualité B, export quarantaine.csv
    SEUIL_REJET       < 0.5   -> qualité D, export rejected.csv
"""

import csv
import io
import json
import time
from pathlib import Path

import httpx
import pandas as pd

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

CACHE_PATH = OUTPUT_DIR / "ban_cache.json"

# Endpoint officiel BAN
BAN_URL_UNITAIRE = "https://api-adresse.data.gouv.fr/search/"
BAN_URL_BATCH = "https://api-adresse.data.gouv.fr/search/csv/"

# Taille d'un batch envoyé à l'API BAN (max recommandé : 50 000)
BATCH_SIZE = 5_000

# Seuils de qualification
SEUIL_PASS = 0.7
SEUIL_QUARANTAINE = 0.5

# Timeout et retry
TIMEOUT_SECONDES = 30
MAX_RETRIES = 3
BACKOFF_BASE = 2.0


def charger_cache() -> dict:
    """
    Charge le cache BAN depuis le fichier JSON local.
    Retourne un dictionnaire {addr_id: résultat_ban}.
    Si le fichier n'existe pas, retourne un dictionnaire vide.
    """
    if not CACHE_PATH.exists():
        return {}
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        print(f"  ATTENTION : cache BAN illisible, réinitialisation -> {CACHE_PATH}")
        return {}


def sauvegarder_cache(cache: dict):
    """Persiste le cache BAN sur disque (écrasement complet)."""
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


def geocoder_unitaire(adresse: str, code_postal: str = "") -> dict:
    """
    T08 - Géocode une adresse unique via l'endpoint /search/ de l'API BAN.

    Utile pour les tests, le debug et les adresses isolées en quarantaine.

    Paramètres :
        adresse     : libellé complet de l'adresse (ex: "15 Rue de la Paix")
        code_postal : code postal pour affiner la recherche (recommandé)

    Retourne un dictionnaire avec les clés :
        result_label   : adresse normalisée retournée par la BAN
        result_score   : score de confiance entre 0 et 1
        latitude       : coordonnée GPS (float ou None)
        longitude      : coordonnée GPS (float ou None)
        result_type    : type de résultat (housenumber, street, municipality)
        ban_ok         : True si score >= SEUIL_PASS
    """
    params = {"q": adresse, "limit": 1}
    if code_postal:
        params["postcode"] = code_postal

    for tentative in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=TIMEOUT_SECONDES) as client:
                reponse = client.get(BAN_URL_UNITAIRE, params=params)
                reponse.raise_for_status()

            data = reponse.json()
            features = data.get("features", [])

            if not features:
                return _resultat_vide()

            feature = features[0]
            props = feature.get("properties", {})
            coords = feature.get("geometry", {}).get("coordinates", [None, None])

            score = float(props.get("score", 0.0))
            return {
                "result_label": props.get("label", ""),
                "result_score": score,
                "latitude": float(coords[1]) if coords[1] is not None else None,
                "longitude": float(coords[0]) if coords[0] is not None else None,
                "result_type": props.get("type", ""),
                "ban_ok": score >= SEUIL_PASS,
            }

        except httpx.TimeoutException:
            print(f"  BAN unitaire : timeout (tentative {tentative}/{MAX_RETRIES})")
        except httpx.HTTPStatusError as e:
            print(f"  BAN unitaire : erreur HTTP {e.response.status_code}")
            break
        except Exception as e:
            print(f"  BAN unitaire : erreur inattendue -> {e}")
            break

        if tentative < MAX_RETRIES:
            time.sleep(BACKOFF_BASE**tentative)

    return _resultat_vide()


def _resultat_vide() -> dict:
    """Retourne un résultat BAN vide (adresse non géocodée)."""
    return {
        "result_label": "",
        "result_score": 0.0,
        "latitude": None,
        "longitude": None,
        "result_type": "",
        "ban_ok": False,
    }


def _construire_csv_batch(df_batch: pd.DataFrame) -> str:
    """
    Construit le CSV en mémoire à envoyer à l'endpoint /search/csv/ de la BAN.

    L'API BAN attend un CSV avec une colonne 'adresse' obligatoire.
    On ajoute aussi 'postcode' pour affiner la recherche.
    La colonne 'id' permet de retrouver chaque ligne dans la réponse.

    Retourne le contenu CSV sous forme de chaîne.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["id", "adresse", "postcode"])

    for _, row in df_batch.iterrows():
        adresse = str(row.get("adresse_normalisee", "")).strip()
        cp = str(row.get("cp_clean", "")).strip()
        addr_id = str(row.get("addr_id", "")).strip()
        writer.writerow([addr_id, adresse, cp])

    return buffer.getvalue()


def _parser_reponse_batch(contenu_csv: str) -> dict:
    """
    Parse la réponse CSV de l'endpoint /search/csv/ de la BAN.

    La BAN retourne le CSV source enrichi avec les colonnes :
        result_label, result_score, result_type,
        latitude, longitude, result_status

    Retourne un dictionnaire {id: résultat_ban}.
    """
    resultats = {}
    reader = csv.DictReader(io.StringIO(contenu_csv))

    for row in reader:
        addr_id = row.get("id", "").strip()
        if not addr_id:
            continue

        try:
            score = float(row.get("result_score", 0.0))
        except (ValueError, TypeError):
            score = 0.0

        try:
            lat = float(row.get("latitude", "")) if row.get("latitude") else None
        except (ValueError, TypeError):
            lat = None

        try:
            lng = float(row.get("longitude", "")) if row.get("longitude") else None
        except (ValueError, TypeError):
            lng = None

        resultats[addr_id] = {
            "result_label": row.get("result_label", ""),
            "result_score": score,
            "latitude": lat,
            "longitude": lng,
            "result_type": row.get("result_type", ""),
            "ban_ok": score >= SEUIL_PASS,
        }

    return resultats


def geocoder_batch(df: pd.DataFrame, col_id: str = "addr_id") -> pd.DataFrame:
    """
    T08 - Géocode un DataFrame entier en mode batch via l'API BAN.

    Algorithme :
        1. Charger le cache local
        2. Filtrer les lignes non encore traitées (non présentes dans le cache)
        3. Découper en batches de BATCH_SIZE lignes
        4. Pour chaque batch : POST /search/csv/ avec retry exponentiel
        5. Mettre à jour le cache après chaque batch (protection contre crash)
        6. Joindre les résultats BAN au DataFrame source
        7. Créer la colonne 'qualite' (A/B/C/D) selon les seuils

    Paramètres :
        df     : DataFrame avec les colonnes 'addr_id', 'adresse_normalisee', 'cp_clean'
        col_id : nom de la colonne identifiant unique (défaut 'addr_id')

    Retourne le DataFrame enrichi avec les colonnes BAN.
    """
    cache = charger_cache()

    # Identifier les lignes non encore dans le cache
    ids_a_traiter = [
        str(row[col_id]) for _, row in df.iterrows() if str(row[col_id]) not in cache
    ]

    nb_total = len(df)
    nb_cache = nb_total - len(ids_a_traiter)
    nb_a_traiter = len(ids_a_traiter)

    print(
        f"  T08 BAN : {nb_total:,} adresses | {nb_cache:,} en cache | {nb_a_traiter:,} à appeler"
    )

    if nb_a_traiter > 0:
        df_a_traiter = df[df[col_id].astype(str).isin(ids_a_traiter)].copy()
        df_a_traiter = df_a_traiter.rename(columns={col_id: "addr_id"})

        nb_batches = (nb_a_traiter + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  T08 BAN : {nb_batches} batch(es) de {BATCH_SIZE} lignes max")

        for i in range(nb_batches):
            debut = i * BATCH_SIZE
            fin = min(debut + BATCH_SIZE, nb_a_traiter)
            df_batch = df_a_traiter.iloc[debut:fin]

            print(f"  Batch {i + 1}/{nb_batches} : lignes {debut + 1} à {fin}...")

            resultats_batch = _appeler_api_batch(df_batch)
            cache.update(resultats_batch)

            # Sauvegarde du cache après chaque batch (protection crash)
            sauvegarder_cache(cache)

    # Joindre les résultats BAN au DataFrame source
    df = _enrichir_dataframe(df, cache, col_id)

    # Afficher la distribution des scores
    _afficher_distribution_scores(df)

    return df


def _appeler_api_batch(df_batch: pd.DataFrame) -> dict:
    """
    Envoie un batch CSV à l'API BAN et retourne les résultats parsés.
    Applique un retry exponentiel en cas d'échec réseau.
    """
    contenu_csv = _construire_csv_batch(df_batch)
    # L'API BAN /search/csv/ attend un multipart/form-data avec le CSV
    # dans un champ nommé "data". Envoyer le CSV en body brut provoque HTTP 400.
    fichier_multipart = {
        "data": ("adresses.csv", contenu_csv.encode("utf-8"), "text/csv")
    }

    for tentative in range(1, MAX_RETRIES + 1):
        try:
            with httpx.Client(timeout=TIMEOUT_SECONDES) as client:
                reponse = client.post(BAN_URL_BATCH, files=fichier_multipart)
                reponse.raise_for_status()

            return _parser_reponse_batch(reponse.text)

        except httpx.TimeoutException:
            print(f"    Timeout (tentative {tentative}/{MAX_RETRIES})")
        except httpx.HTTPStatusError as e:
            print(
                f"    Erreur HTTP {e.response.status_code} (tentative {tentative}/{MAX_RETRIES})"
            )
        except Exception as e:
            print(f"    Erreur inattendue : {e} (tentative {tentative}/{MAX_RETRIES})")

        if tentative < MAX_RETRIES:
            delai = BACKOFF_BASE**tentative
            print(f"    Attente {delai:.0f}s avant retry...")
            time.sleep(delai)

    # Échec total : retourner des résultats vides pour toutes les lignes du batch
    print(f"    Batch échoué après {MAX_RETRIES} tentatives -> résultats vides")
    return {str(row["addr_id"]): _resultat_vide() for _, row in df_batch.iterrows()}


def _enrichir_dataframe(df: pd.DataFrame, cache: dict, col_id: str) -> pd.DataFrame:
    """
    Joint les résultats BAN du cache au DataFrame source.
    Crée les colonnes result_score, latitude, longitude, result_label,
    result_type, qualite, motif_rejet.
    """
    result_scores = []
    latitudes = []
    longitudes = []
    result_labels = []
    result_types = []
    qualites = []
    motifs_rejet = []

    for _, row in df.iterrows():
        addr_id = str(row[col_id])
        r = cache.get(addr_id, _resultat_vide())

        score = float(r.get("result_score", 0.0))
        lat = r.get("latitude")
        lng = r.get("longitude")

        # Si la BAN ne donne pas de coordonnées GPS, utiliser celles de la source
        if lat is None and "lat_src" in row and pd.notna(row["lat_src"]):
            try:
                lat = float(row["lat_src"])
            except (ValueError, TypeError):
                pass

        if lng is None and "lng_src" in row and pd.notna(row["lng_src"]):
            try:
                lng = float(row["lng_src"])
            except (ValueError, TypeError):
                pass

        result_scores.append(score)
        latitudes.append(lat)
        longitudes.append(lng)
        result_labels.append(r.get("result_label", ""))
        result_types.append(r.get("result_type", ""))

        # Qualification selon les seuils du TP
        if score >= SEUIL_PASS:
            qualites.append("A")
            motifs_rejet.append("")
        elif score >= SEUIL_QUARANTAINE:
            qualites.append("B")
            motifs_rejet.append(f"score_ban={score:.3f} < {SEUIL_PASS}")
        else:
            qualites.append("D")
            if score == 0.0:
                motifs_rejet.append("score_ban=0 (adresse non reconnue par la BAN)")
            else:
                motifs_rejet.append(f"score_ban={score:.3f} < {SEUIL_QUARANTAINE}")

    df["result_score"] = result_scores
    df["latitude_ban"] = latitudes
    df["longitude_ban"] = longitudes
    df["result_label"] = result_labels
    df["result_type"] = result_types
    df["qualite"] = qualites
    df["motif_rejet"] = motifs_rejet

    return df


def _afficher_distribution_scores(df: pd.DataFrame):
    """Affiche la distribution des scores BAN et la répartition PASS/QUARANTAINE/REJET."""
    nb_total = len(df)
    if nb_total == 0:
        return

    nb_pass = int((df["qualite"] == "A").sum())
    nb_quarantaine = int((df["qualite"] == "B").sum())
    nb_rejet = int((df["qualite"] == "D").sum())

    print("\n  Distribution des scores BAN :")
    print(
        f"    PASS        (score >= {SEUIL_PASS})  : {nb_pass:>8,}  ({nb_pass / nb_total * 100:.1f}%)"
    )
    print(
        f"    QUARANTAINE (score >= {SEUIL_QUARANTAINE})  : {nb_quarantaine:>8,}  ({nb_quarantaine / nb_total * 100:.1f}%)"
    )
    print(
        f"    REJET       (score <  {SEUIL_QUARANTAINE})  : {nb_rejet:>8,}  ({nb_rejet / nb_total * 100:.1f}%)"
    )


def separer_flux(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Sépare le DataFrame en trois flux selon la qualité BAN.

    Retourne (df_pass, df_quarantaine, df_rejet).
        df_pass        : score >= SEUIL_PASS -> vers DuckDB referentiel_adresses
        df_quarantaine : SEUIL_QUARANTAINE <= score < SEUIL_PASS -> quarantaine.csv
        df_rejet       : score < SEUIL_QUARANTAINE ou CP invalide -> rejected.csv
    """
    if "qualite" not in df.columns:
        raise ValueError(
            "La colonne 'qualite' est absente. Appelez geocoder_batch() d'abord."
        )

    # Les lignes avec CP invalide (cp_invalide=True) vont directement en rejet
    # indépendamment du score BAN
    if "cp_invalide" in df.columns:
        df.loc[df["cp_invalide"], "qualite"] = "D"
        df.loc[df["cp_invalide"], "motif_rejet"] = df.loc[
            df["cp_invalide"], "motif_rejet"
        ].where(
            df.loc[df["cp_invalide"], "motif_rejet"].ne(""),
            "code_postal_invalide",
        )

    df_pass = df[df["qualite"] == "A"].copy()
    df_quarantaine = df[df["qualite"] == "B"].copy()
    df_rejet = df[df["qualite"] == "D"].copy()

    return df_pass, df_quarantaine, df_rejet


def analyser_distribution_scores(df: pd.DataFrame):
    """
    Analyse détaillée de la distribution des scores BAN.
    Affiche l'histogramme par tranche de 0.1 et les statistiques.
    Utile pour l'exercice 5 du TP.
    """
    if "result_score" not in df.columns:
        print("  Colonne result_score absente.")
        return

    scores = df["result_score"].dropna()
    print("\n  Histogramme des scores BAN (par tranche de 0.1) :")
    print(f"  {'Tranche':<15} {'Nb adresses':>12} {'Pourcentage':>12}")
    print("  " + "-" * 42)

    bornes = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
    for i in range(len(bornes) - 1):
        inf = bornes[i]
        sup = bornes[i + 1]
        label = f"[{inf:.1f} - {sup:.1f}["
        nb = int(((scores >= inf) & (scores < sup)).sum())
        pct = round(nb / len(scores) * 100, 1) if len(scores) > 0 else 0.0
        barre = "#" * int(pct / 2)
        print(f"  {label:<15} {nb:>12,} {pct:>11.1f}%  {barre}")

    print(f"\n  Score moyen  : {scores.mean():.4f}")
    print(f"  Score médian : {scores.median():.4f}")
    print(f"  Score min    : {scores.min():.4f}")
    print(f"  Score max    : {scores.max():.4f}")
