# adrea-immo-pipeline

Pipeline ETL de normalisation des adresses postales — Projet ADRÉA — ImmoPro France SAS.

Objectif : normalisation de 1 274 000 adresses issues de 4 systèmes hétérogènes,
validation via la Base Adresse Nationale officielle, chargement dans DuckDB.

## Stack technique

- Python 3.11+, uv, Git Bash, VS Code
- pandas, duckdb, rapidfuzz, httpx
- API BAN (adresse.data.gouv.fr)
- COG INSEE + Hexasmal La Poste

## Structure du projet

```
adrea-immo-pipeline/
├── sources/                   # CSV sources — ne jamais modifier
│   ├── source_crm_contacts.csv
│   ├── source_sap_partenaires.csv
│   ├── Source_portail_biens.csv
│   └── source_agences_flux.csv
├── referentiels/              # Référentiels officiels à télécharger
│   ├── communes2026.csv       # COG INSEE (toute année récente)
│   └── 019HexaSmal.csv        # Hexasmal La Poste — fichier léger 1.6 Mo
├── output/                    # Fichiers générés (non versionnés)
├── etl/                       # Pipeline ETL — règles T01 à T10
│   ├── profiling.py
│   ├── transform.py
│   ├── ban_client.py
│   ├── enrichissement.py
│   └── pipeline_etl.py
├── eval/                      # Évaluation — règles T11 à T15
│   ├── mesure_avant.py
│   ├── transformations_v2.py
│   ├── t15_dedup_inter_sources.py
│   └── mesure_apres.py
├── tests/                     # 80 tests unitaires
├── pyproject.toml
└── README.md
```

## Installation

```bash
# Dans Git Bash, depuis la racine du projet
uv sync
```

## Référentiels à télécharger avant de lancer

Placer les fichiers dans `referentiels/` :

| Fichier            | Source                                       | URL                                                   |
| ------------------ | -------------------------------------------- | ----------------------------------------------------- |
| `communes2026.csv` | COG INSEE                                    | https://www.insee.fr/fr/information/2560452           |
| `019HexaSmal.csv`  | La Poste Hexasmal — fichier d'origine 1.6 Mo | https://datanova.laposte.fr/datasets/laposte-hexasmal |

## Exécution complète

### Étape 1 — Profiling initial des sources

```bash
uv run python etl/profiling.py
```

Lit les 4 CSV et affiche les métriques de qualité initiales.
Exporte `output/rapport_qualite_initial.csv`.

### Étape 2 — Pipeline ETL

```bash
# Mode test rapide (je recommande pour la première exécution)
uv run python -m etl.pipeline_etl --sample 200

# Mode complet (il faut prévoir 45 à 60 minutes — appels API BAN)
uv run python -m etl.pipeline_etl
```

`--sample 200` limite chaque source à 200 lignes pour valider le pipeline sans attendre les appels BAN sur 1,2 million d'adresses. Les résultats BAN sont mis en cache dans `output/ban_cache.json` : une relance ne rappelle pas l'API pour les adresses déjà traitées.

Sorties :

- `output/adrea_etl.duckdb` — base DuckDB avec les adresses PASS
- `output/adresses_quarantaine_*.csv` — adresses à révision manuelle
- `output/adresses_rejetees_*.csv` — adresses rejetées avec motif

### Étape 3 — Tests unitaires

```bash
uv run python -m pytest tests/ -v
```

80 tests couvrant les règles T01 à T10 et le client BAN.

## Évaluation — Transformations T11 à T15

Lancer dans l'ordre après le pipeline ETL :

```bash
# Avec la ma base DuckDB par défaut (output/adrea_etl.duckdb)
uv run python eval/mesure_avant.py
uv run python eval/transformations_v2.py
uv run python eval/t15_dedup_inter_sources.py
uv run python eval/mesure_apres.py

# Avec une base DuckDB externe (votre base de données locale)
uv run python eval/mesure_avant.py         --db chemin/vers/base.duckdb --table nom_table
uv run python eval/transformations_v2.py   --db chemin/vers/base.duckdb --table nom_table
uv run python eval/t15_dedup_inter_sources.py --db chemin/vers/base.duckdb --table nom_table
uv run python eval/mesure_apres.py         --db chemin/vers/base.duckdb --table nom_table
```

Les arguments `--db` et `--table` sont optionnels.
Sans argument, les scripts utilisent `output/adrea_etl.duckdb` et la table `referentiel_adresses`.

## Règles de transformation

### Pipeline ETL — T01 à T10

| Règle | Nom                    | Description                                        |
| ----- | ---------------------- | -------------------------------------------------- |
| T01   | Normalisation casse    | "RUE DU GENERAL" → "Rue du Général"                |
| T02   | Parsing adresse_brute  | Découpe le champ monolithique du flux agences      |
| T03   | Expansion abréviations | r.→Rue, av.→Avenue, bld→Boulevard...               |
| T04   | Nettoyage code postal  | Valide ^[0-9]{5}$, rejette sinon                   |
| T05   | Normalisation ville    | Jointure COG INSEE + Hexasmal                      |
| T06   | Séparation numéro/voie | Extrait "15bis" du libellé de voie                 |
| T07   | Détection complément   | Isole bât, apt, résidence dans une colonne séparée |
| T08   | Validation BAN         | API officielle, score 0-1, géocodage GPS           |
| T09   | Code RIVOLI FANTOIR    | Code officiel DGFiP de la voie                     |
| T10   | Déduplication fuzzy    | Jaro-Winkler >= 0.92 par groupe CP                 |

### Évaluation — T11 à T15

| Règle | Nom                         | Description                              |
| ----- | --------------------------- | ---------------------------------------- |
| T11   | Normalisation pays + zone   | FR/FRA/france→FR, METRO/CORSE/DOM/TOM    |
| T12   | Validation num_voie         | Rejette 0, 00, >9999 — normalise bis/ter |
| T13   | Standardisation compléments | bat A→Bâtiment A, appt 14→Appartement 14 |
| T14   | Score de complétude         | Score 0-100 sur 7 champs clés            |
| T15   | Déduplication inter-sources | Doublons CRM vs Portail par Jaro-Winkler |

## Flux de sortie ETL

| Flux        | Condition                      | Destination                         |
| ----------- | ------------------------------ | ----------------------------------- |
| PASS        | score BAN >= 0.7               | DuckDB `referentiel_adresses`       |
| QUARANTAINE | score BAN 0.5 à 0.7            | `output/adresses_quarantaine_*.csv` |
| REJET       | score BAN < 0.5 ou CP invalide | `output/adresses_rejetees_*.csv`    |
