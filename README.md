# adrea-immo-pipeline

Pipeline ETL/ELT de normalisation des adresses postales.
Projet ADREA - ImmoPro France SAS.

## Stack

- Python 3.11+
- pandas, duckdb, rapidfuzz, httpx, great-expectations
- uv (gestionnaire de paquets)
- Git Bash (CLI)
- DuckDB (base cible)

## Structure

```
adrea-immo-pipeline/
├── sources/                      # CSV sources (lecture seule - ne jamais modifier)
│   ├── source_crm_contacts.csv
│   ├── source_sap_partenaires.csv
│   ├── Source_portail_biens.csv
│   └── source_agences_flux.csv
├── referentiels/                 # Référentiels officiels téléchargés
│   ├── communes2024.csv          # COG INSEE 2024
│   ├── hexasmal.csv              # La Poste - CP/communes
│   └── fantoir.csv               # DGFiP - codes voies RIVOLI
├── output/                       # Fichiers générés (gitignorés)
│   ├── adrea_etl.duckdb
│   ├── rapport_qualite_initial.csv
│   ├── rapport_qualite_final.csv
│   ├── adresses_quarantaine.csv
│   └── adresses_rejetees.csv
├── etl/                          # Partie 1 - Approche ETL
│   ├── profiling.py              # Livrable 1 - Profiling initial
│   ├── transform.py              # Livrable 2 - Règles T01 à T10
│   ├── ban_client.py             # T08 - Client API BAN (géocodage)
│   └── pipeline_etl.py          # Orchestration ETL complète
├── elt/                          # Partie 2 - Approche ELT
│   ├── load_bronze.py
│   └── adrea_elt/                # Projet dbt
├── utils/
│   └── helpers.py                # Fonctions utilitaires partagées
├── tests/
│   └── test_transform.py         # Tests unitaires des règles de transformation
├── pyproject.toml
├── .gitignore
└── README.md
```

## Installation et démarrage (Git Bash)

```bash
# 1. Cloner / créer le projet
git init adrea-immo-pipeline
cd adrea-immo-pipeline

# 2. Créer l'environnement virtuel avec uv
uv venv

# 3. Activer l'environnement (Git Bash)
source .venv/Scripts/activate

# 4. Installer les dépendances
uv sync

# 5. Copier les CSV sources dans le dossier sources/
# (ne jamais modifier les fichiers sources)

# 6. Lancer le profiling initial
python etl/profiling.py
```

## Référentiels officiels à télécharger manuellement

| Référentiel | URL | Fichier cible |
|---|---|---|
| COG INSEE 2024 | https://www.insee.fr/fr/information/2560452 | referentiels/communes2024.csv |
| Hexasmal La Poste | https://datanova.laposte.fr/datasets/laposte-hexasmal | referentiels/hexasmal.csv |
| FANTOIR DGFiP | https://www.data.gouv.fr/fr/datasets/fichier-fantoir-des-voies-et-lieux-dits/ | referentiels/fantoir.csv |

## Règles de transformation ETL (T01 à T10)

| Règle | Nom | Description |
|---|---|---|
| T01 | Normalisation casse | Typographie française des voies |
| T02 | Parsing adresse_brute | Extraction num/type/nom voie, CP, commune |
| T03 | Expansion abréviations | r.→Rue, av.→Avenue, bld→Boulevard... |
| T04 | Nettoyage code postal | Format ^[0-9]{5}$, rejet sinon |
| T05 | Normalisation ville | Jointure COG INSEE 2024 |
| T06 | Séparation numéro/voie | Regex extraction num de voie |
| T07 | Détection complément | Isolation bât, apt, résidence, escalier |
| T08 | Validation BAN | API adresse.data.gouv.fr, score >= 0.7 |
| T09 | Code RIVOLI FANTOIR | Jointure FANTOIR (code_insee + libellé voie) |
| T10 | Déduplication fuzzy | Jaro-Winkler >= 0.92 par groupe CP |

## Flux de sortie

- **PASS** (score BAN >= 0.7) → DuckDB `referentiel_adresses`
- **QUARANTAINE** (score 0.5-0.7) → `output/adresses_quarantaine.csv`
- **REJET** (score < 0.5 ou CP invalide) → `output/adresses_rejetees.csv`
