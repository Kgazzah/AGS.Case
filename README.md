# AGS – Cas d’usage Data Engineer  
**DBT • PostgreSQL • Python • Architecture Médaillon • SCD2 • Idempotence**

---

##  Objectif du projet

Ce projet implémente un pipeline Data de bout en bout structuré selon une architecture Médaillon (Bronze / Silver / Gold), simulant le traitement des avances de salaires versées par l’AGS à partir de fichiers ERP (Excel).

Il met en œuvre :

- une ingestion contrôlée et idempotente des fichiers ERP (Bronze)
- une normalisation et des contrôles qualité via DBT (Silver)
- une historisation métier SCD Type 2 (Gold)
- une traçabilité complète des traitements
- une orchestration automatisée du pipeline

---

##  Architecture cible

ERP métier (fichiers Excel)

   ↓
   
Python – Bronze (Ingestion, contrôle, idempotence, snapshots)

   ↓
   
PostgreSQL – silver_raw (tables physiques)

   ↓
   
DBT – Silver (vues normalisées + tests qualité)

   ↓
   
Python – Gold (historisation SCD Type 2)

   ↓
   
PostgreSQL – Gold (tables historisées)

---

## Structure du projet

AGS_CASE/

├── data/                      # Fichiers d'exemple ERP (cas d'usage)

├── ddl/                       # Scripts SQL (création de la base si nécessaire)

├── macros/                    # Macros DBT (init_db)

├── models/

│   ├── silver/                # Modèles DBT Silver

│   └── tests/                 # Tests DBT (not_null, unique, relationships)

├── scripts/

│   ├── bronze/                # Ingestion fichiers → silver_raw

│   ├── gold/                  # Historisation SCD Type 2

│   └── common/                # Connexion DB, hash, gestion batch_run

├── snapshots/                 # Réservé (DBT)

├── tests/                     # Tests techniques
 
├── run_all.ps1               # Script d’orchestration complet

├── requirements.txt          # Dépendances Python

├── dbt_project.yml           # Configuration du projet DBT

└── README.md                 # Documentation du projet

---

## Modèle de données

### Bronze – Ingestion technique
La couche Bronze correspond à l’ingestion brute et contrôlée des données ERP, sans transformation métier.

#### Source : 
fichiers ERP (Excel / CSV)

#### Objectif :
- garantir l’idempotence
- assurer la traçabilité des flux
- conserver une image fidèle du fichier source

#### Tables Bronze (physiques)
- silver_raw.salarie
- silver_raw.demande_avance
- silver_raw.paiement

#### Caractéristiques :
- 1 ligne = 1 ligne du fichier source
- aucune jointure
- aucune règle métier
- synchronisation snapshot (suppression des lignes absentes du fichier)
- lien avec la table technique etl.batch_run

### Silver (normalisation – DBT)

#### Tables
- `silver.salarie`
- `silver.demande_avance`
- `silver.paiement`

#### Tests DBT :
- `not_null`
- `unique`
- `relationships`

#### Objectif : 
garantir la qualité, la cohérence et la conformité métier des données.

### Gold (historisation SCD2)

#### Principes
- conservation de l’historique complet
- gestion des corrections et suppressions
- séparation claire entre données métier et logique de paiement
- traçabilité vers le flux d’origine

#### Tables
- `gold.salarie_histo`
- `gold.demande_avance_histo`
- `gold.paiement_histo`

#### Champs communs :
- `valid_from`, `valid_to`
- `is_current`, `is_deleted`
- `record_hash`
- `batch_id`

---

## Idempotence & Traçabilité

Une table technique centrale est utilisée :
`etl.batch_run`

Chaque exécution est identifiée par :
- `dataset`
- `as_of_date`
- `source_checksum`

Règles :
- un fichier déjà traité n’est **pas retraité**
- chaque version Gold est reliée à un `batch_id`
- statut : `STARTED`, `SUCCESS`, `FAILED`, `SKIPPED`

Cette table permet :
- auditabilité
- rejouabilité contrôlée
- gouvernance des flux

---

## record_hash

Le `record_hash` est un **hash SHA-256** calculé sur les champs métier significatifs.

Il permet :
- de détecter les changements métier
- d’optimiser les comparaisons SCD2
- d’éviter les mises à jour inutiles

---

## Orchestration

L’orchestration complète est centralisée dans :

```bash
run_all.ps1
Ce script exécute :

- Initialisation DB (dbt run-operation init_db)
- Ingestion Bronze (Python)
- Transformation Silver (DBT)
- Historisation Gold (Python)
- Tests DBT

## Exécution
.\run_all.ps1

## Configuration
Les paramètres de connexion PostgreSQL sont fournis via variables d’environnement :

- PGHOST
- PGPORT
- PGDATABASE
- PGUSER
- PGPASSWORD


## Tests
Tests DBT :
dbt test --select silver
