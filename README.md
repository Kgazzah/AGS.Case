# AGS – Cas d’usage Data Engineer  
**DBT • PostgreSQL • Python • SCD2 • Idempotence**

---

##  Objectif du projet

Ce projet implémente un **pipeline data de bout en bout** simulant le traitement des avances de salaires versées par l’AGS, avec :

- ingestion de fichiers ERP (Excel)
- nettoyage / normalisation via DBT (Silver)
- historisation métier **SCD Type 2** (Gold)
- **idempotence** et **traçabilité complète** des traitements
- orchestration automatisée


##  Architecture cible

ERP (fichiers Excel)
│
▼
Python (Bronze / Landing)
│
▼
PostgreSQL – silver_raw (tables physiques)
│
▼
DBT – Silver (vues normalisées + tests)
│
▼
Python – Gold (SCD2 historisé)
│
▼
PostgreSQL – gold (tables historisées)


---

## Structure du projet

AGS_CASE
├── data/ # Fichiers d'exemple ERP (cas d'usage)
├── ddl/ # Scripts SQL (création DB si nécessaire)
├── macros/ # Macros DBT (init_db)
├── models/
│ ├── silver/ # Modèles DBT Silver
│ └── tests/ # Tests DBT (not_null, unique, relations)
├── scripts/
│ ├── bronze/ # Ingestion fichiers → silver_raw
│ ├── gold/ # Historisation SCD2
│ └── common/ # Connexion DB, hash, batch_run
├── snapshots/ # Réservé (DBT)
├── tests/ # Tests techniques
├── run_all.ps1 # Orchestration complète
├── requirements.txt # Dépendances Python
├── dbt_project.yml
└── README.md


---

## Modèle de données

### Silver (normalisation – DBT)
- `silver.salarie`
- `silver.demande_avance`
- `silver.paiement`

Tests DBT :
- `not_null`
- `unique`
- `relationships`

---

### Gold (historisation SCD2)

#### Principes
- conservation de l’historique complet
- gestion des corrections et suppressions
- lien traçable vers le batch d’ingestion

#### Tables
- `gold.salarie_histo`
- `gold.demande_avance_histo`
- `gold.paiement_histo`

Champs communs :
- `valid_from`, `valid_to`
- `is_current`, `is_deleted`
- `record_hash`
- `batch_id`

---

## Idempotence & Traçabilité

Une table technique centrale est utilisée :

### `etl.batch_run`

Chaque exécution est identifiée par :
- `dataset`
- `as_of_date`
- `source_checksum`

Règles :
- un fichier déjà traité n’est **pas retraité**
- chaque version Gold est reliée à un `batch_id`
- statut : `STARTED`, `SUCCESS`, `FAILED`, `SKIPPED`

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

Initialisation DB (dbt run-operation init_db)

Ingestion Bronze (Python)

Transformation Silver (DBT)

Historisation Gold (Python)

Tests DBT

Exécution
.\run_all.ps1
Configuration
Les paramètres de connexion PostgreSQL sont fournis via variables d’environnement :

PGHOST
PGPORT
PGDATABASE
PGUSER
PGPASSWORD


Tests
Tests DBT :
dbt test --select silver