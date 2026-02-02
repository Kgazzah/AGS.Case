
{# 
  init_db
  ------
  Objectif : rendre l'environnement reproductible (CI/CD, onboarding, re-déploiement).

  Ce macro crée (idempotent) :
  - les schémas techniques et métiers (etl, silver_raw, silver, gold)
  - la table etl.batch_run (traçabilité + idempotence)
  - les tables physiques d'atterrissage (silver_raw.*)

  Note :
  - silver_raw = tables physiques chargées par Python (équivalent "bronze/landing")
  - silver = vues DBT conformes au dictionnaire de données attendu
  - gold = tables historisées (SCD2) alimentées par Python
#}

{% macro init_db() %}
  {# =========================
     Schemas
     ========================= #}
  {% do run_query("create schema if not exists etl;") %}
  {% do run_query("create schema if not exists silver_raw;") %}
  {% do run_query("create schema if not exists silver;") %}
  {% do run_query("create schema if not exists gold;") %}

  {# =========================
     ETL batch table (idempotence / traçabilité)
     Table technique centrale :
   - 1 ligne par exécution (dataset + as_of_date + checksum)
   - permet de SKIP un fichier déjà traité (idempotence)
   - permet de tracer quel batch a produit quelle version Gold (batch_id)
     ========================= #}
  {% do run_query("""
    create table if not exists etl.batch_run (
      batch_id bigserial primary key,
      dataset text not null,
      as_of_date date not null,
      source_name text not null default 'erp',
      source_checksum text not null,
      started_at timestamptz not null default now(),
      finished_at timestamptz,
      status text not null default 'STARTED', -- STARTED, SUCCESS, FAILED, SKIPPED
      message text,
      unique(dataset, as_of_date, source_checksum)
    );
  """) %}

  {% do run_query("""
    create index if not exists ix_batch_run_dataset_date
    on etl.batch_run(dataset, as_of_date);
  """) %}

  {# =========================
     Silver RAW tables (physiques, alimentées par Python)
     - on inclut aussi les champs ERP en plus (rib, rang_creance, date_reception)
     ========================= #}
  {% do run_query("""
    create table if not exists silver_raw.salarie (
      ref_salarie text primary key,
      nni text not null,
      nom text not null,
      prenom text not null,
      rib text
    );
  """) %}

  {% do run_query("""
    create table if not exists silver_raw.demande_avance (
      ref_demande_avance text primary key,
      ref_salarie text not null,
      rang_creance text,
      montant_demande numeric(12,2) not null,
      date_reception date
    );
  """) %}

  {% do run_query("""
    create table if not exists silver_raw.paiement (
      ref_paiement text primary key,
      ref_salarie text not null,
      montant_paye numeric(12,2) not null,
      rib_salarie text not null,
      date_paiement date not null,
      ref_demande_avance text not null
    );
  """) %}

  {# =========================
     GOLD tables (historisées - SCD2)
     ========================= #}

  {% do run_query("""
    create table if not exists gold.salarie_histo (
      ref_salarie text not null,
      nni text not null,
      nom text not null,
      prenom text not null,

      valid_from date not null,
      valid_to date not null default date '9999-12-31',
      is_current boolean not null default true,
      is_deleted boolean not null default false,

      record_hash text not null,
      batch_id bigint not null references etl.batch_run(batch_id),
      ingested_at timestamptz not null default now(),

      primary key (ref_salarie, valid_from)
    );
  """) %}

  {% do run_query("""
    create index if not exists ix_salarie_histo_current
    on gold.salarie_histo(ref_salarie)
    where is_current;
  """) %}

  {% do run_query("""
    create table if not exists gold.demande_avance_histo (
      ref_demande_avance text not null,
      ref_salarie text not null,
      montant_demande numeric(12,2) not null,

      -- champs portés par la demande suite au paiement
      montant_paye numeric(12,2),
      date_paiement date,
      ref_paiement text,

      valid_from date not null,
      valid_to date not null default date '9999-12-31',
      is_current boolean not null default true,
      is_deleted boolean not null default false,

      record_hash text not null,
      batch_id bigint not null references etl.batch_run(batch_id),
      ingested_at timestamptz not null default now(),

      primary key (ref_demande_avance, valid_from)
    );
  """) %}

  {% do run_query("""
    create index if not exists ix_demande_histo_current
    on gold.demande_avance_histo(ref_demande_avance)
    where is_current;
  """) %}

  {% do run_query("""
    create table if not exists gold.paiement_histo (
      ref_paiement text not null,
      ref_salarie text not null,
      montant_paye numeric(12,2) not null,
      rib_salarie text not null,
      date_paiement date not null,
      ref_demande_avance text not null,

      valid_from date not null,
      valid_to date not null default date '9999-12-31',
      is_current boolean not null default true,
      is_deleted boolean not null default false,

      record_hash text not null,
      batch_id bigint not null references etl.batch_run(batch_id),
      ingested_at timestamptz not null default now(),

      primary key (ref_paiement, valid_from)
    );
  """) %}

  {% do run_query("""
    create index if not exists ix_paiement_histo_current
    on gold.paiement_histo(ref_paiement)
    where is_current;
  """) %}

{% endmacro %}
