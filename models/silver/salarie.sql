-- Silver model (DBT)
-- Rôle : exposer uniquement les champs métier attendus (dictionnaire) depuis silver_raw.
-- Remarque : les champs techniques/ERP supplémentaires restent en silver_raw.

select
  ref_salarie::text as ref_salarie,
  nni::text as nni,
  nom::text as nom,
  prenom::text as prenom
from {{ source('silver_raw', 'salarie') }}
