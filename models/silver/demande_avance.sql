-- Silver model (DBT)
-- Rôle : exposer uniquement les champs métier attendus (dictionnaire) depuis silver_raw.
-- Remarque : les champs techniques/ERP supplémentaires restent en silver_raw.

select
  ref_demande_avance::text as ref_demande_avance,
  ref_salarie::text as ref_salarie,
  montant_demande::numeric(12,2) as montant_demande
from {{ source('silver_raw', 'demande_avance') }}
