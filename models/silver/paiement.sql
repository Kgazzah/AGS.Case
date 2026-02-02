-- Silver model (DBT)
-- Rôle : exposer uniquement les champs métier attendus (dictionnaire) depuis silver_raw.
-- Remarque : les champs techniques/ERP supplémentaires restent en silver_raw.

select
  ref_paiement::text as ref_paiement,
  ref_salarie::text as ref_salarie,
  montant_paye::numeric(12,2) as montant_paye,
  rib_salarie::text as rib_salarie,
  date_paiement::date as date_paiement,
  ref_demande_avance::text as ref_demande_avance
from {{ source('silver_raw', 'paiement') }}
