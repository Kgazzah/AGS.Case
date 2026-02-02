-- =====================================================
-- Provisioning initial de la base de données
-- A exécuter une seule fois (hors DBT)
-- PostgreSQL
-- =====================================================
do
$$
begin
  if not exists (
    select 1 from pg_database where datname = 'ags'
  ) then
    create database ags;
  end if;
end
$$;