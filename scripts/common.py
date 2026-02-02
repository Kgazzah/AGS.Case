"""
common.py
---------
Fonctions utilitaires partagées par les scripts Python.

Ce module porte 3 responsabilités :
1) Connexion PostgreSQL (via variables d'environnement)
2) Idempotence : calcul du checksum et enregistrement dans etl.batch_run
3) Upsert générique dans les tables silver_raw.*

Pourquoi etl.batch_run ?
- Garantir la rejouabilité (re-run) sans doublons
- Tracer l'origine technique de chaque chargement (batch_id)
"""
import os
import hashlib
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Charge .env depuis la racine du projet (ags_case/.env)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"
load_dotenv(dotenv_path=ENV_PATH)

def get_conn():
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT")
    dbname = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")

    # Check explicite (utile pour debug)
    missing = [k for k, v in {
        "PGHOST": host,
        "PGPORT": port,
        "PGDATABASE": dbname,
        "PGUSER": user,
        "PGPASSWORD": password,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}. Check {ENV_PATH}")

    return psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
    )

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

# Idempotence:
# - si (dataset, as_of_date, checksum) existe déjà en SUCCESS => SKIP
# - sinon => on crée un batch STARTED puis on le clôture en SUCCESS/FAILED

def register_batch(conn, dataset: str, as_of_date: str, source_name: str, checksum: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select batch_id, status
            from etl.batch_run
            where dataset=%s and as_of_date=%s and source_checksum=%s
            """,
            (dataset, as_of_date, checksum),
        )
        row = cur.fetchone()
        if row:
            batch_id, status = row
            if status in ("SUCCESS", "SKIPPED"):
                return -1

        cur.execute(
            """
            insert into etl.batch_run(dataset, as_of_date, source_name, source_checksum, status)
            values (%s, %s, %s, %s, 'STARTED')
            returning batch_id
            """,
            (dataset, as_of_date, source_name, checksum),
        )
        batch_id = cur.fetchone()[0]

    conn.commit()
    return batch_id

def finish_batch(conn, batch_id: int, status: str, message: str = ""):
    with conn.cursor() as cur:
        cur.execute(
            """
            update etl.batch_run
            set finished_at=now(), status=%s, message=%s
            where batch_id=%s
            """,
            (status, message, batch_id),
        )
    conn.commit()

# Upsert silver_raw :
# - on considère silver_raw comme la "landing zone" structurée
# - on met à jour les enregistrements si la clé métier existe déjà
# - cela permet de simuler les corrections ERP sur les flux suivants

def upsert_table(conn, table: str, pk_col: str, rows: list[dict], cols: list[str]):
    if not rows:
        return

    values = [[r.get(c) for c in cols] for r in rows]
    col_list = ", ".join(cols)
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in cols if c != pk_col])

    sql = f"""
        insert into {table} ({col_list})
        values %s
        on conflict ({pk_col})
        do update set {set_clause}
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()
