"""
Gold SCD2 - demande_avance (DEMANDE ONLY)
----------------------------------------
Alimente la table gold.demande_avance_histo en SCD Type 2.

Source:
- silver.demande_avance (demande ONLY)
- Pas d’enrichissement par paiement

SCD2:
- Si changement métier: close version courante + insert nouvelle version
- Si suppression (absent du flux snapshot): close + insert tombstone is_deleted=true

Traçabilité:
- batch_id = lien vers etl.batch_run (dataset=demande_avance, as_of_date=YYYY-MM-DD)
"""

import argparse
import hashlib
import datetime as dt

from scripts.common import get_conn


def md5_hash(values: list) -> str:
    s = "||".join("" if v is None else str(v) for v in values)
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def get_latest_batch_id(conn, dataset: str, as_of_date: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select batch_id
            from etl.batch_run
            where dataset = %s
              and as_of_date = %s
              and status = 'SUCCESS'
            order by batch_id desc
            limit 1
            """,
            (dataset, as_of_date),
        )
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"No SUCCESS batch found for dataset={dataset} as_of_date={as_of_date}")
        return int(row[0])


def fetch_silver_demande(conn) -> dict:
    """
    Source Silver: demande ONLY
    Colonnes attendues (alignées avec ton modèle):
    - ref_demande_avance
    - ref_salarie
    - montant_demande
    """
    sql = """
      select
        ref_demande_avance,
        ref_salarie,
        montant_demande
      from silver.demande_avance
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out = {}
    for rda, ref_salarie, montant_demande in rows:
        out[str(rda)] = {
            "ref_demande_avance": str(rda),
            "ref_salarie": str(ref_salarie),
            "montant_demande": float(montant_demande) if montant_demande is not None else None,
        }
    return out


def fetch_gold_current(conn) -> dict:
    """
    Versions courantes dans gold.demande_avance_histo
    (uniquement les colonnes présentes dans ta table)
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              ref_demande_avance,
              ref_salarie,
              montant_demande,
              record_hash,
              is_deleted
            from gold.demande_avance_histo
            where is_current = true
            """
        )
        rows = cur.fetchall()

    out = {}
    for rda, ref_salarie, montant_demande, record_hash, is_deleted in rows:
        out[str(rda)] = {
            "ref_salarie": str(ref_salarie),
            "montant_demande": float(montant_demande) if montant_demande is not None else None,
            "record_hash": record_hash,
            "is_deleted": bool(is_deleted),
        }
    return out


def close_current(conn, ref_demande_avance: str, as_of_date: dt.date):
    with conn.cursor() as cur:
        cur.execute(
            """
            update gold.demande_avance_histo
            set valid_to = %s,
                is_current = false
            where ref_demande_avance = %s
              and is_current = true
            """,
            (as_of_date, ref_demande_avance),
        )


def compute_hash(row: dict, is_deleted: bool) -> str:
    # Hash métier DEMANDE ONLY (aligné colonnes table)
    return md5_hash([
        row.get("ref_salarie"),
        row.get("montant_demande"),
        is_deleted,
    ])


def insert_version(conn, row: dict, as_of_date: dt.date, batch_id: int, is_deleted: bool):
    record_hash = compute_hash(row, is_deleted=is_deleted)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into gold.demande_avance_histo (
              ref_demande_avance,
              ref_salarie,
              montant_demande,
              valid_from,
              valid_to,
              is_current,
              is_deleted,
              record_hash,
              batch_id
            )
            values (%s,%s,%s, %s, date '9999-12-31', true, %s, %s, %s)
            """,
            (
                row["ref_demande_avance"],
                row["ref_salarie"],
                row.get("montant_demande"),
                as_of_date,
                is_deleted,
                record_hash,
                batch_id,
            ),
        )


def main():
    ap = argparse.ArgumentParser(
        description="Apply SCD2 historization for gold.demande_avance_histo from silver.demande_avance (DEMANDE ONLY)"
    )
    ap.add_argument("--as-of", required=True, help="Date logique du flux (YYYY-MM-DD)")
    args = ap.parse_args()

    as_of_date = dt.datetime.strptime(args.as_of, "%Y-%m-%d").date()

    conn = get_conn()
    conn.autocommit = False
    try:
        batch_id = get_latest_batch_id(conn, "demande_avance", args.as_of)

        silver = fetch_silver_demande(conn)
        gold_current = fetch_gold_current(conn)

        silver_keys = set(silver.keys())
        gold_keys = set(gold_current.keys())

        # 1) Inserts / Updates (SCD2)
        for rda, row in silver.items():
            new_hash = compute_hash(row, is_deleted=False)

            if rda not in gold_current:
                insert_version(conn, row, as_of_date, batch_id, is_deleted=False)
            else:
                if gold_current[rda]["record_hash"] != new_hash or gold_current[rda]["is_deleted"] is True:
                    close_current(conn, rda, as_of_date)
                    insert_version(conn, row, as_of_date, batch_id, is_deleted=False)

        # 2) Suppressions logiques (tombstone) : absent du flux snapshot
        deleted_rda = gold_keys - silver_keys
        for rda in deleted_rda:
            if gold_current[rda]["is_deleted"] is False:
                close_current(conn, rda, as_of_date)

                tomb = {
                    "ref_demande_avance": rda,
                    "ref_salarie": gold_current[rda]["ref_salarie"],
                    "montant_demande": gold_current[rda]["montant_demande"],
                }
                insert_version(conn, tomb, as_of_date, batch_id, is_deleted=True)

        conn.commit()
        print(f"OK gold.demande_avance_histo applied for as_of={args.as_of} (batch_id={batch_id})")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
