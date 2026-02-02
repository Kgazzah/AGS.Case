"""
Gold SCD2 - paiement
-------------------
Alimente la table gold.paiement_histo en SCD Type 2.

Principe:
- 1 version "courante" (is_current=true) + période de validité (valid_from/valid_to)
- Détection des changements métier via record_hash
- Si changement: close version courante + insert nouvelle version
- Si suppression (absent du flux): close + insert version is_deleted=true

Traçabilité:
- batch_id = lien vers etl.batch_run (quel flux a produit la version)
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


def fetch_silver_paiement(conn) -> dict:
    """
    Source Silver (DBT view): silver.paiement
    Retour dict[ref_paiement] = row
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select ref_paiement, ref_salarie, montant_paye, rib_salarie, date_paiement, ref_demande_avance
            from silver.paiement
            """
        )
        rows = cur.fetchall()

    out = {}
    for ref_paiement, ref_salarie, montant_paye, rib_salarie, date_paiement, ref_demande_avance in rows:
        out[str(ref_paiement)] = {
            "ref_paiement": str(ref_paiement),
            "ref_salarie": str(ref_salarie),
            "montant_paye": float(montant_paye) if montant_paye is not None else None,
            "rib_salarie": str(rib_salarie),
            "date_paiement": date_paiement,  # date
            "ref_demande_avance": str(ref_demande_avance),
        }
    return out


def fetch_gold_current(conn) -> dict:
    """
    Versions courantes dans gold.paiement_histo
    Retour dict[ref_paiement] = {record_hash, is_deleted, ...}
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
              ref_paiement,
              ref_salarie,
              montant_paye,
              rib_salarie,
              date_paiement,
              ref_demande_avance,
              record_hash,
              is_deleted
            from gold.paiement_histo
            where is_current = true
            """
        )
        rows = cur.fetchall()

    out = {}
    for (ref_paiement, ref_salarie, montant_paye, rib_salarie, date_paiement, ref_demande_avance, record_hash, is_deleted) in rows:
        out[str(ref_paiement)] = {
            "ref_salarie": ref_salarie,
            "montant_paye": montant_paye,
            "rib_salarie": rib_salarie,
            "date_paiement": date_paiement,
            "ref_demande_avance": ref_demande_avance,
            "record_hash": record_hash,
            "is_deleted": bool(is_deleted),
        }
    return out


def close_current(conn, ref_paiement: str, as_of_date: dt.date):
    with conn.cursor() as cur:
        cur.execute(
            """
            update gold.paiement_histo
            set valid_to = %s,
                is_current = false
            where ref_paiement = %s
              and is_current = true
            """,
            (as_of_date, ref_paiement),
        )


def insert_version(conn, row: dict, as_of_date: dt.date, batch_id: int, is_deleted: bool):
    record_hash = md5_hash([
        row["ref_salarie"],
        row["montant_paye"],
        row["rib_salarie"],
        row["date_paiement"],
        row["ref_demande_avance"],
        is_deleted,
    ])

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into gold.paiement_histo (
              ref_paiement,
              ref_salarie,
              montant_paye,
              rib_salarie,
              date_paiement,
              ref_demande_avance,
              valid_from,
              valid_to,
              is_current,
              is_deleted,
              record_hash,
              batch_id
            )
            values (%s,%s,%s,%s,%s,%s, %s, date '9999-12-31', true, %s, %s, %s)
            """,
            (
                row["ref_paiement"],
                row["ref_salarie"],
                row["montant_paye"],
                row["rib_salarie"],
                row["date_paiement"],
                row["ref_demande_avance"],
                as_of_date,
                is_deleted,
                record_hash,
                batch_id,
            ),
        )


def main():
    ap = argparse.ArgumentParser(description="Apply SCD2 historization for gold.paiement_histo from silver.paiement")
    ap.add_argument("--as-of", required=True, help="Date logique du flux (YYYY-MM-DD)")
    ap.add_argument("--batch-dataset", default="paiement", help="dataset name in etl.batch_run (default: paiement)")
    args = ap.parse_args()

    as_of_date = dt.datetime.strptime(args.as_of, "%Y-%m-%d").date()

    conn = get_conn()
    conn.autocommit = False
    try:
        batch_id = get_latest_batch_id(conn, args.batch_dataset, args.as_of)

        silver = fetch_silver_paiement(conn)
        gold_current = fetch_gold_current(conn)

        silver_keys = set(silver.keys())
        gold_keys = set(gold_current.keys())

        # 1) insert / update SCD2
        for ref in silver_keys:
            row = silver[ref]
            new_hash = md5_hash([
                row["ref_salarie"],
                row["montant_paye"],
                row["rib_salarie"],
                row["date_paiement"],
                row["ref_demande_avance"],
                False,
            ])

            if ref not in gold_current:
                insert_version(conn, row, as_of_date, batch_id, is_deleted=False)
            else:
                if gold_current[ref]["record_hash"] != new_hash or gold_current[ref]["is_deleted"] is True:
                    close_current(conn, ref, as_of_date)
                    insert_version(conn, row, as_of_date, batch_id, is_deleted=False)

        # 2) suppressions logiques (absent du flux)
        deleted_refs = gold_keys - silver_keys
        for ref in deleted_refs:
            if gold_current[ref]["is_deleted"] is False:
                close_current(conn, ref, as_of_date)
                tomb = {
                    "ref_paiement": ref,
                    "ref_salarie": str(gold_current[ref]["ref_salarie"]),
                    "montant_paye": float(gold_current[ref]["montant_paye"]),
                    "rib_salarie": str(gold_current[ref]["rib_salarie"]),
                    "date_paiement": gold_current[ref]["date_paiement"],
                    "ref_demande_avance": str(gold_current[ref]["ref_demande_avance"]),
                }
                insert_version(conn, tomb, as_of_date, batch_id, is_deleted=True)

        conn.commit()
        print(f"OK gold.paiement_histo applied for as_of={args.as_of} (batch_id={batch_id})")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
