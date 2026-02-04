"""
Gold SCD2 - salarie
-------------------
Alimente la table gold.salarie_histo en SCD Type 2.

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
    """
    Récupère le dernier batch SUCCESS pour (dataset, as_of_date).
    On s'appuie sur etl.batch_run déjà alimenté par l'ingestion Python.
    """
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
            raise RuntimeError(
                f"No SUCCESS batch found in etl.batch_run for dataset={dataset} as_of_date={as_of_date}"
            )
        return int(row[0])


def fetch_silver_salarie(conn) -> dict:
    """
    Source Silver (DBT view): silver.salarie
    Retour: dict[ref_salarie] = {nni, nom, prenom}
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select ref_salarie, nni, nom, prenom
            from silver.salarie
            """
        )
        rows = cur.fetchall()

    out = {}
    for ref_salarie, nni, nom, prenom in rows:
        out[str(ref_salarie)] = {
            "ref_salarie": str(ref_salarie),
            "nni": str(nni),
            "nom": str(nom),
            "prenom": str(prenom),
        }
    return out


def fetch_gold_current(conn) -> dict:
    """
    Récupère la version courante (is_current=true) dans gold.salarie_histo
    Retour: dict[ref_salarie] = {nni, nom, prenom, record_hash, is_deleted}
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select ref_salarie, nni, nom, prenom, record_hash, is_deleted
            from gold.salarie_histo
            where is_current = true
            """
        )
        rows = cur.fetchall()

    out = {}
    for ref_salarie, nni, nom, prenom, record_hash, is_deleted in rows:
        out[str(ref_salarie)] = {
            "nni": nni,
            "nom": nom,
            "prenom": prenom,
            "record_hash": record_hash,
            "is_deleted": bool(is_deleted),
        }
    return out


def close_current(conn, ref_salarie: str, as_of_date: dt.date):
    with conn.cursor() as cur:
        cur.execute(
            """
            update gold.salarie_histo
            set valid_to = %s,
                is_current = false
            where ref_salarie = %s
              and is_current = true
            """,
            (as_of_date, ref_salarie),
        )


def insert_version(
    conn,
    row: dict,
    as_of_date: dt.date,
    batch_id: int,
    is_deleted: bool,
):
    record_hash = md5_hash([row["nni"], row["nom"], row["prenom"], is_deleted])

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into gold.salarie_histo (
              ref_salarie, nni, nom, prenom,
              valid_from, valid_to, is_current, is_deleted,
              record_hash, batch_id
            )
            values (%s,%s,%s,%s, %s, date '9999-12-31', true, %s, %s, %s)
            """,
            (
                row["ref_salarie"],
                row["nni"],
                row["nom"],
                row["prenom"],
                as_of_date,
                is_deleted,
                record_hash,
                batch_id,
            ),
        )


def main():
    ap = argparse.ArgumentParser(description="Apply SCD2 historization for gold.salarie_histo from silver.salarie")
    ap.add_argument("--as-of", required=True, help="Date logique du flux (YYYY-MM-DD)")
    ap.add_argument(
        "--batch-dataset",
        default="salarie",
        help="dataset name in etl.batch_run for batch lookup (default: salarie)",
    )
    args = ap.parse_args()

    as_of_date = dt.datetime.strptime(args.as_of, "%Y-%m-%d").date()

    conn = get_conn()
    conn.autocommit = False
    try:
        batch_id = get_latest_batch_id(conn, args.batch_dataset, args.as_of)

        silver = fetch_silver_salarie(conn)
        gold_current = fetch_gold_current(conn)

        silver_keys = set(silver.keys())
        gold_keys = set(gold_current.keys())

        # 1) inserts + updates (SCD2)
        for ref in silver_keys:
            row = silver[ref]
            new_hash = md5_hash([row["nni"], row["nom"], row["prenom"], False])

            if ref not in gold_current:
                # nouveau salarié
                insert_version(conn, row, as_of_date, batch_id, is_deleted=False)
            else:
                # modification ou réactivation (si deleted auparavant)
                if gold_current[ref]["record_hash"] != new_hash or gold_current[ref]["is_deleted"] is True:
                    close_current(conn, ref, as_of_date)
                    insert_version(conn, row, as_of_date, batch_id, is_deleted=False)

        # 2) suppressions logiques: présent en gold courant mais absent du flux silver
        deleted_refs = gold_keys - silver_keys
        for ref in deleted_refs:
            if gold_current[ref]["is_deleted"] is False:
                # clôture la version courante
                close_current(conn, ref, as_of_date)
                # insère une version "deleted" (tombstone) avec les dernières valeurs connues
                tomb = {
                    "ref_salarie": ref,
                    "nni": gold_current[ref]["nni"],
                    "nom": gold_current[ref]["nom"],
                    "prenom": gold_current[ref]["prenom"],
                }
                insert_version(conn, tomb, as_of_date, batch_id, is_deleted=True)

        conn.commit()
        print(f"OK gold.salarie_histo applied for as_of={args.as_of} (batch_id={batch_id})")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
