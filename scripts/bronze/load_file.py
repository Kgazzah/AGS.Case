"""
load_file.py
--------------
Ingestion des fichiers ERP vers silver_raw.* (tables physiques).

- Supporte Excel (.xlsx) et CSV
- Applique une normalisation légère (types/dates)
- Garantit l'idempotence via etl.batch_run (checksum fichier)

Entrées:
- --dataset : salarie | demande_avance | paiement
- --as-of   : date logique du flux (ex: 2024-08-25)
- --file    : chemin du fichier

Sortie:
- Upsert dans silver_raw.<dataset>
- Synchronisation "snapshot" : suppression des lignes absentes du fichier (gestion suppressions)
- 1 ligne dans etl.batch_run (SUCCESS/FAILED)

Ce script suppose que le fichier fourni est un SNAPSHOT complet du dataset à la date donnée.
Si l’ERP envoie des fichiers incrémentaux (delta), il ne faut PAS activer la suppression.
"""
import argparse
import os
import pandas as pd

from psycopg2.extras import execute_values

from scripts.common import (
    get_conn,
    sha256_file,
    register_batch,
    finish_batch,
    upsert_table,
)

# ------------------------------------------------------------
# Mapping dataset -> table cible + colonnes attendues dans le fichier
# (aligné avec les fichiers Excel fournis)
# ------------------------------------------------------------
DATASETS = {
    "salarie": {
        "table": "silver_raw.salarie",
        "pk": "ref_salarie",
        # salaries.xlsx contient rib en plus
        "cols": ["ref_salarie", "nni", "nom", "prenom", "rib"],
    },
    "demande_avance": {
        "table": "silver_raw.demande_avance",
        "pk": "ref_demande_avance",
        # demandes_avance.xlsx contient rang_creance + date_reception
        "cols": ["ref_demande_avance", "ref_salarie", "rang_creance", "montant_demande", "date_reception"],
    },
    "paiement": {
        "table": "silver_raw.paiement",
        "pk": "ref_paiement",
        # paiements.xlsx
        "cols": ["ref_paiement", "ref_salarie", "montant_paye", "rib_salarie", "date_paiement", "ref_demande_avance"],
    },
}


def read_file(path: str) -> pd.DataFrame:
    """
    Lit un fichier CSV ou Excel.
    - CSV: pd.read_csv
    - Excel: pd.read_excel
    """
    ext = os.path.splitext(path.lower())[1]
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)  # 1ère feuille
    if ext == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file extension: {ext}. Use .csv, .xlsx, or .xls")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalisation simple :
    - strip des noms de colonnes
    - remplace NaN par None
    """
    df.columns = [c.strip() for c in df.columns]
    df = df.where(pd.notnull(df), None)
    return df


def sync_deletions_snapshot(conn, table: str, pk: str, pk_values: list[str]) -> int:
    """
    Gestion des suppressions (mode SNAPSHOT):
    - supprime de la table cible toutes les lignes dont la PK n'est pas dans pk_values.
    - si pk_values est vide => table vidée.

    Implémentation:
    - on charge les PK du fichier dans une table temporaire
    - on supprime dans table toutes les lignes dont la PK n'existe pas dans cette table temp
    """
    with conn.cursor() as cur:
        if not pk_values:
            # Fichier vide => suppression totale (snapshot vide)
            cur.execute(f"delete from {table};")
            return cur.rowcount

        # Table temporaire de clés
        cur.execute("create temporary table tmp_keys(pk text) on commit drop;")

        # Insert PKs dans tmp_keys
        values = [(v,) for v in pk_values]
        execute_values(cur, "insert into tmp_keys(pk) values %s", values, page_size=1000)

        # Supprimer les lignes absentes du snapshot
        cur.execute(
            f"""
            delete from {table} t
            where not exists (
              select 1 from tmp_keys k where k.pk = t.{pk}
            );
            """
        )
        return cur.rowcount


def main():
    ap = argparse.ArgumentParser(description="Load ERP files into silver_raw tables with idempotence + snapshot deletions.")
    ap.add_argument("--dataset", required=True, choices=DATASETS.keys(),
                    help="Dataset to load: salarie | demande_avance | paiement")
    ap.add_argument("--as-of", required=True, help="Date logique du flux (YYYY-MM-DD), ex: 2024-08-25")
    ap.add_argument("--file", required=True, help="Chemin vers le fichier source (.xlsx/.csv)")
    ap.add_argument("--source", default="erp", help="Nom de la source (défaut: erp)")
    ap.add_argument(
        "--snapshot",
        action="store_true",
        default=True,
        help="Mode snapshot (par défaut activé) : supprime les lignes absentes du fichier."
    )
    args = ap.parse_args()

    meta = DATASETS[args.dataset]

    # 1) checksum du fichier (idempotence)
    checksum = sha256_file(args.file)

    conn = get_conn()
    try:
        # 2) enregistrement batch (idempotence)
        batch_id = register_batch(conn, args.dataset, args.as_of, args.source, checksum)
        if batch_id == -1:
            print("SKIP: flux déjà traité (idempotent).")
            return

        # 3) lecture fichier
        df = read_file(args.file)
        df = normalize_dataframe(df)

        # 4) vérification colonnes requises
        missing = [c for c in meta["cols"] if c not in df.columns]
        if missing:
            raise ValueError(
                f"Missing columns in file for dataset '{args.dataset}': {missing}. "
                f"Columns found: {list(df.columns)}"
            )

        # 5) sélection et conversion minimale
        df = df[meta["cols"]].copy()

        # Convertir dates si présentes
        for date_col in ["date_reception", "date_paiement"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

        # Nettoyage PK (important)
        pk_col = meta["pk"]
        df[pk_col] = df[pk_col].astype(str).str.strip()

        rows = df.to_dict(orient="records")

        # 6) upsert vers silver_raw
        upsert_table(conn, meta["table"], meta["pk"], rows, meta["cols"])

        # 6bis) gestion des suppressions (snapshot sync)
        deleted = 0
        if args.snapshot:
            pk_values = df[pk_col].dropna().astype(str).tolist()
            deleted = sync_deletions_snapshot(conn, meta["table"], meta["pk"], pk_values)

        conn.commit()

        # 7) clôture batch
        msg = f"Ingestion {args.dataset} OK ({len(rows)} rows)"
        if args.snapshot:
            msg += f" + snapshot deletions ({deleted} deleted)"
        finish_batch(conn, batch_id, "SUCCESS", msg)

        print(f"OK: batch_id={batch_id} dataset={args.dataset} as_of={args.as_of} rows={len(rows)} deleted={deleted}")

    except Exception as e:
        conn.rollback()
        try:
            if "batch_id" in locals() and batch_id > 0:
                finish_batch(conn, batch_id, "FAILED", str(e))
        except Exception:
            pass
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
