import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

IN_SAL = DATA_DIR / "salaries.xlsx"
IN_DMD = DATA_DIR / "demandes_avance.xlsx"
IN_PAY = DATA_DIR / "paiements.xlsx"

OUT_SAL_0209 = DATA_DIR / "salaries_2024-09-02.xlsx"
OUT_DMD_0209 = DATA_DIR / "demandes_avance_2024-09-02.xlsx"

OUT_SAL_1009 = DATA_DIR / "salaries_2024-09-10.xlsx"
OUT_DMD_1009 = DATA_DIR / "demandes_avance_2024-09-10.xlsx"

def main():
    sal = pd.read_excel(IN_SAL)
    dmd = pd.read_excel(IN_DMD)
    pay = pd.read_excel(IN_PAY)

    # Normaliser noms de colonnes
    sal.columns = [c.strip() for c in sal.columns]
    dmd.columns = [c.strip() for c in dmd.columns]
    pay.columns = [c.strip() for c in pay.columns]

    # ---------------------------
    # 02/09 : corrections + suppression
    # - corriger le nom d’un salarié (ligne 1)
    # - corriger montant_demande d’une demande (ligne 2 si existe)
    # - supprimer une demande (ligne 3 si existe) => suppression logique côté Gold
    # ---------------------------
    sal_0209 = sal.copy()
    dmd_0209 = dmd.copy()

    if len(sal_0209) >= 1 and "nom" in sal_0209.columns:
        sal_0209.loc[sal_0209.index[0], "nom"] = str(sal_0209.loc[sal_0209.index[0], "nom"]) + "_CORR"

    if len(dmd_0209) >= 2 and "montant_demande" in dmd_0209.columns:
        dmd_0209.loc[dmd_0209.index[1], "montant_demande"] = float(dmd_0209.loc[dmd_0209.index[1], "montant_demande"]) + 100.0

    removed_demande_row = None
    if len(dmd_0209) >= 3:
        removed_demande_row = dmd_0209.iloc[[2]].copy()  # on la garde pour la réinsertion 10/09
        dmd_0209 = dmd_0209.drop(dmd_0209.index[2]).reset_index(drop=True)

    # sauvegarde 02/09
    sal_0209.to_excel(OUT_SAL_0209, index=False)
    dmd_0209.to_excel(OUT_DMD_0209, index=False)

    # ---------------------------
    # 10/09 : rectifications post-paiement
    # - ajustement montant_demande sur une demande payée (si existe) : +350
    # - réinsertion de la demande supprimée au 02/09 (si on en a supprimé une)
    # - correction nom d’un salarié (ligne 2 si existe)
    # - changement RIB d’un salarié (ligne 1 si colonne rib existe) => utile côté source/traçabilité
    # ---------------------------
    sal_1009 = sal_0209.copy()
    dmd_1009 = dmd_0209.copy()

    if len(dmd_1009) >= 1 and "montant_demande" in dmd_1009.columns:
        dmd_1009.loc[dmd_1009.index[0], "montant_demande"] = float(dmd_1009.loc[dmd_1009.index[0], "montant_demande"]) + 350.0

    if removed_demande_row is not None:
        dmd_1009 = pd.concat([dmd_1009, removed_demande_row], ignore_index=True)

    if len(sal_1009) >= 2 and "nom" in sal_1009.columns:
        sal_1009.loc[sal_1009.index[1], "nom"] = str(sal_1009.loc[sal_1009.index[1], "nom"]) + "_POSTPAY"

    if "rib" in sal_1009.columns and len(sal_1009) >= 1:
        sal_1009.loc[sal_1009.index[0], "rib"] = str(sal_1009.loc[sal_1009.index[0], "rib"]) + "_V2"

    # sauvegarde 10/09
    sal_1009.to_excel(OUT_SAL_1009, index=False)
    dmd_1009.to_excel(OUT_DMD_1009, index=False)

    print("Generated:")
    print(" -", OUT_SAL_0209.name)
    print(" -", OUT_DMD_0209.name)
    print(" -", OUT_SAL_1009.name)
    print(" -", OUT_DMD_1009.name)

if __name__ == "__main__":
    main()
