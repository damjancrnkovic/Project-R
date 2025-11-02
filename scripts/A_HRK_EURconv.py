import pandas as pd

# === Postavke ===
SRC = "sve_dionice_merged.xlsx"          # ulazna datoteka
DST = "sve_dionice_merged_EUR.xlsx"      # izlazna datoteka
FX = 7.53450                             # fiksni tečaj HRK → EUR

PRICE_COLS = [
    "Open Price", "High Price", "Low Price",
    "Last Price", "VWAP Price", "Prev Close Price"
]
TURNOVER_COL = "Turnover"
PRICE_CCY_COL = "Price Currency"
TURNOVER_CCY_COL = "Turnover Currency"

# === Učitavanje i obrada ===
xls = pd.ExcelFile(SRC)
summary_rows = []

with pd.ExcelWriter(DST, engine="xlsxwriter") as writer:
    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        df.columns = [str(c).strip() for c in df.columns]

        # maske gdje su valute HRK
        price_mask = df[PRICE_CCY_COL].eq("HRK") if PRICE_CCY_COL in df.columns else pd.Series([False]*len(df))
        turnover_mask = df[TURNOVER_CCY_COL].eq("HRK") if TURNOVER_CCY_COL in df.columns else pd.Series([False]*len(df))

        # konverzija cijena
        for col in PRICE_COLS:
            if col in df.columns:
                df.loc[price_mask, col] = df.loc[price_mask, col] / FX
                df[col] = df[col].round(4)
        if PRICE_CCY_COL in df.columns:
            df.loc[price_mask, PRICE_CCY_COL] = "EUR"

        # konverzija prometa
        if TURNOVER_COL in df.columns:
            df.loc[turnover_mask, TURNOVER_COL] = df.loc[turnover_mask, TURNOVER_COL] / FX
            df[TURNOVER_COL] = df[TURNOVER_COL].round(2)
        if TURNOVER_CCY_COL in df.columns:
            df.loc[turnover_mask, TURNOVER_CCY_COL] = "EUR"

        # spremi konvertirani sheet
        df.to_excel(writer, sheet_name=sheet, index=False)

        summary_rows.append({
            "Sheet": sheet,
            "Rows": len(df),
            "HRK→EUR (price rows)": int(price_mask.sum()),
            "HRK→EUR (turnover rows)": int(turnover_mask.sum())
        })

# === Sažetak ===
summary = pd.DataFrame(summary_rows).sort_values("Sheet")
print("\n=== HRK → EUR konverzija: sažetak ===")
print(summary.to_string(index=False))
print(f"\nKonvertirana datoteka spremljena kao: {DST}")
