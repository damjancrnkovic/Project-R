import pandas as pd

SRC = "sve_dionice_merged_EUR.xlsx"
DST = "sve_dionice_merged_EUR_filled.xlsx"

PRICE_COLS = ["Open Price","High Price","Low Price","Last Price","VWAP Price","Prev Close Price"]
ZERO_COLS  = ["Volume","Num Trades","Turnover"]
META_COLS  = ["MIC","Symbol","ISIN","Price Currency"]
DATE_COL   = "Date"
MODEL_COL  = "Trading Model"
FILTER_R_A = True

xls = pd.ExcelFile(SRC)
sheet_names = xls.sheet_names

if FILTER_R_A:
    filtered = []
    for n in sheet_names:
        if n.endswith("-R-A"):
            if n.replace("-R-A","") not in sheet_names:
                filtered.append(n)
        else:
            filtered.append(n)
    sheet_names = filtered

# Unija svih datuma
all_dates = set()
for sh in sheet_names:
    df = xls.parse(sh)
    df.columns = [str(c).strip() for c in df.columns]
    if DATE_COL not in df.columns:
        continue
    dates = pd.to_datetime(df[DATE_COL], errors="coerce").dt.normalize().dropna()
    all_dates.update(dates.tolist())
global_dates = pd.DatetimeIndex(sorted(all_dates))

with pd.ExcelWriter(DST, engine="xlsxwriter") as writer:
    for sh in sheet_names:
        df = xls.parse(sh)
        df.columns = [str(c).strip() for c in df.columns]
        if DATE_COL not in df.columns:
            df.to_excel(writer, sheet_name=sh, index=False); continue

        # Normaliziraj datum
        df[DATE_COL] = pd.to_datetime(df[DATE_COL], errors="coerce").dt.normalize()
        df = df.dropna(subset=[DATE_COL])

        # ⛔ Makni BLOCK/OTC redke (ako stupac postoji)
        if MODEL_COL in df.columns:
            mask_bad = df[MODEL_COL].astype(str).str.upper().str.strip().isin({"BLOCK","OTC"})
            df = df[~mask_bad]

        # Ako je sve ispalo, samo snimi prazan rezultat
        if df.empty:
            df.to_excel(writer, sheet_name=sh, index=False); continue

        df = df.sort_values(DATE_COL).groupby(DATE_COL, as_index=False).last().set_index(DATE_COL)

        active_start, active_end = df.index.min(), df.index.max()
        wanted_idx = global_dates[(global_dates >= active_start) & (global_dates <= active_end)]

        re = df.reindex(wanted_idx)
        new_rows_mask = re.index.difference(df.index)
        new_rows_mask = re.index.isin(new_rows_mask)

        # FFill cijene i meta-info
        for col in PRICE_COLS + META_COLS:
            if col in re.columns:
                re[col] = re[col].ffill()

        # Nove dane -> nule za volumene/trgovanja/promet
        for col in ZERO_COLS:
            if col in re.columns:
                re.loc[new_rows_mask, col] = 0

        re = re.reset_index().rename(columns={"index": DATE_COL})
        re.to_excel(writer, sheet_name=sh, index=False)

print(f"Gotovo ✅ Spremio sam: {DST}")
