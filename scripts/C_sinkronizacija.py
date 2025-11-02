import pandas as pd

file_path = "sve_dionice_merged_EUR_filled.xlsx"

xls = pd.ExcelFile(file_path)

print("Sheetovi pronađeni u datoteci:")

# zadrži:
# - sve sheetove bez -R-A
# - sheetove s -R-A samo ako ne postoji njihova verzija bez nastavka
all_sheets = xls.sheet_names
base_names = set(s.replace("-R-A", "") for s in all_sheets)
filtered_sheets = []
izbaceni=[]
for name in all_sheets:
    if name.endswith("-R-A"):
        if name.replace("-R-A", "") not in all_sheets:
            filtered_sheets.append(name)
        else:
            izbaceni.append(name)
    else:
        filtered_sheets.append(name)

# 2) Odaberi metriku i agregaciju (za Volume/Num Trades često je "sum", za cijene "last")
metric = "Last Price"  # npr. "Volume", "Num Trades", "Last Price", "VWAP Price", "Turnover"
default_agg = "last"
per_metric_agg = {"Volume": "sum", "Num Trades": "sum"}
agg = per_metric_agg.get(metric, default_agg)

# 3) Inkrementalno gradi pivot (outer join po datumu)
pivot = pd.DataFrame()

for sheet in filtered_sheets:
    df = xls.parse(sheet)
    df.columns = [str(c).strip() for c in df.columns]
    if "Date" not in df.columns or metric not in df.columns:
        continue

    # Parsiraj datum na date (bez vremena) i ukloni loše retke
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    df = df.dropna(subset=["Date"])

    # Agregiraj na dnevnu razinu po metrikama
    if agg == "last":
        daily = df.groupby("Date", as_index=True)[metric].last()
    elif agg == "sum":
        daily = df.groupby("Date", as_index=True)[metric].sum(min_count=1)
    elif agg == "mean":
        daily = df.groupby("Date", as_index=True)[metric].mean()
    elif agg == "max":
        daily = df.groupby("Date", as_index=True)[metric].max()
    elif agg == "min":
        daily = df.groupby("Date", as_index=True)[metric].min()
    else:
        # fallback na "last" ako je zadano nešto drugo
        daily = df.groupby("Date", as_index=True)[metric].last()

    daily = daily.rename(sheet).to_frame()

    # outer join da se širi skup datuma i stupaca
    pivot = daily if pivot.empty else pivot.join(daily, how="outer")

# 4) Uredi indeks (datumi) i opcijski spremi
pivot = pivot.sort_index(ascending=False)  # padajući datumi
# pivot.to_excel("pivot_output.xlsx", merge_cells=False)
# pivot.to_csv("pivot_output.csv")

print(pivot.shape)
print(pivot.head(10))

import os
import matplotlib.pyplot as plt

# 1) SVE DIONICE NA JEDNOM GRAFU (oprez: može biti nepregledno)
pivot.sort_index().plot(legend=False)
plt.title("Vrijednosti kroz vrijeme - sve dionice")
plt.xlabel("Datum")
plt.ylabel("Vrijednost")
plt.tight_layout()
plt.show()

# 2) PO JEDAN GRAF PO DIONICI (preporučeno)
out_dir = "plots_by_ticker"
os.makedirs(out_dir, exist_ok=True)

for col in pivot.columns:
    s = pivot[col].dropna().sort_index()
    if s.empty:
        continue
    # (opcija) downsample na tjedno da bude preglednije:
    # s = s.resample("W").median()
    plt.figure()
    s.plot()
    plt.title(f"{col} — vrijednost kroz vrijeme")
    plt.xlabel("Datum")
    plt.ylabel("Vrijednost")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, f"{col}.png"), dpi=150)
    plt.close()
