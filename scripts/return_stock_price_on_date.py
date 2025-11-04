import pandas as pd
import numpy as np

# -----------------------
# FILE PATHS
# -----------------------
events_file = "xlsx//inserted-deleted.xlsx"
index_file = "XZAG-IndexHistory-HRZB00ICBEX6-2010-01-01 - 2025-11-03.csv"
stocks_file = "sve_dionice_merged_EUR_filled.xlsx"


# -----------------------
# LOAD EVENTS & CLEAN DATES
# -----------------------
events = pd.read_excel(events_file)

# Normalize date format for "Datum objave"
events["Datum objave"] = (
    events["Datum objave"]
    .astype(str)
    .str.strip()
    .str.replace(r'\.$', '', regex=True)
    .pipe(pd.to_datetime, format="%d.%m.%Y", dayfirst=True, errors="coerce")
)

# Clean the "Prvi dan trgovanja nakon provedbe" column
events["Prvi dan trgovanja nakon provedbe"] = (
    events["Prvi dan trgovanja nakon provedbe"]
    .astype(str)
    .str.strip()
    .str.replace(r'\.$', '', regex=True)
    .pipe(pd.to_datetime, format="%d.%m.%Y", dayfirst=True, errors="coerce")
)


# Split tickers in "Uključeni" & "Isključeni" columns into lists
def parse_ticker_list(x):
    if isinstance(x, str):
        return [t.strip() for t in x.split(",") if t.strip()]
    return []

events["included_tickers"] = events["Uključeni"].apply(parse_ticker_list)
events["excluded_tickers"] = events["Isključeni"].apply(parse_ticker_list)


# -----------------------
# LOAD INDEX (CROBEX)
# -----------------------
index = pd.read_csv(index_file, sep=";", decimal=",")

index["date"] = pd.to_datetime(index["date"], errors="coerce")
index = index[["date", "last_value"]].rename(columns={"last_value": "index_close"})


# -----------------------
# LOAD STOCKS — ONE SHEET PER TICKER
# -----------------------
xls = pd.ExcelFile(stocks_file)
sheet_names = xls.sheet_names

stocks = {}

for ticker in sheet_names:
    df = pd.read_excel(stocks_file, sheet_name=ticker)

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Standardize price column name possibilities
    if "last price" in df.columns:
        df.rename(columns={"last price": "close"}, inplace=True)
    if "close" not in df.columns:
        raise ValueError(f"Ticker {ticker}: no close price column found")

    stocks[ticker] = df[["date", "close", "volume"]]


# -----------------------
# HELPER: GET PRICE ON / AFTER EVENT DATE
# -----------------------
def get_stock_price_on_or_after(ticker, date):
    """Return first trading price >= date"""
    if ticker not in stocks:
        return None
    
    df = stocks[ticker]
    row = df[df["date"] >= date].sort_values("date").head(1)
    return row.iloc[0] if not row.empty else None


def get_index_on_or_after(date):
    row = index[index["date"] >= date].sort_values("date").head(1)
    return row.iloc[0] if not row.empty else None


info = get_stock_price_on_or_after("ARNT", pd.Timestamp("2010-01-05"))
print(info)