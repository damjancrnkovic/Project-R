import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
import os
from datetime import datetime

# --- Configuration ---
# Define the lengths of your estimation and event windows (in trading days)
# Based on the paper: 59-day estimation, 21-day event window (-10 to +10)
ESTIMATION_DAYS = 59
EVENT_WINDOW_PRE = 10  # Days before event
EVENT_WINDOW_POST = 10 # Days after event
EVENT_WINDOW_LEN = EVENT_WINDOW_PRE + EVENT_WINDOW_POST + 1

# --- File Definitions ---
EVENTS_FILE = 'xlsx\\inserted-deleted.xlsx' # <-- File path updated
MARKET_INDEX_FILE = 'XZAG-IndexHistory-HRZB00ICBEX6-2010-01-01 - 2025-11-03.csv' # <-- File path updated
STOCK_FILE_PREFIX = 'sve_dionice_merged_EUR_filled.xlsx'

# Load all sheet names (each ticker)
sheets = pd.ExcelFile(STOCK_FILE_PREFIX).sheet_names

# Dictionary: {ticker: dataframe}
stocks_dict = {ticker: pd.read_excel(STOCK_FILE_PREFIX, sheet_name=ticker) for ticker in sheets}

def load_and_preprocess_market_data(filepath):
    """Loads and processes the market index data."""
    try:
        market_df = pd.read_csv(filepath, delimiter=';', decimal=',')
    except FileNotFoundError:
        print(f"--- ERROR ---")
        print(f"Market index file not found: {filepath}")
        print("Please add this file to the script's directory and re-run.")
        print("---------------------------------------------------------")
        return None

    market_df.columns = market_df.columns.str.strip('"')
    # Ensure 'date' and 'last_value' exist
    if 'date' not in market_df.columns or 'last_value' not in market_df.columns:
        print(f"Market file missing required columns: found {market_df.columns.tolist()}")
        return None

    market_df['Date'] = pd.to_datetime(market_df['date']).dt.normalize()
    market_df = market_df.set_index('Date').sort_index()
    market_df['market_return'] = np.log(market_df['last_value'] / market_df['last_value'].shift(1))
    return market_df[['market_return']].dropna()

# --- Load full stock universe once into memory ---
STOCK_MASTER = None

def load_stock_master():
    global STOCK_MASTER
    if STOCK_MASTER is None:
        print("Loading full stock dataset into memory...")
        STOCK_MASTER = pd.read_excel("sve_dionice_merged_EUR_filled.xlsx")

        # Normalize date just like market data
        STOCK_MASTER['Date'] = pd.to_datetime(STOCK_MASTER['Date']).dt.normalize()

        # Sort for safety
        STOCK_MASTER.sort_values(by=['Symbol', 'Date'], inplace=True)
    return STOCK_MASTER


def load_and_preprocess_stock_data(ticker):
    """Loads and filters the master stock file for one ticker."""
    stocks = load_stock_master()

    # Filter only rows for this ticker
    stock_df = stocks[stocks['Symbol'] == ticker].copy()

    if stock_df.empty:
        print(f"Warning: No data found for ticker {ticker}. Skipping.")
        return None

    # Set index and calculate log returns
    stock_df = stock_df.set_index('Date').sort_index()

    if 'Last Price' not in stock_df.columns:
        print(f"Error: 'Last Price' column missing for ticker {ticker}.")
        return None

    stock_df['stock_return'] = np.log(stock_df['Last Price'] / stock_df['Last Price'].shift(1))

    return stock_df[['stock_return']].dropna()

def calculate_event_car(event_date, ticker, market_data):
    """
    Performs the event study for a single stock and a single event.
    Returns a DataFrame indexed by Event Day (-EVENT_WINDOW_PRE .. +EVENT_WINDOW_POST)
    with columns ['abnormal_return','car'], or None if it cannot be computed.
    """
    stock_data = load_and_preprocess_stock_data(ticker)
    if stock_data is None:
        return None

    # Align by index (Date). Use inner join so only trading days common to both remain.
    # Both stock_data and market_data are indexed by Date.
    data = stock_data.join(market_data, how='inner')
    if data.empty:
        print(f"Warning: No overlapping trading days for {ticker} and market index. Skipping.")
        return None

    # Locate the event day: first trading day on or after event_date
    # Use get_indexer with method='backfill' (alias 'bfill' not used) — stable across pandas versions.
    idx = data.index.get_indexer([event_date], method='backfill')[0]
    if idx == -1:
        print(f"Warning: Event date {event_date.date()} for {ticker} is outside its trading range. Skipping.")
        return None
    event_date_loc = int(idx)

    # Define windows
    event_win_start_loc = event_date_loc - EVENT_WINDOW_PRE
    event_win_end_loc = event_date_loc + EVENT_WINDOW_POST

    # Estimation window: end = (event_win_start - gap_day - 1). Gap of 1 trading day before event window.
    gap_days = 1
    est_win_end_loc = event_win_start_loc - gap_days - 1
    est_win_start_loc = est_win_end_loc - ESTIMATION_DAYS + 1  # inclusive, so +1

    # Bounds checks
    if est_win_start_loc < 0 or event_win_end_loc >= len(data):
        print(f"Warning: Not enough data for {ticker} around event {event_date.date()}. Skipping.")
        return None

    # Extract windows (make copies before mutating)
    estimation_data = data.iloc[est_win_start_loc : est_win_end_loc + 1].copy()
    event_data = data.iloc[event_win_start_loc : event_win_end_loc + 1].copy()

    # Confirm lengths
    if len(estimation_data) != ESTIMATION_DAYS:
        print(f"Warning: Estimation window length mismatch for {ticker} (got {len(estimation_data)} rows). Skipping.")
        return None
    if len(event_data) != EVENT_WINDOW_LEN:
        print(f"Warning: Event window for {ticker} is incomplete ({len(event_data)} days). Skipping.")
        return None

    # Prepare regression data and drop any NaNs
    Y = estimation_data['stock_return'].dropna()
    X = estimation_data['market_return'].dropna()
    # Align Y and X index (in case some NaNs differ)
    reg_df = pd.concat([Y, X], axis=1).dropna()
    if reg_df.shape[0] < 20:  # require a reasonable minimum number of obs
        print(f"Warning: Not enough observations ({reg_df.shape[0]}) in estimation window for {ticker}. Skipping.")
        return None

    X_with_const = sm.add_constant(reg_df['market_return'])
    try:
        model = sm.OLS(reg_df['stock_return'], X_with_const).fit()
    except Exception as e:
        print(f"Warning: OLS failed for {ticker} on {event_date.date()}: {e}. Skipping.")
        return None

    # Extract params safely
    alpha = float(model.params.get('const', 0.0))
    beta = float(model.params.get('market_return', 0.0))

    # Compute expected/abnormal returns on event window (use available market_return rows)
    event_data = event_data.copy()
    # if there are NaNs in market_return or stock_return in event window, they will produce NaNs in AR — that's OK.
    event_data['expected_return'] = alpha + (beta * event_data['market_return'])
    event_data['abnormal_return'] = event_data['stock_return'] - event_data['expected_return']
    event_data['car'] = event_data['abnormal_return'].cumsum()

    # Standardize index to Event Day: -EVENT_WINDOW_PRE ... +EVENT_WINDOW_POST
    event_data.reset_index(drop=True, inplace=True)
    event_data.index = event_data.index - EVENT_WINDOW_PRE
    event_data.index.name = 'Event Day'

    return event_data[['abnormal_return', 'car']]

def parse_stock_list(stock_str):
    """Parses the semi-colon separated stock tickers."""
    if pd.isna(stock_str):
        return []
    # Split by semicolon, strip whitespace, and remove empty strings
    return [ticker.strip() for ticker in stock_str.split(';') if ticker.strip()]

def plot_caar(caar_df, title):
    """Plots the final Cumulative Average Abnormal Return."""
    plt.figure(figsize=(12, 6))
    plt.plot(caar_df.index, caar_df['caar'], marker='o', markersize=4)
    plt.axvline(x=0, color='r', linestyle='--', linewidth=1, label='Event Day (0)')
    plt.axhline(y=0, color='k', linestyle='-', linewidth=0.5)
    plt.title(title, fontsize=16)
    plt.xlabel('Event Day Relative to Announcement', fontsize=12)
    plt.ylabel('Cumulative Average Abnormal Return (CAAR)', fontsize=12)
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.legend()
    # Set x-ticks to be integers
    plt.xticks(range(-EVENT_WINDOW_PRE, EVENT_WINDOW_POST + 1, 2))
    plt.tight_layout()
    
    # Save the plot
    filename = title.lower().replace(' ', '_') + '.png'
    plt.savefig(filename)
    print(f"Plot saved as {filename}")
    plt.show()

# --- Main Execution ---
def main():
    print("Starting CAR Event Study Analysis...")
    
    # 1. Load Market Data
    market_data = load_and_preprocess_market_data(MARKET_INDEX_FILE)
    if market_data is None:
        return # Stop execution if market data is missing

    # 2. Load Event Data
    try:
        # Updated to read the .xlsx file directly
        events_df = pd.read_excel(EVENTS_FILE)
    except FileNotFoundError:
        print(f"--- ERROR ---")
        print(f"Events file not found: {EVENTS_FILE}")
        print("---------------------------------------------------------")
        return
        
    # Clean the date column and parse
    # Normalize to remove time component
    events_df['Datum objave'] = pd.to_datetime(events_df['Datum objave'], dayfirst=True).dt.normalize()

    all_inserted_results = []
    all_deleted_results = []

    # 3. Iterate through each event
    print(f"Processing {len(events_df)} events...")
    for _, event in events_df.iterrows():
        event_date = event['Datum objave']
        
        # Process Inserted Stocks
        inserted_stocks = parse_stock_list(event['Uključeni'])
        for ticker in inserted_stocks:
            res = calculate_event_car(event_date, ticker, market_data)
            if res is not None:
                all_inserted_results.append(res['abnormal_return'])
        
        # Process Deleted Stocks
        deleted_stocks = parse_stock_list(event['Isključeni'])
        for ticker in deleted_stocks:
            res = calculate_event_car(event_date, ticker, market_data)
            if res is not None:
                all_deleted_results.append(res['abnormal_return'])

    if not all_inserted_results and not all_deleted_results:
        print("No valid event data could be processed. Exiting.")
        print("Please check file names, dates, and data ranges.")
        return

    # 4. Aggregate and Plot Results
    print("\n--- Analysis Complete ---")

    # --- INSERTED STOCKS ---
    if all_inserted_results:
        # Concatenate all Abnormal Return series into a single DataFrame
        aar_inserted_df = pd.concat(all_inserted_results, axis=1)
        # Calculate Average Abnormal Return (AAR) for each day
        aar_inserted_df['aar'] = aar_inserted_df.mean(axis=1)
        # Calculate Cumulative Average Abnormal Return (CAAR)
        aar_inserted_df['caar'] = aar_inserted_df['aar'].cumsum()
        
        print(f"\nResults for Inserted Stocks ({len(all_inserted_results)} events):")
        print(aar_inserted_df[['aar', 'caar']])
        plot_caar(aar_inserted_df, 'CAAR for Stocks Inserted into CROBEX')
    else:
        print("\nNo valid data found for 'Inserted' stocks.")

    # --- DELETED STOCKS ---
    if all_deleted_results:
        # Concatenate all Abnormal Return series into a single DataFrame
        aar_deleted_df = pd.concat(all_deleted_results, axis=1)
        # Calculate Average Abnormal Return (AAR) for each day
        aar_deleted_df['aar'] = aar_deleted_df.mean(axis=1)
        # Calculate Cumulative Average Abnormal Return (CAAR)
        aar_deleted_df['caar'] = aar_deleted_df['aar'].cumsum()
        
        print(f"\nResults for Deleted Stocks ({len(all_deleted_results)} events):")
        print(aar_deleted_df[['aar', 'caar']])
        plot_caar(aar_deleted_df, 'CAAR for Stocks Deleted from CROBEX')
    else:
        print("\nNo valid data found for 'Deleted' stocks.")


if __name__ == "__main__":
    main()