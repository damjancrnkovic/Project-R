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
STOCK_FILE_PREFIX = 'sve_dionice_merged_EUR_filled.xlsx - '
STOCK_FILE_SUFFIX = '.csv'

def load_and_preprocess_market_data(filepath):
    """Loads and processes the market index data."""
    try:
        # Updated to handle semicolon delimiter and comma decimal
        market_df = pd.read_csv(filepath, delimiter=';', decimal=',')
    except FileNotFoundError:
        print(f"--- ERROR ---")
        print(f"Market index file not found: {filepath}")
        print("Please add this file to the script's directory and re-run.")
        print("---------------------------------------------------------")
        return None

    # Clean column names (e.g., remove quotes from '"date"')
    market_df.columns = market_df.columns.str.strip('"')

    # Use the correct column names from the new file ('date' and 'last_value')
    # Normalize to remove time component
    market_df['Date'] = pd.to_datetime(market_df['date']).dt.normalize()
    market_df = market_df.set_index('Date').sort_index()
    # Calculate log returns for the market
    market_df['market_return'] = np.log(market_df['last_value'] / market_df['last_value'].shift(1))
    return market_df[['market_return']].dropna()

def load_and_preprocess_stock_data(ticker):
    """Loads and processes a single stock's data."""
    filepath = f"{STOCK_FILE_PREFIX}{ticker}{STOCK_FILE_SUFFIX}"
    if not os.path.exists(filepath):
        print(f"Warning: Stock file not found for ticker: {ticker}. Skipping.")
        return None
        
    stock_df = pd.read_csv(filepath)
    # Normalize to remove time component
    stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.normalize()
    stock_df = stock_df.set_index('Date').sort_index()
    # Calculate log returns for the stock
    stock_df['stock_return'] = np.log(stock_df['Last Price'] / stock_df['Last Price'].shift(1))
    return stock_df[['stock_return']].dropna()

def calculate_event_car(event_date, ticker, market_data):
    """
    Performs the full event study calculation for a single stock and event.
    """
    stock_data = load_and_preprocess_stock_data(ticker)
    if stock_data is None:
        return None

    # 1. Merge stock and market data to align trading days
    data = pd.merge(stock_data, market_data, on='Date', how='inner')

    # 2. Find the event date location
    try:
        # Find the first trading day *on or after* the announcement date
        event_date_loc = data.index.get_loc(event_date, method='bfill')
    except KeyError:
        # Event date is outside the stock's trading range
        print(f"Warning: Event date {event_date.date()} for {ticker} is outside its data range. Skipping.")
        return None
    except Exception as e:
        print(f"Error finding event date for {ticker}: {e}. Skipping.")
        return None

    # 3. Define Estimation and Event window locations
    # Estimation Window
    est_win_end_loc = event_date_loc - EVENT_WINDOW_PRE - 2 # Gap of 1 day
    est_win_start_loc = est_win_end_loc - ESTIMATION_DAYS
    
    # Event Window
    event_win_start_loc = event_date_loc - EVENT_WINDOW_PRE
    event_win_end_loc = event_date_loc + EVENT_WINDOW_POST

    # Check if windows are out of bounds
    if est_win_start_loc < 0 or event_win_end_loc >= len(data):
        print(f"Warning: Not enough data for {ticker} around event {event_date.date()}. Skipping.")
        return None

    # 4. Extract window data
    estimation_data = data.iloc[est_win_start_loc : est_win_end_loc + 1]
    event_data = data.iloc[event_win_start_loc : event_win_end_loc + 1]

    if len(event_data) != EVENT_WINDOW_LEN:
        # This can happen if the event is too close to the start/end of the data
        print(f"Warning: Event window for {ticker} is incomplete ({len(event_data)} days). Skipping.")
        return None

    # 5. Run Market Model Regression (OLS)
    # Y = stock_return, X = market_return
    Y = estimation_data['stock_return']
    X = estimation_data['market_return']
    X_with_const = sm.add_constant(X) # Add constant (alpha)

    model = sm.OLS(Y, X_with_const).fit()
    alpha, beta = model.params['const'], model.params['market_return']

    # 6. Calculate Abnormal Returns (AR)
    event_data['expected_return'] = alpha + (beta * event_data['market_return'])
    event_data['abnormal_return'] = event_data['stock_return'] - event_data['expected_return']

    # 7. Calculate Cumulative Abnormal Returns (CAR)
    event_data['car'] = event_data['abnormal_return'].cumsum()
    
    # Standardize index to -10, -9, ..., 0, ..., +10
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






