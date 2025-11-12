# FILE ZA GENERIRANJE GRAFA CAR-A U NEKOM WINDOWU S EFFECTIVE DATE U SREDINI
# PROMJENA TRAJANJA WINDOWA - window = x
# dogadjaji_path = INSERTIONS_EVENT.csv / DELETIONS_EVENT.csv


import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

file_path = "sve_dionice_merged_EUR_filled.xlsx"
dogadaji_path = "INSERTIONS_EVENT.csv" # ili DELETIONS_EVENT.csv
ime_benchmarka = "CBX"
window = 50     # ± broj trgovačkih dana
output_dir = "testiranje_car_skripta"
os.makedirs(output_dir, exist_ok=True)

wb = pd.ExcelFile(file_path)    # u wb lista svih sheetova/tickera
cbx = wb.parse(ime_benchmarka)  # parsiranje cbx sheeta
cbx.columns = cbx.columns.str.strip()   # parsiranje kolona
cbx["Date"] = pd.to_datetime(cbx["Date"], errors="coerce")
cbx = cbx.sort_values("Date").reset_index(drop=True)    # cbx sortiran po datumima

# print(cbx["Date"].isna().sum()) # koliko redova je Na - 0, dobro je

cijena_kolona_cbx = "Last Price"

cbx["Return"] = cbx[cijena_kolona_cbx].pct_change(fill_method=None)
# print(cbx[["Date", "Last Price", "Return"]].tail(100))

dogadaji = pd.read_csv(dogadaji_path, dtype=str)
dogadaji["EventDate"] = pd.to_datetime(dogadaji["EventDate"].str.strip(), format="%Y-%m-%d", errors="coerce")

# print(dogadaji)   # DOBRO

rezultati = []             # STRUKTURA ZA POHRANU - ZAPAMTI!
sve_car_podaci = []        # STRUKTURA ZA POHRANU - ZAPAMTI!

for idx_dogadaj, red in dogadaji.iterrows():    # iterira red po red po dogadajima
    # idx dogadaj - broj redka
    # red - dataframe s Symbol, EventDate  (iz csv-a)
    ime_dionice = red["Symbol"].strip()
    datum_dogadaja = red["EventDate"]
    
    if pd.isna(datum_dogadaja):
        print(f"PAZI: DATUM JE Na ZA DIONICU {ime_dionice}")
        continue
    
    # Ako nema tickera u sheet nameovima, trazimo pomocu sufiksa -R-A jer za neke dionice ima samo taj sheet u excelici
    if ime_dionice not in wb.sheet_names:
        ime_dionice_alt = ime_dionice.replace("-R-A", "").strip()
        if ime_dionice_alt in wb.sheet_names:
            ime_dionice = ime_dionice_alt
        else:
            print(f"NEMA TICKERA ZA DIONICU {ime_dionice}")
            continue

    stock = wb.parse(ime_dionice)   # objekt tipa ExcelFile u varijabli stock
    stock.columns = stock.columns.str.strip()
    stock["Date"] = pd.to_datetime(stock["Date"], errors="coerce")
    stock = stock.sort_values("Date").reset_index(drop=True)

    # print(stock[["Date","Symbol","Last Price"]].head(5))
    # if(idx_dogadaj > 5):
    #     break                 # ISPROBAN ISPIS - DOBRO

    cijena_kolona = "Last Price"
    stock["Return"] = stock[cijena_kolona].pct_change(fill_method=None)
    # print(stock[["Date","Symbol","Last Price", "Return"]].head(5))
    # if(idx_dogadaj > 5):
    #     break           


    # OBAVEZNO POKUSAJTE GENERIRATI ZA DELETIONS_EVENT.csv s ovim i bez ovog skipa
    # ispada da je to ogroman outlier
    # if (stock["Symbol"] == 'DDJH').any():
    #     continue


    exact_match = stock[stock["Date"] == datum_dogadaja]
    if exact_match.empty:
        print(f"Nema cijene za dionicu {ime_dionice}, datum: {datum_dogadaja}")
        continue   # POPRAVI, TU JE ZBOG SUNH-R-A,2016-09-19
    
    event_idx = exact_match.index[0]    # NOTE: AKO ZELIMO POMAKNUTI UNATRAG, EVENT_IDX -= 1, UZET CE RADNI DAN PRIJE REBALANSA 
    event_date_actual = stock.loc[event_idx, "Date"]
    # print(event_idx)
    # print(event_date_actual)
    # break

    start_idx = max(event_idx - window, 0)      
    end_idx = min(event_idx + window, len(stock) - 1)

    stock_window = stock.iloc[start_idx:end_idx+1].copy()   
    # BITNO: stock_window je novi dataframe od 2 * window + 1 redaka, s event_idx u sredini 
    # print(len(stock_window))
    # break

    # postavljanje prozora za benchmark cbx
    pocetak = stock["Date"].min()
    kraj = stock["Date"].max()
    cbx_window = cbx[(cbx["Date"] >= pocetak) & (cbx["Date"] <= kraj)].copy()

    # print(cbx_window[["Date","Last Price", "Return"]].head(20))
    # break

    merged = pd.merge(
        stock_window[["Date", cijena_kolona, "Return"]],
        cbx_window[["Date", cijena_kolona, "Return"]],
        on="Date", suffixes=("_stock", "_cbx"), how="inner"
    )

    # print(merged.head(10))  
    # break

    # VAŽNO: RAČUNANJE AR, CAR - DOBRO
    merged["AR"] = merged["Return_stock"] - merged["Return_cbx"]
    merged["AR"] = merged["AR"].replace([np.inf, -np.inf], np.nan).fillna(0)
    merged["CAR"] = merged["AR"].cumsum()

    # print(merged.head(15))
    # break

    merged = merged.reset_index(drop=True)  
    event_pos_in_merged = merged[merged["Date"] == event_date_actual].index     # ne treba previse gledati, za svaki slucaj ovdje
    event_row_number = event_pos_in_merged[0]
    merged["DayOffset"] = range(-event_row_number, len(merged) - event_row_number)
    # VAŽNO: dataframeu pridružena kolona DayOffset kao dan u odnosu na effective date

    # print(merged.head(15))
    # break

    # SPREMANJE REZULTATA - DOBRO ZA PREGLEDAVANJE
    # out_path = os.path.join(output_dir, f"{ime_dionice}_CAR.csv")
    # merged_output = merged.copy()
    # merged_output["EventDateOriginal"] = datum_dogadaja.date()
    # merged_output["EventDateActual"] = event_date_actual.date()
    # merged_output.to_csv(out_path, index=False)

    car_kraj = merged['CAR'].iloc[-1]

    # KONTROLNI ISPIS ZA PRAĆENJE TOKA
    # print(f"Iteracija: {idx_dogadaj}, Dionica: {ime_dionice}, Datum dogadaja: {datum_dogadaja.date()}, CAR_kraj={car_kraj:.4f}")
    
    if car_kraj > 0.05 or car_kraj < -0.05:
        print(f"PAZI, {ime_dionice}: Event={event_date_actual.date()} , CAR_kraj={car_kraj:.4f}, CAR_event={merged.loc[merged['DayOffset']==0, 'CAR'].values[0]:.4f}")

    # print(f"[OK] {ime_dionice}: Event={event_date_actual.date()} (original: {datum_dogadaja.date()}), {len(merged)} dana, CAR_kraj={merged['CAR'].iloc[-1]:.4f}, CAR_event={merged.loc[merged['DayOffset']==0, 'CAR'].values[0]:.4f}")

    # STRUKTURA ZA REZULTAT
    car_za_prosjek = merged[["DayOffset", "CAR", "AR"]].copy()
    car_za_prosjek["Symbol"] = ime_dionice
    sve_car_podaci.append(car_za_prosjek)   # JEDNA OD POČETNE DVIJE

    rezultati.append({
        "Symbol": ime_dionice,
        "EventDateOriginal": datum_dogadaja.date(),
        "EventDateActual": event_date_actual.date(),
        "DaysData": len(merged),
        "CAR_total": merged["CAR"].iloc[-1]
    })

    if ime_dionice == 'RIVP':
        out_path = os.path.join(output_dir, f"{ime_dionice}_CAR.csv")
        merged_output = merged.copy()
        merged_output["EventDateOriginal"] = datum_dogadaja.date()
        merged_output["EventDateActual"] = event_date_actual.date()
        merged_output.to_csv(out_path, index=False)

# VAŽNO - stvara se df iz rjecnika
df_rez = pd.DataFrame(rezultati)
df_rez.to_csv(os.path.join(output_dir, "rezultati_pojedinacni.csv"), index=False)
# print(f"\n[OK] Rezultati spremljeni ({len(df_rez)} dionice)")
if sve_car_podaci:
    combined = pd.concat(sve_car_podaci, ignore_index=True)
    
    avg_car = combined.groupby("DayOffset").agg({
        "CAR": ["mean", "std", "count"],
        "AR": ["mean"]
    }).reset_index()
    
    avg_car.columns = ["DayOffset", "CAR_mean", "CAR_std", "Count", "AR_mean"]
    avg_car = avg_car.sort_values("DayOffset")
    
    avg_car.to_csv(os.path.join(output_dir, "CAR_prosjek.csv"), index=False)
    print(f"[OK] Prosjecni CAR spremljen ({len(avg_car)} dana)")

    # Grafikon
    plt.figure(figsize=(12, 6))
    plt.plot(avg_car["DayOffset"], avg_car["CAR_mean"], marker='o', linewidth=2, label="Prosjecni CAR")
    plt.axvline(0, color='red', linestyle='--', linewidth=2, label='Event datum (day 0)')
    plt.axhline(0, color='gray', linestyle='-', linewidth=0.5, alpha=0.5)
    plt.title(f"Prosjecni kumulativni abnormalni prinos (CAR)\nn={len(rezultati)} events", fontsize=12, fontweight='bold')
    plt.xlabel("Trgovacki dani oko event datuma (Dan 0 = event)", fontsize=11)
    plt.ylabel("Prosječni CAR", fontsize=11)
    plt.legend(loc='best')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "CAR_prosjek.png"), dpi=150)
    plt.show()
    
    if len(avg_car[avg_car['DayOffset']==0]) > 0:
        print(f"\n[GRAF] CAR na dan event (day 0): {avg_car[avg_car['DayOffset']==0]['CAR_mean'].values[0]:.4f}")
    if len(avg_car[avg_car['DayOffset']==15]) > 0:
        print(f"[GRAF] CAR 15 dana nakon (day +15): {avg_car[avg_car['DayOffset']==15]['CAR_mean'].values[0]:.4f}")
    if len(avg_car[avg_car['DayOffset']==-15]) > 0:
        print(f"[GRAF] CAR 15 dana prije (day -15): {avg_car[avg_car['DayOffset']==-15]['CAR_mean'].values[0]:.4f}")
else:
    print("Nema podataka za prosječni CAR.")
