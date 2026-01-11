import pandas as pd
import os
from rich import print as pretty_print


"""
| Kategoria           | Zmienne |
| ------------------- | ------- |
| Mobilność (m)       | C6, C7  |
| Usługi (s)          | C3, C4  |
| Produkcja (p)       | C2, C5  |
| Granice (x)         | C8      |
| Praca (opcjonalnie) | C1      |

"""

DATA_FOLDER = os.path.join(os.path.dirname(__file__), "data")

RESTRICTIONS_DATA = os.path.join(DATA_FOLDER, "OxCGRT_compact_national_v1.csv")
CLEAN_RESTRICTIONS_DATA = os.path.join(DATA_FOLDER, "oxcgrt_clean.csv")

def cleanup(df):
    id_cols = ["CountryName", "CountryCode", "Date"]

    policy_cols = [
        "C1M_School closing",
        "C2M_Workplace closing",
        "C3M_Cancel public events",
        "C4M_Restrictions on gatherings",
        "C5M_Close public transport",
        "C6M_Stay at home requirements",
        "C7M_Restrictions on internal movement",
        "C8EV_International travel controls",
    ]

    econ_support_cols = [
        "E1_Income support",
        "E2_Debt/contract relief",
        "E3_Fiscal measures",
        "E4_International support",
    ]

    # Składam listę kolumn do zachowania, ale tylko tych, które faktycznie istnieją w df
    keep = [c for c in (id_cols + policy_cols + econ_support_cols) if c in df.columns]

    df_clean = df.loc[:, keep].copy()

    # --- Wyrzucam wiersze z brakującą datą / krajem
    df_clean = df_clean.dropna(subset=["CountryCode", "Date"])

    # --- Konwersja policy do numeric (czasem przychodzą jako tekst)
    for c in df_clean.columns:
        if c not in id_cols:
            df_clean[c] = pd.to_numeric(df_clean[c], errors="coerce")

    df_clean.to_csv(CLEAN_RESTRICTIONS_DATA, index=False)
    pretty_print(df_clean)

    #--- Normalizacja restrykcji
    for column in policy_cols:
        max_value = df_clean[column].max()
        pretty_print(column, max_value)
        df_clean[column] = df_clean[column] / max_value
    
    #--- Tworzymy rok
    df_clean["Year"] = df_clean["Date"].astype(str).str[:4].astype(int)

    filter_policy_cols = df_clean.columns.difference(["CountryName", "CountryCode", "Date", "Year"])
    annual_df_clean = (
        df_clean
        .groupby(["CountryName", "Year"])[filter_policy_cols]
        .mean()
        .reset_index()
    )
    annual_df_clean.to_csv(CLEAN_RESTRICTIONS_DATA, index=False)
    pretty_print(annual_df_clean)


if __name__ == "__main__":
    df = pd.read_csv(RESTRICTIONS_DATA)
    cleanup(df)
