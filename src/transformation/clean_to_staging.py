import pandas as pd
import logging
import unicodedata
from pathlib import Path

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base project path
BASE_PATH = Path(__file__).resolve().parents[2]

# Input RAW files
RAW_SAL = BASE_PATH / "data/raw/salaries/salaries_by_community_es_70.csv"
RAW_HOU = BASE_PATH / "data/raw/housing/housing_by_community_es_70.csv"

# Output STAGING files
OUT_SAL = BASE_PATH / "data/staging/salaries/salaries_clean.csv"
OUT_HOU = BASE_PATH / "data/staging/housing/housing_clean.csv"


def normalize_text(s: str) -> str:
    # Normalize text to create a clean join key
    if pd.isna(s):
        return s
    s = str(s).strip().lower()
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    return s


def clean_common(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    # Common cleaning logic shared by both datasets
    df = df.copy()

    # Standardize date and community fields
    df["date"] = df["date"].astype(str).str.strip()
    df["autonomous_community_raw"] = df["autonomous_community"]
    df["autonomous_community_key"] = df["autonomous_community"].apply(normalize_text)

    # Remove duplicated rows and log how many were dropped
    before = len(df)
    
    df = df.drop_duplicates()
    logging.info(f"{dataset_name}: dropped {before - len(df)} duplicate rows")

    return df


def clean_salaries(df: pd.DataFrame) -> pd.DataFrame:
    # Clean salaries dataset
    df = clean_common(df, "Salaries")


    # Convert numeric columns safely
    df["avg_salary_eur"] = pd.to_numeric(df["avg_salary_eur"], errors="coerce")
    df["employment_rate"] = pd.to_numeric(df["employment_rate"], errors="coerce")


    # Fill missing salary values using the median per community
    df["avg_salary_eur"] = df.groupby("autonomous_community_key")["avg_salary_eur"].transform(
        lambda x: x.fillna(x.median())
    )

    return df


def clean_housing(df: pd.DataFrame) -> pd.DataFrame:
    # Clean housing dataset
    df = clean_common(df, "Housing")

    # Convert numeric columns safely
    df["avg_price_m2_eur"] = pd.to_numeric(df["avg_price_m2_eur"], errors="coerce")

    df["avg_rent_m2_eur"] = pd.to_numeric(df["avg_rent_m2_eur"], errors="coerce")

    # Fill missing housing prices using the median per community
    df["avg_price_m2_eur"] = df.groupby("autonomous_community_key")["avg_price_m2_eur"].transform(
        lambda x: x.fillna(x.median())
    )

    # Cap extreme values using the 99th percentile
    def cap_p99(x):
        p99 = x.quantile(0.99)
        return x.clip(upper=p99)

    df["avg_price_m2_eur"] = df.groupby("autonomous_community_key")["avg_price_m2_eur"].transform(cap_p99)


    return df


if __name__ == "__main__":
    # Read raw input datasets
    logging.info("Reading RAW datasets...")
    salaries = pd.read_csv(RAW_SAL)
    housing = pd.read_csv(RAW_HOU)


    # Apply cleaning logic
    logging.info("Cleaning Salaries -> Staging")
    salaries_clean = clean_salaries(salaries)


    logging.info("Cleaning Housing -> Staging")
    housing_clean = clean_housing(housing)

    # Ensure output folders exist
    OUT_SAL.parent.mkdir(parents=True, exist_ok=True)
    OUT_HOU.parent.mkdir(parents=True, exist_ok=True)

    # Save cleaned datasets
    salaries_clean.to_csv(OUT_SAL, index=False)
    housing_clean.to_csv(OUT_HOU, index=False)

    logging.info(f"Saved salaries staging file: {OUT_SAL}")
    logging.info(f"Saved housing staging file: {OUT_HOU}")
