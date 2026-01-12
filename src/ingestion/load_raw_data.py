import pandas as pd
import logging
from pathlib import Path

# Simple logging so we can see what happens when we run the script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Base project path (two folders up from this file)
BASE_PATH = Path(__file__).resolve().parents[2]

# Raw data files (we keep them "as they are", no cleaning here)
SALARIES_PATH = BASE_PATH / "data/raw/salaries/salaries_by_community_es_70.csv"
HOUSING_PATH = BASE_PATH / "data/raw/housing/housing_by_community_es_70.csv"


def load_salaries_data():
    #Load the salaries CSV from the raw layer
    logging.info("Loading salaries dataset")

    if not SALARIES_PATH.exists():
        raise FileNotFoundError(f"Raw salaries file not found: {SALARIES_PATH}")

    df = pd.read_csv(SALARIES_PATH)
    logging.info(f"Salaries dataset loaded with {len(df)} rows")
    return df


def load_housing_data():
    #Load the housing CSV from the raw layer.
    logging.info("Loading housing dataset")

    if not HOUSING_PATH.exists():
        raise FileNotFoundError(f"Raw housing file not found: {HOUSING_PATH}")

    df = pd.read_csv(HOUSING_PATH)
    logging.info(f"Housing dataset loaded with {len(df)} rows")
    return df


if __name__ == "__main__":
    # Quick test: load both datasets and check the number of rows
    salaries_df = load_salaries_data()
    housing_df = load_housing_data()
