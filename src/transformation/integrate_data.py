import pandas as pd
import logging
from pathlib import Path

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Base project path
BASE_PATH = Path(__file__).resolve().parents[2]

# Input STAGING files
SAL_CLEAN = BASE_PATH / "data/staging/salaries/salaries_clean.csv"
HOU_CLEAN = BASE_PATH / "data/staging/housing/housing_clean.csv"

# Output INTEGRATED file
OUT_INT = BASE_PATH / "data/staging/integrated/affordability_integrated.csv"

if __name__ == "__main__":
    # Read cleaned datasets from staging
    logging.info("Reading STAGING datasets...")
    sal = pd.read_csv(SAL_CLEAN)
    hou = pd.read_csv(HOU_CLEAN)

    # Merge both datasets using date and normalized community key
    logging.info("Integrating datasets (JOIN)...")
    df = pd.merge(
        sal,
        hou,
        on=["date", "autonomous_community_key"],
        how="inner",
        suffixes=("_sal", "_hou")
    )

    # Log how many rows remain after the join
    logging.info(f"Integrated rows: {len(df)}")

    # Create affordability-related metrics
    logging.info("Creating derived metrics...")
    df["affordability_index"] = df["avg_salary_eur"] / df["avg_price_m2_eur"]
    df["salary_to_rent_ratio"] = df["avg_salary_eur"] / (df["avg_rent_m2_eur"] * 80)

    # Ensure output folder exists and save result
    OUT_INT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_INT, index=False)

    logging.info(f"Saved integrated dataset: {OUT_INT}")
