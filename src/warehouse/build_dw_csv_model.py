import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_PATH = Path(__file__).resolve().parents[2]
INTEGRATED_PATH = BASE_PATH / "data/staging/integrated/affordability_integrated.csv"
OUT_DIR = BASE_PATH / "data/processed"

DIM_DATE_OUT = OUT_DIR / "dim_date.csv"
DIM_COMM_OUT = OUT_DIR / "dim_community.csv"
FACT_OUT = OUT_DIR / "fact_affordability.csv"


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    return df[["date"]].drop_duplicates().sort_values("date").reset_index(drop=True)


def build_dim_community(df: pd.DataFrame) -> pd.DataFrame:
    rep_col = None
    for candidate in ["autonomous_community_raw_sal", "autonomous_community_raw", "autonomous_community_raw_hou"]:
        if candidate in df.columns:
            rep_col = candidate
            break

    dim_comm = df[["autonomous_community_key"]].drop_duplicates()

    if rep_col:
        names = (
            df.groupby("autonomous_community_key")[rep_col]
              .agg(lambda x: x.dropna().astype(str).mode().iloc[0] if not x.dropna().empty else None)
              .reset_index()
              .rename(columns={rep_col: "autonomous_community_name"})
        )
        dim_comm = dim_comm.merge(names, on="autonomous_community_key", how="left")
    else:
        dim_comm["autonomous_community_name"] = dim_comm["autonomous_community_key"]

    return dim_comm.sort_values("autonomous_community_key").reset_index(drop=True)


def build_fact(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "date",
        "autonomous_community_key",
        "avg_salary_eur",
        "employment_rate",
        "avg_price_m2_eur",
        "avg_rent_m2_eur",
        "affordability_index",
        "salary_to_rent_ratio",
    ]
    cols = [c for c in cols if c in df.columns]
    return df[cols].sort_values(["autonomous_community_key", "date"]).reset_index(drop=True)


if __name__ == "__main__":
    logging.info("Reading integrated dataset...")
    df = pd.read_csv(INTEGRATED_PATH)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("Building processed dimensional model (CSV)...")
    dim_date = build_dim_date(df)
    dim_comm = build_dim_community(df)
    fact = build_fact(df)

    dim_date.to_csv(DIM_DATE_OUT, index=False)
    dim_comm.to_csv(DIM_COMM_OUT, index=False)
    fact.to_csv(FACT_OUT, index=False)

    logging.info(f"Saved: {DIM_DATE_OUT}")
    logging.info(f"Saved: {DIM_COMM_OUT}")
    logging.info(f"Saved: {FACT_OUT}")
