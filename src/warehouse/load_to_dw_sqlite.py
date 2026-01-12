import sqlite3
import pandas as pd
import logging
from pathlib import Path

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log"),
        logging.StreamHandler()
    ]
)

# Base project path
BASE_PATH = Path(__file__).resolve().parents[2]

# Input integrated dataset
INTEGRATED_PATH = BASE_PATH / "data/staging/integrated/affordability_integrated.csv"


# SQLite data warehouse path
DW_PATH = BASE_PATH / "data/warehouse/dw.sqlite"


def create_tables(conn: sqlite3.Connection):
    # Create DW tables if they do not exist
    cur = conn.cursor()

    # Date dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE
    );
    """)

    # Community dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_community (
        community_key INTEGER PRIMARY KEY AUTOINCREMENT,
        autonomous_community_key TEXT UNIQUE,
        autonomous_community_name TEXT
    );
    """)

    # Fact table with main metrics
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_affordability (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date_key INTEGER,
        community_key INTEGER,
        avg_salary_eur REAL,
        employment_rate REAL,
        avg_price_m2_eur REAL,
        avg_rent_m2_eur REAL,
        affordability_index REAL,
        salary_to_rent_ratio REAL,
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (community_key) REFERENCES dim_community(community_key)
    );
    """)

    conn.commit()


def load_dimensions(conn: sqlite3.Connection, df: pd.DataFrame):
    # Load date dimension
    dates = df[["date"]].drop_duplicates().sort_values("date")
    dates.to_sql("tmp_dates", conn, if_exists="replace", index=False)

    conn.execute("""
    INSERT OR IGNORE INTO dim_date(date)
    SELECT date FROM tmp_dates;
    """)
    conn.execute("DROP TABLE tmp_dates;")
    conn.commit()

    # Prepare community dimension
    comm = df[["autonomous_community_key"]].drop_duplicates()

    # Try to find a readable community name column
    rep_col = None
    for candidate in ["autonomous_community_raw_sal", "autonomous_community_raw"]:
        if candidate in df.columns:
            rep_col = candidate
            break

    # Select a representative name (most common value)
    if rep_col:
        rep = (
            df.groupby("autonomous_community_key")[rep_col]
              .agg(lambda x: x.dropna().astype(str).mode().iloc[0]
                   if not x.dropna().empty else None)
              .reset_index()
              .rename(columns={rep_col: "autonomous_community_name"})
        )
    else:
        
        rep = comm.copy()
        rep["autonomous_community_name"] = rep["autonomous_community_key"]

    comm = comm.merge(rep, on="autonomous_community_key", how="left")
    comm.to_sql("tmp_comm", conn, if_exists="replace", index=False)

    conn.execute("""
    INSERT OR IGNORE INTO dim_community(autonomous_community_key, autonomous_community_name)
    SELECT autonomous_community_key, autonomous_community_name FROM tmp_comm;
    """)
    conn.execute("DROP TABLE tmp_comm;")
    conn.commit()
    




def load_facts(conn: sqlite3.Connection, df: pd.DataFrame):
    # Read dimension keys
    dim_date = pd.read_sql_query("SELECT date_key, date FROM dim_date", conn)
    dim_comm = pd.read_sql_query(
        "SELECT community_key, autonomous_community_key FROM dim_community", conn
    )

    # Join integrated data with dimensions
    fact = (
        df.merge(dim_date, on="date", how="left")
          .merge(dim_comm, on="autonomous_community_key", how="left")
    )


    # Select fact table columns
    fact_out = fact[
        [
            "date_key", "community_key",
            "avg_salary_eur", "employment_rate",
            "avg_price_m2_eur", "avg_rent_m2_eur",
            "affordability_index", "salary_to_rent_ratio"
        ]
    ].copy()

    # Clear fact table to avoid duplicated loads
    conn.execute("DELETE FROM fact_affordability;")
    fact_out.to_sql("fact_affordability", conn, if_exists="append", index=False)
    conn.commit()


if __name__ == "__main__":
    # Load integrated dataset
    logging.info("Reading integrated dataset...")
    df = pd.read_csv(INTEGRATED_PATH)

    # Ensure DW folder exists
    DW_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to SQLite data warehouse
    logging.info(f"Connecting to DW: {DW_PATH}")
    
    conn = sqlite3.connect(DW_PATH)

    # Create schema and load data
    logging.info("Creating tables...")
    create_tables(conn)

    logging.info("Loading dimensions...")
    load_dimensions(conn, df)

    logging.info("Loading fact table...")
    load_facts(conn, df)

    logging.info("DW loaded successfully.")
    conn.close()
