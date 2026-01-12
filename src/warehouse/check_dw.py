import sqlite3
from pathlib import Path

BASE_PATH = Path(__file__).resolve().parents[2]
DW_PATH = BASE_PATH / "data/warehouse/dw.sqlite"

conn = sqlite3.connect(DW_PATH)
cur = conn.cursor()

print("dim_date:", cur.execute("SELECT COUNT(*) FROM dim_date;").fetchone()[0])
print("dim_community:", cur.execute("SELECT COUNT(*) FROM dim_community;").fetchone()[0])
print("fact_affordability:", cur.execute("SELECT COUNT(*) FROM fact_affordability;").fetchone()[0])

conn.close()
