# load_bronze_sales_to_db.py
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv


def main():
    load_dotenv()

    schema = os.getenv("DB_SCHEMA", "bronze")
    table = os.getenv("DB_TABLE", "sales")
    load_mode = os.getenv("LOAD_MODE", "replace").lower()

    data_dir = os.getenv("DATA_DIR", "data")
    bronze_file = os.getenv("BRONZE_FILE", "bronze_sales.csv")
    bronze_path = os.path.join(data_dir, "bronze", bronze_file)

    # Lê bronze e normaliza tipos
    df = pd.read_csv(bronze_path, dtype=str, keep_default_na=True)

    # order_id -> int (nullable)
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").astype("Int64")

    # total -> numeric
    df["total"] = pd.to_numeric(df["total"], errors="coerce")

    # order_date já vem ISO string; se vier lixo, vira NaT->NaN->None
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
    )
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            order_id BIGINT,
            order_date DATE,
            total NUMERIC(12,2),
            customer_name TEXT
        );
    """)

    if load_mode == "replace":
        cur.execute(f"TRUNCATE TABLE {schema}.{table};")

    insert_sql = f"""
        INSERT INTO {schema}.{table} (order_id, order_date, total, customer_name)
        VALUES (%s, %s, %s, %s);
    """

    rows = []
    for _, r in df.iterrows():
        order_id = None if pd.isna(r["order_id"]) else int(r["order_id"])
        order_date = None if pd.isna(r["order_date"]) else r["order_date"]  # date object
        total = None if pd.isna(r["total"]) else float(r["total"])
        customer_name = r.get("customer_name")
        if isinstance(customer_name, str) and customer_name.strip() == "":
            customer_name = None

        rows.append((order_id, order_date, total, customer_name))

    cur.executemany(insert_sql, rows)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Load concluído com sucesso em {schema}.{table} (mode={load_mode})")
    print(f"Linhas inseridas: {len(rows)}")


if __name__ == "__main__":
    main()



COMANDOS (resumão em 6 linhas)

py -3.11 -m venv .venv
.venv\Scripts\activate
pip install pandas psycopg2-binary python-dotenv
python ingest_bronze_sales.py
python load_bronze_sales_to_db.py
git add . && git commit -m "Bronze 5: sales pipeline" && git push
