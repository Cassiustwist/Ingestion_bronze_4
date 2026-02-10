# BRONZE 5 — Pipeline CSV (Sales) → Postgres (RAW → BRONZE → DB)

Objetivo: repetir o fluxo completo com um RAW “sujo” e validar rápido.

RAW (CSV) → INGEST (limpeza/padronização) → BRONZE (CSV) → LOAD (Postgres)

---

## 1) Estrutura do projeto (na raiz do repo)

.
├── data/
│   ├── raw/
│   │   └── sales.csv
│   └── bronze/
│       └── bronze_sales.csv
├── ingest_bronze_sales.py
├── load_bronze_sales_to_db.py
├── requirements.txt
├── .env               (NÃO sobe)
├── .env.example       (sobe)
└── .gitignore         (sobe)

---

## 2) Criar pastas

data/raw
data/bronze

---

## 3) Criar o RAW sujo

Arquivo: data/raw/sales.csv

order id , Order Date , Total $ , customer name
  1001 , 2026-02-10 , $1,250.50 , ana SILVA
1002, 10/02/2026 ,  300 , BRUNO costa
1003 , 2026/02/10, $ 99.9, CARLA  souza
1004, , $2.000,00 , diego lima
1005, 2026-02-11, invalid , fernanda alves

Regras de limpeza (Bronze):
- Colunas padronizadas: order_id, order_date, total, customer_name
- customer_name: trim, espaços duplicados, Title Case
- order_date: aceitar formatos YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD (vazio → NULL)
- total: aceitar $ e separadores (1,250.50 | 2.000,00 | 300) e invalid → NULL

---

## 4) Venv + dependências

py -3.11 -m venv .venv
.venv\Scripts\activate

pip install pandas psycopg2-binary python-dotenv
pip freeze > requirements.txt

---

## 5) .env (local)

DATA_DIR=data

DB_HOST=localhost
DB_PORT=5432
DB_NAME=empregadados_local
DB_USER=postgres
DB_PASS=SUA_SENHA_AQUI

DB_SCHEMA=bronze
DB_TABLE=sales

RAW_FILE=sales.csv
BRONZE_FILE=bronze_sales.csv

# replace = TRUNCATE antes de inserir | append = só insere
LOAD_MODE=replace

---

## 6) .env.example (para subir)

DATA_DIR=data

DB_HOST=localhost
DB_PORT=5432
DB_NAME=empregadados_local
DB_USER=postgres
DB_PASS=CHANGE_ME

DB_SCHEMA=bronze
DB_TABLE=sales

RAW_FILE=sales.csv
BRONZE_FILE=bronze_sales.csv

LOAD_MODE=replace

---

## 7) .gitignore (para subir)

.venv/
__pycache__/
.env
data/
*.log
.ipynb_checkpoints/

---

## 8) Rodar INGEST (RAW → BRONZE)

python ingest_bronze_sales.py

Saída esperada:
data/bronze/bronze_sales.csv

Validação rápida (não abre milhões de linhas):
- conferir só head e contagens no print do script

---

## 9) Rodar LOAD (BRONZE → Postgres)

python load_bronze_sales_to_db.py

Validação rápida no banco:
SELECT COUNT(*) FROM bronze.sales;
SELECT * FROM bronze.sales LIMIT 10;
SELECT COUNT(*) FROM bronze.sales WHERE total IS NULL;

---

## 10) Git (subir só o que deve)

git add .
git commit -m "Bronze 5: sales pipeline (ingest + load)"
git push


# ingest_bronze_sales.py
import os
from pathlib import Path
import re
import pandas as pd
from dotenv import load_dotenv


def standardize_column_name(col: str) -> str:
    col = str(col).strip().lower()
    col = re.sub(r"\s+", " ", col)
    col = col.replace(" ", "_")
    col = re.sub(r"[^a-z0-9_]", "", col)
    return col


def normalize_whitespace(x):
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s if s else pd.NA


def clean_customer_name(x):
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA
    return str(x).title()


def clean_order_id(x):
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA
    try:
        return int(float(str(x)))
    except Exception:
        return pd.NA


def clean_order_date(x):
    """
    Aceita: YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD (vazio -> NA)
    Retorna string ISO YYYY-MM-DD (facilita salvar CSV e carregar no Postgres)
    """
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA

    s = str(x)

    # tenta formatos comuns
    for dayfirst in (False, True):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
        if not pd.isna(dt):
            return dt.date().isoformat()

    return pd.NA


def clean_total(x):
    """
    Aceita:
      "$1,250.50" (US)
      "$2.000,00" (BR)
      "300"
      "invalid" -> NA
    Retorna float (ou NA)
    """
    x = normalize_whitespace(x)
    if pd.isna(x):
        return pd.NA

    s = str(x)
    s = s.replace("$", "").replace(" ", "")

    # Se tem vírgula e ponto, decidir pelo último separador como decimal
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            # BR: 2.000,00 -> 2000.00
            s = s.replace(".", "").replace(",", ".")
        else:
            # US: 1,250.50 -> 1250.50
            s = s.replace(",", "")
    else:
        # Se só tem vírgula, assumir decimal BR: 99,9 -> 99.9
        if "," in s and "." not in s:
            s = s.replace(".", "").replace(",", ".")
        # Se só tem ponto, ok

    try:
        return float(s)
    except Exception:
        return pd.NA


def main():
    load_dotenv()

    data_dir = Path(os.getenv("DATA_DIR", "data"))
    raw_file = os.getenv("RAW_FILE", "sales.csv")
    bronze_file = os.getenv("BRONZE_FILE", "bronze_sales.csv")

    raw_path = data_dir / "raw" / raw_file
    bronze_path = data_dir / "bronze" / bronze_file
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        raise FileNotFoundError(f"RAW não encontrado: {raw_path.resolve()}")

    df = pd.read_csv(raw_path, dtype=str, skipinitialspace=True)
    df.columns = [standardize_column_name(c) for c in df.columns]

    # aliases simples
    rename_map = {
        "order_id": "order_id",
        "orderid": "order_id",
        "order_date": "order_date",
        "orderdate": "order_date",
        "total": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "total": "total",
        "total__": "total",
        "total_": "total",
        "total__": "total",
        "customer_name": "customer_name",
        "customername": "customer_name",
    }
    df = df.rename(columns={c: rename_map.get(c, c) for c in df.columns})

    expected = {"order_id", "order_date", "total", "customer_name"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas faltando no RAW: {missing}. Encontradas: {list(df.columns)}")

    # Limpeza Bronze
    df["order_id"] = df["order_id"].apply(clean_order_id).astype("Int64")
    df["order_date"] = df["order_date"].apply(clean_order_date)
    df["total"] = df["total"].apply(clean_total)
    df["customer_name"] = df["customer_name"].apply(clean_customer_name)

    # Remove linhas totalmente vazias (segurança)
    df = df.dropna(how="all")

    df.to_csv(bronze_path, index=False)
    print(f"✅ Bronze gerado em: {bronze_path.resolve()}")
    print("Amostra (head):")
    print(df.head(10))
    print("\nNulos por coluna:")
    print(df.isna().sum())


if __name__ == "__main__":
    main()


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
