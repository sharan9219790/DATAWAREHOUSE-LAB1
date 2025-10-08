# stock_daily_snowflake.py
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import os
import pandas as pd
import yfinance as yf
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------- Config helpers ----------
def ge(*names: str, default: str | None = None) -> str | None:
    """Get first non-empty env var among names."""
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default

SF_ACCOUNT   = ge("SF_ACCOUNT",   "SNOWFLAKE_ACCOUNT")
SF_USER      = ge("SF_USER",      "SNOWFLAKE_USER")
SF_PASSWORD  = ge("SF_PASSWORD",  "SNOWFLAKE_PASSWORD")
SF_WAREHOUSE = ge("SF_WAREHOUSE", "SNOWFLAKE_WAREHOUSE")
SF_DATABASE  = ge("SF_DATABASE",  "SNOWFLAKE_DATABASE")
SF_SCHEMA    = ge("SF_SCHEMA",    "SNOWFLAKE_SCHEMA") or "RAW"

SYMBOLS = os.environ.get("YF_SYMBOLS", os.environ.get("YF_TICKERS", "MSFT,ORCL"))
SYMBOLS = [s.strip().upper() for s in SYMBOLS.split(",") if s.strip()]

TABLE_NAME = os.environ.get("YF_TABLE", "STOCK_YF")
TABLE_FQN  = f"{SF_DATABASE}.{SF_SCHEMA}.{TABLE_NAME}"

# ---------- Snowflake connection ----------
def sf_conn():
    return snowflake.connector.connect(
        account   = SF_ACCOUNT,
        user      = SF_USER,
        password  = SF_PASSWORD,
        warehouse = SF_WAREHOUSE,
        database  = SF_DATABASE,
        schema    = SF_SCHEMA,
    )

# ---------- Core functions ----------
def fetch_last_180_days() -> pd.DataFrame:
    """Fetch 180 days per ticker; flatten yfinance columns; clean dtypes."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=200)

    frames = []
    for t in SYMBOLS:
        # Key fix: avoid MultiIndex by grouping by column
        df = yf.download(
            t,
            start=start,
            end=end,
            progress=False,
            group_by="column",
        ).reset_index()

        # Flatten any MultiIndex/tuple columns defensively
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(x) for x in tup if x != ""]).lower() for tup in df.columns]
        else:
            flat_cols = []
            for c in df.columns:
                if isinstance(c, tuple):
                    flat_cols.append("_".join([str(x) for x in c if x != ""]).lower())
                else:
                    flat_cols.append(str(c).lower())
            df.columns = flat_cols

        # Choose close column (fallback to adj close)
        close_col = "close" if "close" in df.columns else ("adj close" if "adj close" in df.columns else None)
        if not close_col:
            # No usable price columns; skip this symbol
            continue

        keep = ["date", "open", "high", "low", close_col, "volume"]
        keep = [c for c in keep if c in df.columns]
        df = df[keep]

        # Types
        if "date" in df:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        for col in ["open", "high", "low", "close", "adj close"]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "volume" in df:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

        # Ensure final column name is 'close'
        if "close" not in df.columns and "adj close" in df.columns:
            df = df.rename(columns={"adj close": "close"})

        df["symbol"] = t
        df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["symbol","date","open","high","low","close","volume"])

    out = pd.concat(frames, ignore_index=True).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.dropna(subset=["date"])
    return out

def ensure_table():
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_FQN} (
      SYMBOL STRING NOT NULL,
      DATE   DATE   NOT NULL,
      OPEN   NUMBER(18,4),
      HIGH   NUMBER(18,4),
      LOW    NUMBER(18,4),
      CLOSE  NUMBER(18,4),
      VOLUME NUMBER,
      CONSTRAINT PK_STOCK_YF PRIMARY KEY (SYMBOL, DATE)
    );
    """
    with sf_conn() as con, con.cursor() as cur:
        cur.execute(create_sql)

def full_refresh_load():
    """Delete-all + insert fresh dataset."""
    ensure_table()
    df = fetch_last_180_days()

    if df.empty:
        print("No data fetched from yfinance; skipping insert.")
        return

    rows = [
        (
            str(r["symbol"]),
            r["date"],  # datetime.date
            None if pd.isna(r["open"])  else float(r["open"]),
            None if pd.isna(r["high"])  else float(r["high"]),
            None if pd.isna(r["low"])   else float(r["low"]),
            None if pd.isna(r["close"]) else float(r["close"]),
            None if pd.isna(r["volume"]) else int(r["volume"]),
        )
        for _, r in df.iterrows()
    ]

    insert_sql = f"""
      INSERT INTO {TABLE_FQN} (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
      VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with sf_conn() as con, con.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute(f"DELETE FROM {TABLE_FQN}")
            cur.executemany(insert_sql, rows)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

def run_forecast_or_skip():
    """
    Try Snowflake built-in ML FORECAST if present; otherwise, noop with a log.
    """
    with sf_conn() as con, con.cursor() as cur:
        try:
            cur.execute("SHOW FUNCTIONS LIKE 'FORECAST' IN SCHEMA SNOWFLAKE.ML")
            has = cur.fetchall()
        except Exception:
            has = []

        if not has:
            print("SNOWFLAKE.ML.FORECAST not available; skipping forecast step.")
            return

        cur.execute(f"""
            CREATE OR REPLACE TABLE {SF_DATABASE}.{SF_SCHEMA}.STOCK_YF_FORECAST AS
            WITH BASE AS (
                SELECT SYMBOL, DATE, CLOSE
                FROM {TABLE_FQN}
                WHERE CLOSE IS NOT NULL
            ),
            FC AS (
                SELECT
                    SYMBOL,
                    FORECASTED_TIME AS DATE,
                    FORECASTED_VALUE AS CLOSE_FORECAST,
                    LOWER_BOUND AS LCL,
                    UPPER_BOUND AS UCL
                FROM TABLE(SNOWFLAKE.ML.FORECAST(
                    INPUT_DATA => TO_VARIANT(BASE),
                    SERIES_COLS => 'SYMBOL',
                    TIMESTAMP_COL => 'DATE',
                    TARGET_COL => 'CLOSE',
                    HORIZON => 14
                ))
            )
            SELECT * FROM FC
        """)
        print("Forecast table STOCK_YF_FORECAST refreshed.")

# ---------- DAG ----------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_daily_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    default_args=default_args,
    tags=["yfinance", "snowflake"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=fetch_last_180_days,  # we don't push the DF via XCom
    )

    t_ensure_table = PythonOperator(
        task_id="ensure_table",
        python_callable=ensure_table,
    )

    t_load = PythonOperator(
        task_id="load_full_refresh",
        python_callable=full_refresh_load,
    )

    t_forecast = PythonOperator(
        task_id="forecast_task",
        python_callable=run_forecast_or_skip,
    )

    # Tree (linear) dependencies
    t_extract >> t_ensure_table >> t_load >> t_forecast
