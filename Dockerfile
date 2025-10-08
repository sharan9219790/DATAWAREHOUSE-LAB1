FROM apache/airflow:2.10.1-python3.12
USER airflow
RUN pip install --no-cache-dir yfinance pandas 'snowflake-connector-python[pandas]'

