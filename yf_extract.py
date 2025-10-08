import os
import pandas as pd
import yfinance as yf

def extract_to_csv(csv_path: str, symbols_env="YF_SYMBOLS"):
    end   = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=200)

    symbols = [s.strip().upper() for s in os.environ.get(symbols_env, "MSFT,ORCL").split(",") if s.strip()]
    frames = []
    for sym in symbols:
        df = yf.download(sym, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or df.empty:
            continue
        df = df.reset_index()
        df.columns = [str(c).upper() for c in df.columns]
        dt_col = "DATE" if "DATE" in df.columns else ("DATETIME" if "DATETIME" in df.columns else None)
        if not dt_col:
            continue

        keep = ["OPEN","HIGH","LOW","CLOSE","VOLUME"]
        for k in keep:
            if k not in df.columns:
                df[k] = pd.NA

        out = pd.DataFrame({
            "SYMBOL": sym,
            "DATE":   pd.to_datetime(df[dt_col], errors="coerce").dt.date,
            "OPEN":   pd.to_numeric(df["OPEN"], errors="coerce"),
            "HIGH":   pd.to_numeric(df["HIGH"], errors="coerce"),
            "LOW":    pd.to_numeric(df["LOW"], errors="coerce"),
            "CLOSE":  pd.to_numeric(df["CLOSE"], errors="coerce"),
            "VOLUME": pd.to_numeric(df["VOLUME"], errors="coerce").astype("Int64")
        }).dropna(subset=["DATE"])
        frames.append(out)

    final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["SYMBOL","DATE","OPEN","HIGH","LOW","CLOSE","VOLUME"]
    )
    final.sort_values(["SYMBOL","DATE"], inplace=True, ignore_index=True)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    final.to_csv(csv_path, index=False)
