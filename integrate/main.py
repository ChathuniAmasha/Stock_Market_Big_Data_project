# main.py
from google.cloud import storage
import pandas as pd
import os
import io
import re
from typing import List, Tuple

def load_csv_from_bucket(bucket, blob_name):
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"Blob not found: {blob_name}")
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data))

def find_trend_mappings(trends_df: pd.DataFrame, symbols: List[str]) -> List[Tuple[str,str]]:
    mappings = []
    trend_cols = [c for c in trends_df.columns if c.lower() != "timestamp"]
    for col in trend_cols:
        for sym in symbols:
            if sym.lower() in col.lower():
                mappings.append((col, sym))
                break
    if not mappings and trend_cols:
        for col in trend_cols:
            token = re.split(r'\s|[:\-]', col.strip())[0]
            if re.fullmatch(r'[A-Z]{1,5}', token):
                mappings.append((col, token))
    return mappings

def integrate_data(request):
    BUCKET_NAME = os.environ.get("BUCKET_NAME")
    if not BUCKET_NAME:
        return ("Environment variable BUCKET_NAME not set", 500)

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Paths of cleaned data
    stock_path  = "cleaned_data/cleaned_stock.csv"
    yahoo_path  = "cleaned_data/cleaned_yahoo.csv"
    trends_path = "cleaned_data/cleaned_trends.csv"
    fred_path   = "cleaned_data/cleaned_fred.csv"

    # Load data
    stock_df  = load_csv_from_bucket(bucket, stock_path)
    try: yahoo_df = load_csv_from_bucket(bucket, yahoo_path)
    except: yahoo_df = pd.DataFrame()
    try: trends_df = load_csv_from_bucket(bucket, trends_path)
    except: trends_df = pd.DataFrame()
    try: fred_df = load_csv_from_bucket(bucket, fred_path)
    except: fred_df = pd.DataFrame()

    # Parse timestamps
    for df in [stock_df, yahoo_df, trends_df, fred_df]:
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.floor("H")

    # Filter from given timestamp
    cutoff = pd.Timestamp("2025-08-03 05:00:00+00:00")
    stock_df = stock_df[stock_df["timestamp"] >= cutoff]
    if not yahoo_df.empty:
        yahoo_df = yahoo_df[yahoo_df["timestamp"] >= cutoff]
    if not trends_df.empty:
        trends_df = trends_df[trends_df["timestamp"] >= cutoff]
    if not fred_df.empty:
        fred_df = fred_df[fred_df["timestamp"] >= cutoff]

    # FRED pivot
    if (not fred_df.empty) and {"indicator","value","timestamp"}.issubset(set(fred_df.columns)):
        fred_wide = fred_df.pivot_table(index="timestamp", columns="indicator", values="value", aggfunc="last").sort_index()
        fred_hourly = fred_wide.resample("1H").ffill().reset_index()
    else:
        fred_hourly = pd.DataFrame()

    # Trends long-form
    trends_long = pd.DataFrame()
    if not trends_df.empty:
        symbols = stock_df["symbol"].dropna().unique().astype(str).tolist()
        mappings = find_trend_mappings(trends_df, symbols)
        if mappings:
            rows = []
            for col, sym in mappings:
                tmp = trends_df[["timestamp", col]].rename(columns={col: "trend_score"})
                tmp["symbol"] = sym
                rows.append(tmp)
            trends_long = pd.concat(rows, ignore_index=True)

        if not trends_long.empty:
            trends_long["timestamp"] = pd.to_datetime(trends_long["timestamp"], utc=True).dt.floor("H")

    combined_frames = []
    symbols = stock_df["symbol"].dropna().unique().astype(str).tolist()

    for sym in symbols:
        try:
            s = stock_df[stock_df["symbol"] == sym].set_index("timestamp").sort_index()
            merged = s
            if not yahoo_df.empty:
                y = yahoo_df[yahoo_df["symbol"] == sym].set_index("timestamp").sort_index()
                merged = merged.join(y, how="left", rsuffix="_y")
            if not trends_long.empty:
                t = trends_long[trends_long["symbol"] == sym].set_index("timestamp").sort_index()
                merged = merged.join(t[["trend_score"]], how="left")
            if not fred_hourly.empty:
                f = fred_hourly.set_index("timestamp").sort_index()
                merged = merged.join(f, how="left")

            merged = merged.reset_index()
            if "c" in merged.columns:
                merged = merged[merged["c"].notnull()]

            # Daily mean imputation for numeric columns
            merged["date"] = merged["timestamp"].dt.date
            for col in merged.select_dtypes(include=["float64","int64"]).columns:
                merged[col] = merged.groupby("date")[col].transform(lambda x: x.fillna(x.mean()))
            merged.drop(columns=["date"], inplace=True)

            if "c" in merged.columns:
                merged["ret_1h"] = merged["c"].astype(float).pct_change()
                merged["ret_1h_next"] = merged["ret_1h"].shift(-1)

            out_path = f"integrated_data/{sym}.csv"
            bucket.blob(out_path).upload_from_string(merged.to_csv(index=False), content_type="text/csv")

            combined_frames.append(merged.assign(symbol=sym))
        except Exception as e:
            print(f"Error integrating symbol {sym}: {e}")

    if combined_frames:
        integrated_all = pd.concat(combined_frames, ignore_index=True)
        out_all = "integrated_data/integrated_all.csv"
        bucket.blob(out_all).upload_from_string(integrated_all.to_csv(index=False), content_type="text/csv")

    return ("Integration completed successfully.", 200)