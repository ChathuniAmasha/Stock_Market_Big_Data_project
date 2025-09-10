import logging
from google.cloud import storage
import pandas as pd
import numpy as np
from io import BytesIO
from xgboost import XGBRegressor
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
import joblib

# Optional: make logs show up clearly in Cloud Logging
logging.getLogger().setLevel(logging.INFO)

# --- Utility: load CSV from GCS ---
def load_csv_from_gcs(bucket_name, file_path):
    logging.info(f"Loading file {file_path} from bucket {bucket_name}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    if not blob.exists():
        raise FileNotFoundError(f"{file_path} not found in bucket {bucket_name}")
    data = blob.download_as_bytes()
    logging.info(f"Downloaded {len(data)} bytes from GCS")
    df = pd.read_csv(BytesIO(data), parse_dates=["timestamp"])
    return df

# --- Utility: save CSV to GCS ---
def save_csv_to_gcs(bucket_name, file_path, df):
    logging.info(f"Saving CSV to {bucket_name}/{file_path}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    csv_data = df.to_csv(index=False)
    blob.upload_from_string(csv_data, "text/csv")
    logging.info(f"Saved CSV with shape {df.shape}")

# --- Utility: save model to GCS ---
def save_model_to_gcs(bucket_name, file_path, model):
    logging.info(f"Saving model to {bucket_name}/{file_path}")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    model_bytes = BytesIO()
    joblib.dump(model, model_bytes)
    model_bytes.seek(0)
    blob.upload_from_file(model_bytes, content_type="application/octet-stream")
    logging.info("Model saved successfully")

# --- Correlation ---
def compute_correlation(df, drop_cols):
    features = [col for col in df.columns if col not in drop_cols]
    corr = df[features].corr(method="pearson")
    logging.info("Correlation matrix computed")
    return corr

# --- Granger causality ---
def compute_granger(df, target_col, predictors, maxlag=5):
    results = []
    logging.info(f"Running Granger causality for {len(predictors)} predictors")
    for predictor in predictors:
        try:
            subset = df[[target_col, predictor]].dropna()
            if len(subset) < (maxlag + 2):
                raise ValueError("Not enough observations for Granger test")
            test = grangercausalitytests(subset, maxlag=maxlag, verbose=False)
            for lag in range(1, maxlag + 1):
                pval = round(test[lag][0]["ssr_ftest"][1], 5)
                results.append({"predictor": predictor, "lag": lag, "p_value": pval})
        except Exception as e:
            logging.warning(f"Granger test failed for {predictor}: {e}")
            results.append({
                "predictor": predictor,
                "lag": None,
                "p_value": np.nan,
                "error": str(e)
            })
    return pd.DataFrame(results)

# --- Forecasting with SARIMAX ---
def train_and_forecast(df, drop_cols, target_col="c", horizon=168, n_lags=24):
    # Use only lag features of 'c'
    for lag in range(1, n_lags+1):
        df[f"{target_col}_lag{lag}"] = df[target_col].shift(lag)
    df = df.dropna().reset_index(drop=True)

    features = [col for col in df.columns if col.startswith(f"{target_col}_lag")]
    X = df[features]
    y = df[target_col]

    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = XGBRegressor(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    logging.info(f"XGBoost Test RMSE: {rmse}")

    # Forecast iteratively using predicted values only
    last_lags = list(X.iloc[-1].values)  # latest lag values
    forecasts = []
    for _ in range(horizon):
        pred = model.predict([last_lags])[0]
        forecasts.append(pred)
        # update lag list
        last_lags = [pred] + last_lags[:-1]

    forecast_df = pd.DataFrame({
        "step": range(1, horizon+1),
        "forecast_c": forecasts
    })
    forecast_df["rmse"] = rmse

    return model, forecast_df


# --- Main entrypoint for Cloud Function ---
def run_analysis(request):
    try:
        bucket_name = "stock-project-cleaned-data"
        file_path = "integrated_data/integrated_all.csv"

        logging.info("Starting analysis pipeline")
        df = load_csv_from_gcs(bucket_name, file_path)
        logging.info(f"Loaded dataframe with shape {df.shape}")

        required_cols = {"timestamp", "symbol", "c"}
        missing = required_cols - set(df.columns)
        if missing:
            raise KeyError(f"Missing required columns: {missing}")

        # Ensure chronological order
        df = df.sort_values("timestamp")

        drop_cols = ["timestamp", "symbol", "symbol_y", "ret_1h", "ret_1h_next","Close","Open","High","Low","t","d"]
        companies = df["symbol"].dropna().unique()
        logging.info(f"Found companies: {companies}")

        for company in companies:
            logging.info(f"Processing company: {company}")
            df_c = df[df["symbol"] == company].copy()

            # Correlation
            corr = compute_correlation(df_c, drop_cols)
            save_csv_to_gcs(bucket_name, f"results/{company}_correlation.csv", corr)

            # Causation
            predictors = [col for col in df_c.columns if col not in drop_cols + ["c"]]
            causality_df = compute_granger(df_c, "c", predictors)
            save_csv_to_gcs(bucket_name, f"results/{company}_causality.csv", causality_df)

            # Forecast with SARIMAX
            model, forecast_df = train_and_forecast(df_c, drop_cols, target_col="c", horizon=168)
            save_csv_to_gcs(bucket_name, f"results/{company}_forecast.csv", forecast_df)
            save_model_to_gcs(bucket_name, f"results/{company}_sarimax.pkl", model)

        logging.info("Analysis complete for all companies")
        return "Analysis complete. Results saved to GCS."

    except Exception as e:
        logging.exception("Error in run_analysis")
        return f"Error: {str(e)}"