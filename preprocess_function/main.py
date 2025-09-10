from google.cloud import firestore, storage
import pandas as pd
import datetime
import os

def preprocess_data(request):
    db = firestore.Client()

    # ====== 1. Stock Data (Finnhub) ======
    stock_docs = db.collection('stock_quotes').stream()
    stock_data = [doc.to_dict() for doc in stock_docs]
    stock_df = pd.DataFrame(stock_data)
    stock_df['timestamp'] = pd.to_datetime(stock_df['timestamp'], errors='coerce')
    stock_df.drop_duplicates(inplace=True)
    stock_df.dropna(inplace=True)

    # ====== 2. FRED Data ======
    fred_docs = db.collection('fred_data').stream()
    fred_data = [doc.to_dict() for doc in fred_docs]
    fred_df = pd.DataFrame(fred_data)
    fred_df['timestamp'] = pd.to_datetime(fred_df['timestamp'], errors='coerce')
    fred_df.drop_duplicates(inplace=True)
    fred_df.dropna(inplace=True)

    # ====== 3. Google Trends Data ======
    trends_docs = db.collection('google_trends').stream()
    trends_data = [doc.to_dict() for doc in trends_docs]
    trends_df = pd.DataFrame(trends_data)
    trends_df['timestamp'] = pd.to_datetime(trends_df['timestamp'], errors='coerce')
    trends_df.drop_duplicates(inplace=True)
    trends_df.dropna(inplace=True)

    # ====== 4. Yahoo Data (if exists in firestore) ======
    try:
        yahoo_docs = db.collection('yahoo_data').stream()
        yahoo_data = [doc.to_dict() for doc in yahoo_docs]
        yahoo_df = pd.DataFrame(yahoo_data)
        yahoo_df['timestamp'] = pd.to_datetime(yahoo_df['timestamp'], errors='coerce')
        yahoo_df.drop_duplicates(inplace=True)
        yahoo_df.dropna(inplace=True)
    except Exception as e:
        yahoo_df = pd.DataFrame()

    # ====== Upload to Cloud Storage ======
    bucket_name = os.environ.get("BUCKET_NAME")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    def upload(df, name):
        if not df.empty:
            blob = bucket.blob(f"cleaned_data/{name}.csv")
            blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

    upload(stock_df, "cleaned_stock")
    upload(fred_df, "cleaned_fred")
    upload(trends_df, "cleaned_trends")
    upload(yahoo_df, "cleaned_yahoo")

    return "All data cleaned and uploaded."