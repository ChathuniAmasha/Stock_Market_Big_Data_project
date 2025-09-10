import requests
from google.cloud import firestore
import datetime
import os

def fetch_finnhub_data(request):
    api_key = os.environ.get("FINNHUB_API_KEY")
    print("API Key:", api_key) 
    
    symbols = ['AAPL', 'MSFT', 'AMZN', 'TSLA']  # List of companies(apple, microsoft, amazon, tesla)
    db = firestore.Client()

    for symbol in symbols:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={api_key}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            db.collection('stock_quotes').add({
                'symbol': symbol,
                'data': data,
                'timestamp': datetime.datetime.utcnow()
            })
        else:
            print(f"Failed to fetch data for {symbol}: {response.text}")

    return 'Data for all symbols fetched and stored successfully.'