import yfinance as yf
from google.cloud import firestore
import datetime

def fetch_yfinance_data(request):
    symbols = ['AAPL', 'MSFT', 'AMZN', 'TSLA']
    db = firestore.Client()

    for symbol in symbols:
        stock = yf.Ticker(symbol)
        hist = stock.history(period='1d')

        if not hist.empty:
            latest = hist.iloc[-1].to_dict()
            db.collection('yfinance_data').add({
                'symbol': symbol,
                'data': latest,
                'timestamp': datetime.datetime.utcnow()
            })

    return 'Yahoo Finance data stored'