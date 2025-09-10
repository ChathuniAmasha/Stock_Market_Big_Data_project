from fredapi import Fred
from google.cloud import firestore
import datetime
import os

def fetch_fred_data(request):
    fred_api_key = os.environ.get("FRED_API_KEY")
    fred = Fred(api_key=fred_api_key)
    db = firestore.Client()

    indicators = ['GDP', 'CPIAUCSL', 'UNRATE']  # GDP, Inflation, Unemployment

    for ind in indicators:
        series_data = fred.get_series(ind, start_date='2023-01-01')  # or use an earlier start if needed

        if series_data is not None and not series_data.empty:
            latest_value = series_data.iloc[-1]  # get the most recent value
        else:
            latest_value = None

        db.collection('fred_data').add({
            'indicator': ind,
            'value': latest_value,
            'timestamp': datetime.datetime.utcnow()
        })

    return 'FRED data stored successfully.'
