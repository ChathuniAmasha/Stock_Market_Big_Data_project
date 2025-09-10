from pytrends.request import TrendReq
from google.cloud import firestore
import datetime

def fetch_google_trends(request):
    try:
        pytrends = TrendReq()
        db = firestore.Client()

        keywords = ['AAPL stock', 'MSFT stock', 'TSLA stock', 'AMZN stock']
        pytrends.build_payload(keywords, timeframe='now 1-d')

        trends = pytrends.interest_over_time()

        if not trends.empty:
            latest = trends.iloc[-1].to_dict()
            db.collection('google_trends').add({
                'trends': latest,
                'timestamp': datetime.datetime.utcnow()
            })
            return 'Google Trends data stored'
        else:
            return 'No trend data found'

    except Exception as e:
        return f'Error occurred: {str(e)}'