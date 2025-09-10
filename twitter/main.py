import snscrape.modules.twitter as sntwitter
from google.cloud import firestore
import datetime
import functions_framework  # Only needed for Cloud Functions Gen 2

@functions_framework.http
def fetch_twitter_data(request):
    symbols = ['AAPL', 'MSFT', 'AMZN', 'TSLA']
    db = firestore.Client()

    for symbol in symbols:
        try:
            query = f'{symbol} stock'
            tweets = []
            for i, tweet in enumerate(sntwitter.TwitterSearchScraper(query).get_items()):
                if i >= 50:
                    break
                tweets.append({
                    'content': tweet.content,
                    'username': tweet.user.username,
                    'date': tweet.date.isoformat()
                })

            db.collection('tweets').add({
                'symbol': symbol,
                'tweets': tweets,
                'timestamp': datetime.datetime.utcnow()
            })

        except Exception as e:
            print(f"Error while fetching tweets for {symbol}: {str(e)}")

    return 'Twitter data fetch attempted for all symbols.'
