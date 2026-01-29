import tweepy
import json
from datetime import datetime, timedelta

class TwitterScraper:
    def __init__(self, bearer_token):
        self.bearer_token = bearer_token
        self.client = None
    
    def authenticate(self):
        """Authenticate with Twitter API v2"""
        try:
            self.client = tweepy.Client(bearer_token=self.bearer_token)
            return True
        except Exception as e:
            print(f"Twitter auth error: {e}")
            return False
    
    def search_tweets(self, hours_back=6):
        """Search recent terror-related tweets"""
        if not self.client:
            if not self.authenticate():
                return []
        
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        query = 'terror OR terrorist OR encounter OR militant OR attack OR explosion OR IED OR ambush OR infiltration OR "ceasefire violation" OR gunfight (Jammu OR Kashmir OR Manipur OR Nagaland OR Assam OR Mizoram OR Tripura OR Meghalaya OR Arunachal) -is:retweet lang:en'
        
        tweets = []
        try:
            response = self.client.search_recent_tweets(
                query=query,
                max_results=100,
                start_time=start_time,
                end_time=end_time,
                tweet_fields=['created_at', 'author_id', 'public_metrics']
            )
            
            if response.data:
                for tweet in response.data:
                    tweets.append({
                        'text': tweet.text,
                        'created': tweet.created_at.isoformat(),
                        'author_id': str(tweet.author_id),
                        'likes': tweet.public_metrics['like_count'],
                        'retweets': tweet.public_metrics['retweet_count'],
                        'region': self._classify_region(tweet.text)
                    })
        except Exception as e:
            print(f"Twitter search error: {e}")
        
        return tweets
    
    def _classify_region(self, text):
        """Classify tweet by region"""
        text = text.lower()
        if any(word in text for word in ['jammu', 'kashmir']):
            return 'Jammu & Kashmir'
        elif any(state in text for state in ['manipur', 'nagaland', 'assam', 'mizoram', 'tripura', 'meghalaya', 'arunachal']):
            return 'North East'
        return 'Other'
