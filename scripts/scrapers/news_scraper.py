import requests
import json
from datetime import datetime, timedelta
import yaml

class NewsScraper:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
    
    def search_incidents(self, hours_back=6):
        """Search for terror incidents in target regions"""
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(hours=hours_back)
        
        params = {
            'q': 'terror OR terrorist OR encounter OR militant OR attack OR explosion OR IED OR ambush OR infiltration OR "ceasefire violation" OR gunfight (Jammu OR Kashmir OR Manipur OR Nagaland OR Assam OR Mizoram OR Tripura OR Meghalaya OR "Arunachal Pradesh")',
            'from': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'to': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 100,
            'apiKey': self.api_key
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()['articles']
        except Exception as e:
            print(f"NewsAPI error: {e}")
            return []
    
    def filter_relevant(self, articles):
        """Filter articles by region relevance"""
        jk_keywords = ['Jammu', 'Kashmir', 'J&K', 'Srinagar']
        ne_keywords = ['Manipur', 'Nagaland', 'Assam', 'Mizoram', 'Tripura', 'Meghalaya', 'Arunachal']
        
        relevant = []
        for article in articles:
            title = article.get('title', '').lower()
            content = article.get('description', '').lower()
            
            if any(kw in title or kw in content for kw in jk_keywords + ne_keywords):
                relevant.append({
                    'title': article['title'],
                    'description': article['description'],
                    'url': article['url'],
                    'published': article['publishedAt'],
                    'source': article['source']['name'],
                    'region': self._classify_region(title + ' ' + content)
                })
        return relevant
    
    def _classify_region(self, text):
        """Simple region classification"""
        text = text.lower()
        if any(word in text for word in ['jammu', 'kashmir', 'j&k', 'srinagar']):
            return 'Jammu & Kashmir'
        elif any(state in text for state in ['manipur', 'nagaland', 'assam', 'mizoram', 'tripura', 'meghalaya', 'arunachal']):
            return 'North East'
        return 'Other'
