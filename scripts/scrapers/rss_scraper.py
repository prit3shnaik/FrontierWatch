import feedparser
import requests
from datetime import datetime, timedelta

class RSSScraper:
    def __init__(self):
        self.feeds = {
            'hindu': 'https://www.thehindu.com/feeder/default.rss',
            'toi': 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms',
            'india_today': 'https://www.indiatoday.in/rss/1206578',
            'business_standard': 'https://www.business-standard.com/rss/home'
        }
    
    def scrape_feeds(self, hours_back=6):
        """Scrape RSS feeds for incidents"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours_back)
        
        all_entries = []
        for name, url in self.feeds.items():
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries:
                    pub_date = entry.get('published_parsed')
                    if pub_date and datetime(*pub_date[:6]) > start_time:
                        all_entries.append({
                            'title': entry.title,
                            'summary': entry.get('summary', ''),
                            'url': entry.link,
                            'published': entry.published if 'published' in entry else '',
                            'source': name.title(),
                            'region': self._classify_region(entry.title + ' ' + entry.summary)
                        })
            except Exception as e:
                print(f"RSS {name} error: {e}")
        
        return all_entries[:100]  # Limit results
    
    def _classify_region(self, text):
        """Classify RSS content by region"""
        text = text.lower()
        jk_words = ['jammu', 'kashmir', 'j&k']
        ne_words = ['manipur', 'nagaland', 'assam', 'mizoram', 'tripura', 'meghalaya', 'arunachal']
        
        if any(word in text for word in jk_words):
            return 'Jammu & Kashmir'
        elif any(word in text for word in ne_words):
            return 'North East'
        return 'Other'
