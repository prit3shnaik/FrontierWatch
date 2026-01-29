#!/usr/bin/env python3
import os
import sys
import yaml
from datetime import datetime
import pandas as pd

# FIX: Add parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import all modules
from scrapers.news_scraper import NewsScraper
from scrapers.rss_scraper import RSSScraper
from scrapers.twitter_scraper import TwitterScraper
from processors.data_processor import deduplicate_data, classify_incidents
from processors.geocoder import FrontierGeocoder
from analyzers.geo_analyzer import GeoAnalyzer
from notifiers.telegram_notifier import TelegramNotifier

def load_config():
    """Load configuration with secret substitution"""
    import os
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Replace GitHub secrets
    config['newsapi_key'] = os.getenv('NEWSAPI_KEY', '')
    config['twitter_bearer'] = os.getenv('TWITTER_BEARER', '')
    config['telegram_token'] = os.getenv('TELEGRAM_TOKEN', '')
    config['telegram_chat_id'] = os.getenv('TELEGRAM_CHAT_ID', '')
    
    return config

def collect_data(config):
    """Collect data from all sources"""
    all_incidents = []
    
    # NewsAPI
    print("📰 Scraping NewsAPI...")
    if config['newsapi_key']:
        news = NewsScraper(config['newsapi_key'])
        news_articles = news.search_incidents()
        relevant_news = news.filter_relevant(news_articles)
        for article in relevant_news:
            article['source'] = 'NewsAPI'
            all_incidents.append(article)
    else:
        print("⚠️  No NewsAPI key - skipping")
    
    # RSS (no API key needed)
    print("📰 Scraping RSS feeds...")
    rss = RSSScraper()
    rss_incidents = rss.scrape_feeds()
    all_incidents.extend(rss_incidents)
    
    # Twitter
    print("🐦 Scraping Twitter...")
    if config['twitter_bearer']:
        twitter = TwitterScraper(config['twitter_bearer'])
        tweets = twitter.search_tweets()
        for tweet in tweets:
            tweet['source'] = 'Twitter'
            all_incidents.append(tweet)
    else:
        print("⚠️  No Twitter Bearer - skipping")
    
    print(f"✅ Collected {len(all_incidents)} total incidents")
    return pd.DataFrame(all_incidents)

def process_and_analyze(df, config):
    """Process data and generate analysis"""
    os.makedirs('data', exist_ok=True)
    os.makedirs('maps', exist_ok=True)
    os.makedirs('charts', exist_ok=True)
    
    if df.empty:
        print("⚠️  No incidents to process")
        return df, None, {}
    
    # Process
    print("🔄 Processing data...")
    df['published'] = pd.to_datetime(df['published'], errors='coerce')
    df_processed = deduplicate_data(df.to_dict('records'))
    df_processed = pd.DataFrame(df_processed)
    
    if not df_processed.empty:
        df_processed = classify_incidents(df_processed, config)
    
    # Geocode (limit to avoid timeouts)
    print("📍 Geocoding (first 20 incidents)...")
    geocoder = FrontierGeocoder()
    df_sample = df_processed.head(20).copy()
    df_geocoded = geocoder.geocode_incidents(df_sample)
    df_geocoded.to_csv('data/incidents.csv', index=False)
    
    # Analyze
    print("📊 Analyzing...")
    analyzer = GeoAnalyzer(df_geocoded)
    map_path = analyzer.create_incident_map('maps/incidents.html')
    charts = analyzer.create_stats_charts('charts')
    
    return df_geocoded, map_path, charts

def send_report(notifier, count, map_path, charts):
    """Send Telegram report"""
    if not notifier.telegram_token or not notifier.telegram_chat_id:
        print("⚠️  No Telegram config - skipping notifications")
        return
    
    print("📤 Sending report...")
    notifier.send_daily_report(count, map_path, charts)

def main(mode='full'):
    """Main orchestration"""
    print(f"🚀 FrontierWatch starting... ({mode} mode)")
    
    config = load_config()
    print(f"✅ Config loaded: {len([k for k,v in config.items() if v])} keys")
    
    if mode == 'scrape':
        df = collect_data(config)
        if not df.empty:
            df.to_csv('data/raw_scrape.csv', index=False)
        print(f"✅ Scraped {len(df)} incidents")
        return
    
    # Full analysis
    df = collect_data(config)
    df_final, map_path, charts = process_and_analyze(df, config)
    
    notifier = TelegramNotifier(
        config['telegram_token'],
        config['telegram_chat_id']
    )
    
    send_report(notifier, len(df_final), map_path, charts)
    print("✅ Pipeline complete!")

if __name__ == "__main__":
    main()
