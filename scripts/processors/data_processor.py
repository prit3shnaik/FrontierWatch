import pandas as pd
from datetime import datetime
import hashlib
import yaml

def load_config():
    import os
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def deduplicate_data(all_incidents):
    """Deduplicate incidents across sources using safe column access"""
    if not all_incidents:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_incidents)
    if df.empty:
        return df
    
    # ✅ SAFE: Ensure required columns exist with defaults
    df = df.fillna({'title': '', 'description': '', 'text': '', 'summary': ''})
    
    # ✅ SAFE: Use available title/description fields
    df['content'] = (
        df['title'].astype(str) + ' ' +
        df['description'].astype(str).fillna('') + ' ' +
        df['text'].astype(str).fillna('') + ' ' +
        df['summary'].astype(str).fillna('')
    )
    
    df['content_hash'] = df['content'].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
    
    # Keep most recent duplicate
    if 'published' in df.columns:
        df['published'] = pd.to_datetime(df['published'], errors='coerce')
        df = df.sort_values('published', ascending=False)
    
    df_unique = df.drop_duplicates(subset=['content_hash'], keep='first')
    print(f"✅ Deduplicated: {len(df)} → {len(df_unique)} incidents")
    return df_unique

def classify_incidents(df, config):
    """Classify incidents by type and region"""
    if df.empty:
        return df
    
    df['incident_type'] = 'Other'
    df['region'] = 'Other'
    
    # Ensure content for classification
    df['full_text'] = (
        df['title'].astype(str) + ' ' +
        df['description'].astype(str).fillna('') + ' ' +
        df['text'].astype(str).fillna('') + ' ' +
        df['summary'].astype(str).fillna('')
    ).str.lower()
    
    terror_keywords = config.get('keywords', {}).get('terror', [])
    encounter_keywords = config.get('keywords', {}).get('encounter', [])
    attack_keywords = config.get('keywords', {}).get('attack', [])
    
    for idx, row in df.iterrows():
        text = row['full_text']
        
        if any(kw in text for kw in terror_keywords):
            df.at[idx, 'incident_type'] = 'Terror'
        elif any(kw in text for kw in encounter_keywords):
            df.at[idx, 'incident_type'] = 'Encounter'
        elif any(kw in text for kw in attack_keywords):
            df.at[idx, 'incident_type'] = 'Attack'
    
    return df

def save_incidents(df, filename='../data/incidents.json'):
    """Save processed incidents"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_json(filename, orient='records', date_format='iso')
    print(f"💾 Saved {len(df)} incidents to {filename}")
