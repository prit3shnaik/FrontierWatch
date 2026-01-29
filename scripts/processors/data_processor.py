import pandas as pd
from datetime import datetime
import hashlib
import yaml

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def deduplicate_data(all_incidents):
    """Deduplicate incidents across sources using hash"""
    df = pd.DataFrame(all_incidents)
    if df.empty:
        return df
    
    df['content_hash'] = df.apply(lambda row: hashlib.md5(
        (str(row['title']) + str(row['description'] or '')).encode()
    ).hexdigest(), axis=1)
    
    # Keep most recent duplicate
    df = df.sort_values('published', ascending=False)
    df_unique = df.drop_duplicates(subset=['content_hash'], keep='first')
    
    return df_unique

def classify_incidents(df, config):
    """Classify incidents by type and region"""
    if df.empty:
        return df
    
    df['incident_type'] = 'Other'
    terror_keywords = config['keywords']['terror']
    encounter_keywords = config['keywords']['encounter']
    attack_keywords = config['keywords']['attack']
    
    for idx, row in df.iterrows():
        text = (str(row['title']) + ' ' + str(row['description'] or '')).lower()
        
        if any(kw in text for kw in terror_keywords):
            df.at[idx, 'incident_type'] = 'Terror'
        elif any(kw in text for kw in encounter_keywords):
            df.at[idx, 'incident_type'] = 'Encounter'
        elif any(kw in text for kw in attack_keywords):
            df.at[idx, 'incident_type'] = 'Attack'
    
    return df

def save_incidents(df, filename='data/incidents.json'):
    """Save processed incidents"""
    df.to_json(filename, orient='records', date_format='iso')
    print(f"Saved {len(df)} incidents to {filename}")
