import pandas as pd
import hashlib
import os
import yaml
from pathlib import Path

def deduplicate_data(all_incidents):
    """Deduplicate incidents - PRODUCTION READY"""
    if not all_incidents:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_incidents)
    if df.empty:
        return df
    
    print(f"📋 Processing {len(df)} incidents with columns: {list(df.columns)}")
    
    # Create ALL columns with defaults
    for col in ['title', 'description', 'text', 'summary', 'published', 'source']:
        if col not in df.columns:
            df[col] = ''
    
    # Build content ROW-BY-ROW
    def get_content(row):
        content = []
        for col in ['title', 'description', 'text', 'summary']:
            val = row.get(col, '')
            if pd.notna(val) and val:
                content.append(str(val))
        return ' '.join(content)
    
    df['content'] = df.apply(get_content, axis=1)
    df['content_hash'] = df['content'].apply(
        lambda x: hashlib.md5(str(x).encode()).hexdigest()
    )
    
    # FIXED: Use na_position instead of na_last
    if 'published' in df.columns:
        df['published'] = pd.to_datetime(df['published'], errors='coerce')
        df = df.sort_values('published', ascending=False, na_position='last')
    
    before_count = len(df)
    df_unique = df.drop_duplicates(subset=['content_hash'], keep='first')
    print(f"✅ Deduplicated: {before_count} → {len(df_unique)} incidents")
    return df_unique

def classify_incidents(df, config):
    """Classify incidents by type and region"""
    if df.empty:
        return df
    
    df = df.copy()
    df['incident_type'] = 'Other'
    df['region'] = 'Other'
    
    def get_full_text(row):
        text = []
        for col in ['title', 'description', 'text', 'summary']:
            val = row.get(col, '')
            if pd.notna(val) and val:
                text.append(str(val))
        return ' '.join(text).lower()
    
    df['full_text'] = df.apply(get_full_text, axis=1)
    
    keywords = config.get('keywords', {})
    terror_kws = keywords.get('terror', [])
    encounter_kws = keywords.get('encounter', [])
    attack_kws = keywords.get('attack', [])
    
    for idx, row in df.iterrows():
        text = row['full_text']
        if any(kw in text for kw in terror_kws):
            df.at[idx, 'incident_type'] = 'Terror'
        elif any(kw in text for kw in encounter_kws):
            df.at[idx, 'incident_type'] = 'Encounter'
        elif any(kw in text for kw in attack_kws):
            df.at[idx, 'incident_type'] = 'Attack'
    
    return df

def save_incidents(df, filename='../data/incidents.csv'):
    """Save processed incidents"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    df.to_csv(filename, index=False)
    print(f"💾 Saved {len(df)} incidents to {filename}")
