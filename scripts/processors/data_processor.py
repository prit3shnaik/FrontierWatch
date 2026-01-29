import pandas as pd
import hashlib
import os
import yaml

def deduplicate_data(all_incidents):
    """Deduplicate incidents - BULLETPROOF column handling"""
    if not all_incidents:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_incidents)
    if df.empty:
        return df
    
    # ✅ BULLETPROOF: Create ALL possible columns with empty strings
    required_cols = ['title', 'description', 'text', 'summary', 'published']
    for col in required_cols:
        if col not in df.columns:
            df[col] = ''
    
    # ✅ SAFE: Build content from ANY available fields
    content_parts = []
    for col in ['title', 'description', 'text', 'summary']:
        if col in df.columns:
            content_parts.append(df[col].astype(str))
    
    df['content'] = ' '.join(content_parts)
    df['content_hash'] = df['content'].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
    
    # Sort by date if available
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
    
    # Safe defaults
    df['incident_type'] = 'Other'
    df['region'] = 'Other'
    
    # Build full text safely
    content_cols = ['title', 'description', 'text', 'summary']
    df['full_text'] = ' '
    for col in content_cols:
        if col in df.columns:
            df['full_text'] += ' ' + df[col].astype(str)
    df['full_text'] = df['full_text'].str.lower()
    
    # Keywords from config (safe access)
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
