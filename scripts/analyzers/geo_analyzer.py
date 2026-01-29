import folium
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import os

class GeoAnalyzer:
    def __init__(self, incidents_df):
        self.df = incidents_df
    
    def create_incident_map(self, output='map.html'):
        """Create interactive incident map"""
        if self.df.empty or self.df['lat'].isna().all():
            print("No geocode data for mapping")
            return None
        
        # Center on J&K/Northeast
        m = folium.Map(location=[28.5, 78.0], zoom_start=6)
        
        # Add incidents
        for idx, row in self.df.dropna(subset=['lat']).iterrows():
            folium.CircleMarker(
                location=[row['lat'], row['lon']],
                radius=8,
                popup=f"{row['title'][:100]}...<br>Source: {row['source']}",
                color='red' if row['incident_type'] == 'Terror' else 'orange',
                fill=True,
                fillOpacity=0.7
            ).add_to(m)
        
        m.save(output)
        print(f"Map saved: {output}")
        return output
    
    def create_stats_charts(self, output_dir='charts'):
        """Create statistical charts"""
        os.makedirs(output_dir, exist_ok=True)
        
        if self.df.empty:
            return {}
        
        # Chart 1: Incidents by Region
        region_chart = px.bar(
            self.df.groupby('region').size().reset_index(name='count'),
            x='region', y='count',
            title='Incidents by Region'
        )
        region_chart.write_html(f"{output_dir}/regions.html")
        
        # Chart 2: Incidents by Type
        type_chart = px.pie(
            self.df['incident_type'].value_counts().reset_index(),
            values='count', names='incident_type',
            title='Incident Types'
        )
        type_chart.write_html(f"{output_dir}/types.html")
        
        # Chart 3: Sources
        source_chart = px.bar(
            self.df['source'].value_counts().head(10).reset_index(),
            x='source', y='count',
            title='Top Sources'
        )
        source_chart.write_html(f"{output_dir}/sources.html")
        
        return {
            'regions': f"{output_dir}/regions.html",
            'types': f"{output_dir}/types.html",
            'sources': f"{output_dir}/sources.html"
      }
