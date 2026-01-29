from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import pandas as pd
import time

class FrontierGeocoder:
    def __init__(self):
        self.geolocator = Nominatim(user_agent="FrontierWatch/1.0")
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)
    
    def geocode_incidents(self, df):
        """Add coordinates to incidents"""
        if df.empty:
            return df
        
        coords = []
        for idx, row in df.iterrows():
            location = self._extract_location(row)
            if location:
                try:
                    result = self.geocode(location + ", India")
                    if result:
                        coords.append({
                            'lat': result.latitude,
                            'lon': result.longitude,
                            'location': location
                        })
                    else:
                        coords.append({'lat': None, 'lon': None, 'location': location})
                except:
                    coords.append({'lat': None, 'lon': None, 'location': location})
            else:
                coords.append({'lat': None, 'lon': None, 'location': None})
            
            time.sleep(1.2)  # Rate limiting
        
        geo_df = pd.DataFrame(coords)
        return pd.concat([df.reset_index(drop=True), geo_df], axis=1)
    
    def _extract_location(self, row):
        """Extract location from incident text"""
        locations = [
            'Jammu', 'Srinagar', 'Kupwara', 'Baramulla', 'Anantnag',
            'Imphal', 'Kohima', 'Guwahati', 'Aizawl', 'Agartala',
            'Shillong', 'Itanagar', 'Manipur', 'Nagaland', 'Assam'
        ]
        
        text = (str(row['title']) + ' ' + str(row['description'] or '')).lower()
        for loc in locations:
            if loc.lower() in text:
                return loc
        return None
