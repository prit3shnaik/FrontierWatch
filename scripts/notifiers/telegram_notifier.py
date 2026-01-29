import requests
import os
from datetime import datetime

class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    def send_message(self, text):
        """Send text message"""
        url = f"{self.base_url}/sendMessage"
        data = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        requests.post(url, data=data)
    
    def send_photo(self, photo_path, caption=""):
        """Send photo/map"""
        if not os.path.exists(photo_path):
            return
        
        url = f"{self.base_url}/sendPhoto"
        with open(photo_path, 'rb') as photo:
            files = {'photo': photo}
            data = {'chat_id': self.chat_id, 'caption': caption}
            requests.post(url, files=files, data=data)
    
    def send_daily_report(self, incidents_count, map_path=None, charts=None):
        """Send comprehensive daily report"""
        report = f"""
🚨 <b>FrontierWatch Daily Report</b> 🚨
📅 <i>{datetime.now().strftime('%Y-%m-%d %H:%M IST')}</i>

📊 <b>Today's Summary:</b>
• Total Incidents: <b>{incidents_count}</b>

"""
        if map_path:
            self.send_photo(map_path, "📍 Incident Heatmap")
        
        self.send_message(report)
        
        if charts:
            for chart_name, chart_path in charts.items():
                self.send_photo(chart_path, f"📈 {chart_name.title()}")
