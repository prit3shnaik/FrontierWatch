import requests
import os
from datetime import datetime

class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_message(self, text):
        """Send text message"""
        if not self.bot_token or not self.chat_id:
            print("⚠️ No Telegram config - skipping")
            return
        
        data = {
            'chat_id': self.chat_id,
            'text': text[:4096],  # Telegram limit
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        try:
            response = requests.post(self.base_url.replace('/sendMessage', ''), 
                                   data=data, timeout=10)
            print(f"📤 Telegram sent: {response.status_code}")
        except Exception as e:
            print(f"Telegram error: {e}")
    
    def send_photo(self, photo_path, caption=""):
        """Send photo/map - SIMPLIFIED"""
        if not os.path.exists(photo_path) or not self.bot_token or not self.chat_id:
            print(f"⚠️ Photo skip: {photo_path}")
            return
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendPhoto"
        try:
            with open(photo_path, 'rb') as photo:
                files = {'photo': photo}
                data = {'chat_id': self.chat_id, 'caption': caption[:1024]}
                response = requests.post(url, files=files, data=data, timeout=30)
            print(f"📸 Telegram photo sent: {response.status_code}")
        except Exception as e:
            print(f"Photo error: {e}")
    
    def send_daily_report(self, incidents_count, map_path=None, charts=None):
        """Send comprehensive daily report"""
        report = f"""
🚨 <b>FrontierWatch Daily Report</b> 🚨
📅 <i>{datetime.now().strftime('%Y-%m-%d %H:%M IST')}</i>

📊 <b>Today's Summary:</b>
• Total Incidents: <b>{incidents_count}</b>
• Regions Monitored: Jammu & Kashmir, North East India

⚠️ Incidents detected - check data/ folder for details
📍 Interactive map: maps/incidents.html
📈 Charts: charts/ folder

#FrontierWatch #OSINT
"""
        
        self.send_message(report)
        
        if map_path:
            self.send_photo(map_path, "📍 Incident Heatmap")
        
        print(f"✅ Report sent for {incidents_count} incidents")
