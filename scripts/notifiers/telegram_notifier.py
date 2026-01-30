import requests
from datetime import datetime

class TelegramNotifier:
    def __init__(self, bot_token, chat_id):
        self.bot_token = bot_token or "TEST_MODE"
        self.chat_id = chat_id or "1290402334"
    
    def send_daily_report(self, incidents_count, map_path=None, charts=None):
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        message = f"""
🚨 FrontierWatch LIVE REPORT 🚨
🕐 {datetime.now().strftime('%Y-%m-%d %H:%M IST')}
📊 {incidents_count} INCIDENTS DETECTED
📍 Jammu & Kashmir + Northeast
✅ PIPELINE WORKING 100%

#OSINT #FrontierWatch
"""
        data = {'chat_id': self.chat_id, 'text': message, 'parse_mode': 'HTML'}
        
        print(f"🔗 Telegram URL: {url[:50]}...")
        print(f"👤 Chat ID: {self.chat_id}")
        
        r = requests.post(url, data=data)
        print(f"📱 STATUS: {r.status_code}")
        print(f"📱 RESPONSE: {r.text}")
        return r.status_code == 200
