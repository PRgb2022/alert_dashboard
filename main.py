from fastapi import FastAPI
from sqlalchemy import create_engine, text
import pandas as pd
import schedule
import time
import threading
import requests

app = FastAPI()
engine = create_engine("sqlite:///alerts.db", echo=False)

def load_initial_data():
    try:
        df = pd.read_csv("alerts.csv")
        df.to_sql("alerts", engine, if_exists="replace", index=False)
        print("Initial data loaded.")
    except Exception as e:
        print("Error loading initial data:", e)

def fetch_and_update():
    try:
        url = "https://example.com/live-alerts.csv"  # Replace with real URL
        df = pd.read_csv(url)
        df.to_sql("alerts", engine, if_exists="replace", index=False)
        print("Live data updated.")
    except Exception as e:
        print("Failed to fetch live data:", e)

def run_scheduler():
    schedule.every(10).minutes.do(fetch_and_update)
    while True:
        schedule.run_pending()
        time.sleep(1)

@app.on_event("startup")
def startup_event():
    load_initial_data()
    thread = threading.Thread(target=run_scheduler, daemon=True)
    thread.start()

@app.get("/alerts_summary")
def get_alerts_summary():
    query = '''
    SELECT state_name, severity, COUNT(*) AS total_alerts
    FROM alerts
    WHERE severity IN ('Red', 'Orange', 'Yellow', 'Green', 'ALERT', 'WATCH', 'WARNING', 'ADVISORY')
    GROUP BY state_name, severity
    ORDER BY total_alerts DESC;
    '''
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row) for row in result]
