import sqlite3
import threading
from contextlib import closing
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / 'data' / 'safeahead.db'
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

DB_LOCK = threading.Lock()

def init_db():
    with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                source TEXT,
                event_type TEXT,
                message TEXT,
                severity TEXT,
                latitude REAL,
                longitude REAL,
                timestamp INTEGER
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS guidance (
                id TEXT PRIMARY KEY,
                topic TEXT,
                content TEXT,
                last_updated INTEGER
            )
        ''')
        conn.commit()

def insert_alert(alert):
    with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO alerts (id, source, event_type, message, severity, latitude, longitude, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            alert['id'], alert['source'], alert['event_type'], alert['message'], alert.get('severity'),
            alert.get('latitude'), alert.get('longitude'), alert['timestamp']
        ))
        conn.commit()

def get_alerts():
    with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute('SELECT id, source, event_type, message, severity, latitude, longitude, timestamp FROM alerts ORDER BY timestamp DESC')
        rows = c.fetchall()
        cols = ['id','source','event_type','message','severity','latitude','longitude','timestamp']
        return [dict(zip(cols,row)) for row in rows]

def insert_guidance(guidance):
    with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute('''
            INSERT OR REPLACE INTO guidance (id, topic, content, last_updated)
            VALUES (?, ?, ?, ?)
        ''', (
            guidance['id'], guidance['topic'], guidance['content'], guidance['last_updated']
        ))
        conn.commit()

def get_guidance():
    with DB_LOCK, closing(sqlite3.connect(DB_PATH)) as conn:
        c = conn.cursor()
        c.execute('SELECT id, topic, content, last_updated FROM guidance')
        rows = c.fetchall()
        cols = ['id','topic','content','last_updated']
        return [dict(zip(cols,row)) for row in rows]