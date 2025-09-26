from flask import Flask, jsonify, request
import time
import uuid
import threading
import requests
import math
from datetime import datetime

# Import DB helpers (relative import when using single-file view)
from db import init_db, insert_alert, get_alerts, insert_guidance, get_guidance

app = Flask(__name__)

# Configuration - minimal, change for production
POLL_INTERVAL_SECONDS = 60  # poll every 60s for MVP
MONITOR_COORD = (12.9716, 77.5946)  # Bangalore example center (lat, lon)
MONITOR_RADIUS_KM = 500  # consider events within this radius for local alerts

# Simple preloaded guidance (cached offline)
PRELOADED_GUIDANCE = [
    {
        'id': 'guid-earthquake',
        'topic': 'Earthquake Safety',
        'content': 'Drop, Cover, Hold On. Stay away from glass and heavy furniture. After shaking stops, check for injuries and gas leaks.',
        'last_updated': int(time.time())
    },
    {
        'id': 'guid-flood',
        'topic': 'Flood Safety',
        'content': 'Move to higher ground, avoid driving through flood water, bring emergency kit and important documents.',
        'last_updated': int(time.time())
    },
    {
        'id': 'guid-cyclone',
        'topic': 'Cyclone/Tropical Storm',
        'content': 'Secure windows and outdoor objects, follow evacuation orders, keep radio and phone charged.',
        'last_updated': int(time.time())
    }
]

# Utility

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(a))

# Simple monitors

def poll_usgs():
    """Poll USGS recent earthquakes (past hour) and create alerts for nearby ones."""
    try:
        url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        for feat in data.get('features', []):
            props = feat.get('properties', {})
            geom = feat.get('geometry', {})
            coords = geom.get('coordinates', [None, None])
            lon, lat = coords[0], coords[1]
            if lat is None or lon is None:
                continue
            dist = haversine_km(lat, lon, MONITOR_COORD[0], MONITOR_COORD[1])
            if dist <= MONITOR_RADIUS_KM and props.get('mag', 0) >= 3.0:
                alert = {
                    'id': str(feat.get('id') or uuid.uuid4()),
                    'source': 'USGS',
                    'event_type': 'earthquake',
                    'message': f"M{props.get('mag')} earthquake {dist:.0f} km from center: {props.get('place')}",
                    'severity': 'medium' if props.get('mag',0)<5 else 'high',
                    'latitude': lat,
                    'longitude': lon,
                    'timestamp': int((props.get('time') or int(time.time()*1000))/1000)
                }
                insert_alert(alert)
    except Exception as e:
        app.logger.debug('USGS poll failed: %s', e)


def poll_weather():
    """Poll a public weather forecast API (Open-Meteo) to detect severe conditions.
    For MVP we will check for high wind speed or heavy precipitation forecast in the next few hours."""
    try:
        lat, lon = MONITOR_COORD
        # Open-Meteo hourly forecast (no API key required)
        url = f'https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=windgusts_10m,precipitation&forecast_days=1'
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        hourly = data.get('hourly', {})
        windgusts = hourly.get('windgusts_10m', [])
        precip = hourly.get('precipitation', [])
        times = hourly.get('time', [])
        # simple heuristic: if any hour shows gusts > 30 m/s or precipitation > 50 mm, alert
        for i, t in enumerate(times[:24]):
            try:
                g = windgusts[i]
                p = precip[i]
            except Exception:
                continue
            if g is not None and g >= 30:
                alert = {
                    'id': f'weather-gust-{t}',
                    'source': 'OpenMeteo',
                    'event_type': 'high_wind',
                    'message': f'High wind gusts forecast at {t}: {g} m/s',
                    'severity': 'medium',
                    'latitude': lat,
                    'longitude': lon,
                    'timestamp': int(time.time())
                }
                insert_alert(alert)
            if p is not None and p >= 50:
                alert = {
                    'id': f'weather-precip-{t}',
                    'source': 'OpenMeteo',
                    'event_type': 'heavy_precipitation',
                    'message': f'Heavy precipitation forecast at {t}: {p} mm',
                    'severity': 'medium',
                    'latitude': lat,
                    'longitude': lon,
                    'timestamp': int(time.time())
                }
                insert_alert(alert)
    except Exception as e:
        app.logger.debug('Weather poll failed: %s', e)


def poll_loop(stop_event):
    while not stop_event.is_set():
        poll_usgs()
        poll_weather()
        # re-save guidance (acts as offline cache)
        for g in PRELOADED_GUIDANCE:
            insert_guidance(g)
        stop_event.wait(POLL_INTERVAL_SECONDS)

# API endpoints

@app.route('/alerts', methods=['GET'])
def api_alerts():
    alerts = get_alerts()
    return jsonify({'alerts': alerts})

@app.route('/guidance', methods=['GET'])
def api_guidance():
    guidance = get_guidance()
    return jsonify({'guidance': guidance})

@app.route('/status', methods=['GET'])
def api_status():
    return jsonify({'status': 'ok', 'server_time': int(time.time())})

@app.route('/ack', methods=['POST'])
def api_ack():
    data = request.json or {}
    # for MVP just echo back
    return jsonify({'status': 'acknowledged', 'data': data})

# App startup: initialize DB and start poller thread

stop_event = threading.Event()

def start_background_poller():
    init_db()
    for g in PRELOADED_GUIDANCE:
        insert_guidance(g)
    t = threading.Thread(target=poll_loop, args=(stop_event,), daemon=True)
    t.start()

if __name__ == '__main__':
    start_background_poller()
    app.run(host='0.0.0.0', port=5000)