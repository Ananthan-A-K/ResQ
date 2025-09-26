"""Simple command-line client that fetches alerts and caches them locally for offline access.
It will try to reach the backend; if unavailable, it reads the last cached alerts from disk.
"""

import requests
import json
import sys
from pathlib import Path
import time

BACKEND_URL = 'http://127.0.0.1:5000'
CACHE_FILE = Path.home() / '.safeahead_cache.json'

def fetch_online():
    try:
        r = requests.get(BACKEND_URL + '/alerts', timeout=5)
        r.raise_for_status()
        alerts = r.json().get('alerts', [])
        # write cache
        CACHE_FILE.write_text(json.dumps({'alerts': alerts, 'ts': int(time.time())}))
        return alerts
    except Exception as e:
        print('Failed to reach backend:', e)
        return None

def read_cache():
    if CACHE_FILE.exists():
        data = json.loads(CACHE_FILE.read_text())
        return data.get('alerts', []), data.get('ts')
    return [], None

def pretty_print_alert(a):
    ts = a.get('timestamp')
    tstr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts)) if ts else 'unknown'
    print(f"- [{a.get('event_type')}] {a.get('message')} (severity: {a.get('severity')}) @ {tstr}")

if __name__ == '__main__':
    alerts = fetch_online()
    if alerts is None:
        alerts, ts = read_cache()
        if alerts:
            print(f"Offline mode — showing cached alerts (cached at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))})")
            for a in alerts:
                pretty_print_alert(a)
        else:
            print('No cached alerts available.')
        sys.exit(0)
    if not alerts:
        print('No active alerts.')
    else:
        print('Active alerts (live):')
        for a in alerts:
            pretty_print_alert(a)