#!/usr/bin/env python3
"""
dmasst.py - Disaster Management Assistant (prototype)

Usage:
  python dmasst.py --id NODE_ID

Run multiple instances (different --id) on same LAN or different machines to
simulate a P2P network using UDP broadcast.

Features:
- UDP broadcast-based peer discovery and message relaying (store-and-forward)
- SQLite storage for messages and cached alerts
- Background "online monitor" to fetch alert feeds (configurable)
- Signal notifier: detects internet connectivity and attempts to "forward" pending messages
- Simple console commands:
    /send SOS message text...
    /send ALERT message text...
    /list messages
    /list alerts
    /quit
"""

import argparse
import asyncio
import json
import os
import socket
import sqlite3
import sys
import time
import uuid
from contextlib import closing
from datetime import datetime
from typing import Optional

try:
    import requests
except ImportError:
    requests = None  # we'll check and tell user to install if needed

# network config
BCAST_PORT = 50000
BCAST_ADDR = "<broadcast>"
BCAST_INTERVAL = 2.0  # seconds between periodic announce/heartbeat
RECV_BUFFER = 65536

# message behavior
MESSAGE_TTL = 6  # max hops
RELAY_DELAY = 0.2  # seconds to wait before relaying to reduce collisions

# database
DB_FILE_TEMPLATE = "dmasst_{node_id}.db"

# alert feed examples (user can modify): simple JSON endpoint or text
DEFAULT_ALERT_FEEDS = [
    # these are placeholders; real endpoints can be added to config
    # "https://example.com/my_alerts.json"
]


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


class Storage:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init()

    def _init(self):
        c = self.conn.cursor()
        c.execute(
            """CREATE TABLE IF NOT EXISTS messages(
                id TEXT PRIMARY KEY,
                origin_id TEXT,
                origin_label TEXT,
                type TEXT,
                payload TEXT,
                timestamp TEXT,
                received_at TEXT,
                hops INTEGER,
                ttl INTEGER,
                forwarded INTEGER DEFAULT 0
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS alerts(
                id TEXT PRIMARY KEY,
                title TEXT,
                body TEXT,
                source TEXT,
                fetched_at TEXT
            )"""
        )
        self.conn.commit()

    def insert_message(self, msg):
        c = self.conn.cursor()
        try:
            c.execute(
                "INSERT INTO messages(id, origin_id, origin_label, type, payload, timestamp, received_at, hops, ttl, forwarded) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    msg["id"],
                    msg["origin_id"],
                    msg.get("origin_label", ""),
                    msg["type"],
                    msg["payload"],
                    msg.get("timestamp", now_iso()),
                    now_iso(),
                    msg.get("hops", 0),
                    msg.get("ttl", MESSAGE_TTL),
                    0,
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # already exists

    def mark_forwarded(self, message_id):
        c = self.conn.cursor()
        c.execute("UPDATE messages SET forwarded = 1 WHERE id = ?", (message_id,))
        self.conn.commit()

    def list_messages(self, unseen_only=False):
        c = self.conn.cursor()
        if unseen_only:
            c.execute("SELECT id, origin_id, type, payload, timestamp, hops, ttl, forwarded FROM messages WHERE forwarded=0 ORDER BY rowid DESC")
        else:
            c.execute("SELECT id, origin_id, type, payload, timestamp, hops, ttl, forwarded FROM messages ORDER BY rowid DESC")
        return c.fetchall()

    def insert_alert(self, alert_id, title, body, source):
        c = self.conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO alerts(id, title, body, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (alert_id, title, body, source, now_iso()),
        )
        self.conn.commit()

    def list_alerts(self):
        c = self.conn.cursor()
        c.execute("SELECT id, title, body, source, fetched_at FROM alerts ORDER BY fetched_at DESC")
        return c.fetchall()

    def get_pending_messages(self):
        c = self.conn.cursor()
        c.execute("SELECT id, origin_id, type, payload FROM messages WHERE forwarded=0")
        return c.fetchall()


class UDPPeer:
    def __init__(self, node_id: str, node_label: str, storage: Storage, loop: asyncio.AbstractEventLoop):
        self.node_id = node_id
        self.node_label = node_label
        self.storage = storage
        self.loop = loop
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # enable broadcast
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # bind to port so we can listen
        self._sock.bind(("", BCAST_PORT))
        self._sock.setblocking(False)
        self._running = False

    def send_packet(self, data: dict):
        # ensure small messages (we don't implement fragmentation here)
        raw = json.dumps(data).encode("utf-8")
        try:
            self._sock.sendto(raw, (BCAST_ADDR, BCAST_PORT))
        except Exception as e:
            print("[udp] send failed:", e)

    async def broadcaster(self):
        """Regular heartbeat / announce + send queued messages to the LAN"""
        while True:
            # announce presence (lightweight)
            announce = {"type": "ANNOUNCE", "node_id": self.node_id, "label": self.node_label, "timestamp": now_iso()}
            self.send_packet(announce)
            await asyncio.sleep(BCAST_INTERVAL)

    async def receiver(self, on_message_cb):
        """Continuously read UDP packets and dispatch to handler"""
        while True:
            try:
                data, addr = await self.loop.sock_recvfrom(self._sock, RECV_BUFFER)
                # parse JSON
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception:
                    # ignore invalid packets
                    continue
                # ignore own announce responses (we might receive our own broadcast)
                # but we still want to process messages originated by this node if they come back with hops
                await on_message_cb(msg, addr)
            except (asyncio.CancelledError, KeyboardInterrupt):
                break
            except Exception:
                await asyncio.sleep(0.01)  # avoid tight error loop

    def close(self):
        try:
            self._sock.close()
        except:
            pass


class OnlineMonitor:
    """When online, fetch configured alert feeds and cache them."""

    def __init__(self, storage: Storage, feeds=None, poll_interval=30.0):
        self.storage = storage
        self.feeds = feeds or []
        self.poll_interval = poll_interval
        self._running = True

    @staticmethod
    def is_online():
        # quick internet check: attempt to resolve or perform small request
        try:
            # use socket to avoid heavy dependencies
            with closing(socket.create_connection(("8.8.8.8", 53), timeout=2)):
                return True
        except Exception:
            return False

    def fetch_feed(self, url) -> Optional[dict]:
        if not requests:
            return None
        try:
            r = requests.get(url, timeout=6)
            # accept JSON or plain text
            ctype = r.headers.get("content-type", "")
            if "application/json" in ctype:
                j = r.json()
                # user must supply a sensible JSON structure; we try to extract title/body
                if isinstance(j, dict):
                    title = j.get("title", url)
                    body = json.dumps(j)[:1000]
                else:
                    title = url
                    body = json.dumps(j)[:1000]
                return {"id": url + "::" + str(hash(r.text)), "title": title, "body": body, "source": url}
            else:
                text = r.text.strip()
                return {"id": url + "::" + str(hash(text)), "title": url, "body": text[:2000], "source": url}
        except Exception:
            return None

    async def run(self):
        while self._running:
            online = self.is_online()
            if online and self.feeds and requests:
                for url in self.feeds:
                    fetched = self.fetch_feed(url)
                    if fetched:
                        self.storage.insert_alert(fetched["id"], fetched["title"], fetched["body"], fetched["source"])
                        print(f"[online-monitor] fetched alert from {url}")
            await asyncio.sleep(self.poll_interval)


class SignalNotifier:
    """Watches for internet connectivity returning, then attempts to forward pending messages.

    In this prototype 'forwarding' is simulated: we just mark messages forwarded and print action.
    Replace with SMTP/HTTP/API calls to notify responders in production.
    """

    def __init__(self, storage: Storage, check_interval=5.0):
        self.storage = storage
        self.check_interval = check_interval
        self._seen_online = False

    @staticmethod
    def is_online():
        try:
            with closing(socket.create_connection(("8.8.8.8", 53), timeout=2)):
                return True
        except Exception:
            return False

    async def run(self):
        while True:
            online = self.is_online()
            if online and not self._seen_online:
                print("[signal-notifier] connectivity detected. Attempting to forward pending messages...")
                pending = self.storage.get_pending_messages()
                for mid, origin, mtype, payload in pending:
                    # simulate forwarding via internet
                    print(f"[signal-notifier] forwarding message {mid} (origin={origin}, type={mtype}) -> (SIMULATED)")
                    self.storage.mark_forwarded(mid)
                self._seen_online = True
            elif not online:
                self._seen_online = False
            await asyncio.sleep(self.check_interval)


async def on_incoming_message(msg: dict, addr, storage: Storage, udp_peer: UDPPeer):
    """Process an incoming UDP packet that is intended as a message or announce"""
    t = msg.get("type", "")
    if t == "ANNOUNCE":
        # ignore announces beyond maybe printing
        # print(f"[peer] announce from {msg.get('label')} ({msg.get('node_id')}) at {addr}")
        return

    if t == "DM_MSG":
        # basic validation
        mid = msg.get("id")
        if not mid:
            return
        # check TTL/hops
        hops = int(msg.get("hops", 0))
        ttl = int(msg.get("ttl", MESSAGE_TTL))
        # If we've seen it before, ignore
        inserted = storage.insert_message({
            "id": mid,
            "origin_id": msg.get("origin_id", ""),
            "origin_label": msg.get("origin_label", ""),
            "type": msg.get("subtype", "SOS"),
            "payload": msg.get("payload", ""),
            "timestamp": msg.get("timestamp", now_iso()),
            "hops": hops,
            "ttl": ttl,
        })
        if inserted:
            print(f"[recv] new msg {mid} type={msg.get('subtype')} hops={hops} ttl={ttl} from {addr}")
        else:
            # we've already seen it; do not relay
            return

        # if TTL not exhausted, increment hops and relay
        if hops < ttl:
            await asyncio.sleep(RELAY_DELAY)  # small randomized backoff could be added
            outgoing = dict(msg)
            outgoing["hops"] = hops + 1
            # forward to LAN
            udp_peer.send_packet(outgoing)
            # note: we don't mark forwarded here (forwarded==internet forwarded).
            print(f"[relay] relayed {mid} hop -> {outgoing['hops']}")
    else:
        # unknown packet type - ignore
        return


def build_outgoing_message(origin_id, origin_label, subtype, payload, ttl=MESSAGE_TTL):
    return {
        "type": "DM_MSG",
        "id": str(uuid.uuid4()),
        "origin_id": origin_id,
        "origin_label": origin_label,
        "subtype": subtype,
        "payload": payload,
        "timestamp": now_iso(),
        "hops": 0,
        "ttl": ttl,
    }


async def repl_input(prompt="> "):
    # simple asynchronous input wrapper
    return await asyncio.get_event_loop().run_in_executor(None, lambda: input(prompt))


async def main_loop(node_id, node_label, feeds):
    db_path = DB_FILE_TEMPLATE.format(node_id=node_id)
    storage = Storage(db_path)
    loop = asyncio.get_event_loop()

    udp_peer = UDPPeer(node_id, node_label, storage, loop)

    online_monitor = OnlineMonitor(storage, feeds=feeds, poll_interval=30.0)
    signal_notifier = SignalNotifier(storage, check_interval=5.0)

    async def message_cb(msg, addr):
        await on_incoming_message(msg, addr, storage, udp_peer)

    # start tasks
    tasks = []
    tasks.append(loop.create_task(udp_peer.receiver(message_cb)))
    tasks.append(loop.create_task(udp_peer.broadcaster()))
    tasks.append(loop.create_task(online_monitor.run()))
    tasks.append(loop.create_task(signal_notifier.run()))

    print(f"Node {node_id} ({node_label}) started. DB: {db_path}")
    print("Commands:")
    print("  /send SOS <message>")
    print("  /send ALERT <message>")
    print("  /list messages")
    print("  /list alerts")
    print("  /quit")

    try:
        while True:
            cmd = (await repl_input()).strip()
            if not cmd:
                continue
            if cmd.startswith("/send "):
                parts = cmd.split(" ", 2)
                if len(parts) < 3:
                    print("usage: /send SOS <message>")
                    continue
                subtype = parts[1].upper()
                text = parts[2]
                msg = build_outgoing_message(node_id, node_label, subtype, text)
                # insert locally
                storage.insert_message({
                    "id": msg["id"],
                    "origin_id": node_id,
                    "origin_label": node_label,
                    "type": subtype,
                    "payload": text,
                    "timestamp": msg["timestamp"],
                    "hops": 0,
                    "ttl": msg["ttl"],
                })
                # broadcast immediately
                udp_peer.send_packet(msg)
                print(f"[send] {msg['id']} subtype={subtype}")
            elif cmd.startswith("/list"):
                if "messages" in cmd:
                    rows = storage.list_messages()
                    print("--- messages ---")
                    for r in rows[:50]:
                        mid, origin, subtype, payload, ts, hops, ttl, forwarded = r
                        print(f"{mid[:8]} | {subtype} | from={origin} | hops={hops}/{ttl} | forwarded={forwarded} | {payload}")
                elif "alerts" in cmd:
                    rows = storage.list_alerts()
                    print("--- alerts ---")
                    for r in rows[:20]:
                        aid, title, body, source, fetched_at = r
                        print(f"{aid[:12]} | {title} | {source} | {fetched_at}")
                        snippet = (body[:120] + "...") if len(body) > 120 else body
                        print("   ", snippet)
                else:
                    print("Unknown list target. Use '/list messages' or '/list alerts'")
            elif cmd == "/quit":
                print("Shutting down...")
                break
            else:
                print("Unknown command. Try /send, /list, /quit")
    except (KeyboardInterrupt, EOFError):
        print("\nInterrupted. Shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        try:
            udp_peer.close()
        except:
            pass
        await asyncio.sleep(0.2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DM Assistant prototype (UDP P2P store-and-forward)")
    parser.add_argument("--id", required=True, help="Node ID (unique per instance). e.g., node1")
    parser.add_argument("--label", default=None, help="Human-friendly label")
    parser.add_argument("--feed", action="append", help="Alert feed URL (can provide multiple)", default=[])
    args = parser.parse_args()

    node_id = args.id
    label = args.label or node_id
    feeds = args.feed or DEFAULT_ALERT_FEEDS

    try:
        asyncio.run(main_loop(node_id, label, feeds))
    except Exception as e:
        print("Fatal:", e)
        sys.exit(1)
