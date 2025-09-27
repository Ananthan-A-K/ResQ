#!/usr/bin/env python3
"""
dmasst_fixed_with_sos.py - Disaster Management Assistant (messaging + SOS features)

Fully debugged and ready-to-run:
- Timezone-aware UTC datetime
- Ensures 'dest_id' column exists
- Async safe shutdown
- Robust SOS resend
- Debug logging
"""
import argparse
import asyncio
import json
import socket
import sqlite3
import sys
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    import requests
except ImportError:
    requests = None

# --- network config ---
BCAST_PORT = 50000
BCAST_ADDR = "255.255.255.255"
BCAST_INTERVAL = 2.0
RECV_BUFFER = 65536

# --- message behavior ---
MESSAGE_TTL = 6
RELAY_DELAY = 0.2
SOS_RELAY_DELAY = 0.05
MAX_SOS_RESENDS = 6
SOS_RESEND_INTERVAL = 5.0  # seconds between SOS resends

# --- database ---
DB_FILE_TEMPLATE = "dmasst_{node_id}.db"
DEFAULT_ALERT_FEEDS = []

# --- utility ---
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def parse_iso(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)

# ------------------- STORAGE -------------------
class Storage:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        c = self.conn.cursor()
        # create messages table
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
                forwarded INTEGER DEFAULT 0,
                acknowledged INTEGER DEFAULT 0,
                resend_count INTEGER DEFAULT 0
            )"""
        )
        # ensure dest_id column exists
        try:
            c.execute("ALTER TABLE messages ADD COLUMN dest_id TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            # column already exists
            pass

        # create alerts table
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

    # --- message operations ---
    def insert_message(self, msg):
        c = self.conn.cursor()
        try:
            c.execute(
                "INSERT INTO messages(id, origin_id, origin_label, dest_id, type, payload, timestamp, received_at, hops, ttl, forwarded, acknowledged, resend_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    msg["id"],
                    msg.get("origin_id", ""),
                    msg.get("origin_label", ""),
                    msg.get("dest_id", ""),
                    msg.get("type", ""),
                    msg.get("payload", ""),
                    msg.get("timestamp", now_iso()),
                    now_iso(),
                    msg.get("hops", 0),
                    msg.get("ttl", MESSAGE_TTL),
                    msg.get("forwarded", 0),
                    msg.get("acknowledged", 0),
                    msg.get("resend_count", 0),
                ),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def mark_forwarded(self, message_id):
        c = self.conn.cursor()
        c.execute("UPDATE messages SET forwarded = 1 WHERE id = ?", (message_id,))
        self.conn.commit()

    def mark_acknowledged(self, message_id):
        c = self.conn.cursor()
        c.execute("UPDATE messages SET acknowledged = 1 WHERE id = ?", (message_id,))
        self.conn.commit()

    def increment_resend(self, message_id):
        c = self.conn.cursor()
        c.execute("UPDATE messages SET resend_count = resend_count + 1 WHERE id = ?", (message_id,))
        self.conn.commit()

    def set_resend_count(self, message_id, val):
        c = self.conn.cursor()
        c.execute("UPDATE messages SET resend_count = ? WHERE id = ?", (val, message_id))
        self.conn.commit()

    def list_messages(self, unseen_only=False):
        c = self.conn.cursor()
        if unseen_only:
            c.execute(
                "SELECT * FROM messages WHERE forwarded=0 ORDER BY rowid DESC"
            )
        else:
            c.execute("SELECT * FROM messages ORDER BY rowid DESC")
        return c.fetchall()

    def list_alerts(self):
        c = self.conn.cursor()
        c.execute("SELECT * FROM alerts ORDER BY fetched_at DESC")
        return c.fetchall()

    def insert_alert(self, alert_id, title, body, source):
        c = self.conn.cursor()
        c.execute(
            "INSERT OR REPLACE INTO alerts(id, title, body, source, fetched_at) VALUES (?, ?, ?, ?, ?)",
            (alert_id, title, body, source, now_iso()),
        )
        self.conn.commit()

    def get_pending_messages(self):
        c = self.conn.cursor()
        c.execute(
            "SELECT * FROM messages WHERE forwarded=0 OR (type='SOS' AND acknowledged=0)"
        )
        return c.fetchall()

    def get_message(self, message_id):
        c = self.conn.cursor()
        c.execute("SELECT * FROM messages WHERE id = ?", (message_id,))
        return c.fetchone()

    def get_unacknowledged_sos(self, age_limit_seconds=3600):
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=age_limit_seconds)
        c = self.conn.cursor()
        c.execute(
            "SELECT * FROM messages WHERE type='SOS' AND acknowledged=0 AND ttl > 0 ORDER BY rowid ASC"
        )
        return c.fetchall()

    def cleanup_expired(self):
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        c = self.conn.cursor()
        c.execute("DELETE FROM messages WHERE received_at < ?", (cutoff.isoformat(),))
        self.conn.commit()


# ------------------- UDPPER -------------------
class UDPPeer:
    """UDP Peer communication"""

    def __init__(self, node_id: str, node_label: str, storage: Storage):
        self.node_id = node_id
        self.node_label = node_label
        self.storage = storage
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass
        self._sock.bind(("", BCAST_PORT))
        self._sock.setblocking(False)

    def send_packet(self, data: dict):
        raw = json.dumps(data).encode("utf-8")
        try:
            self._sock.sendto(raw, (BCAST_ADDR, BCAST_PORT))
        except Exception as e:
            print("[udp] send failed:", e)

    async def broadcaster(self):
        loop = asyncio.get_running_loop()
        while True:
            announce = {"type": "ANNOUNCE", "node_id": self.node_id, "label": self.node_label, "timestamp": now_iso()}
            await loop.run_in_executor(None, lambda: self.send_packet(announce))
            await asyncio.sleep(BCAST_INTERVAL)

    async def receiver(self, on_message_cb):
        loop = asyncio.get_running_loop()
        while True:
            try:
                data, addr = await loop.sock_recvfrom(self._sock, RECV_BUFFER)
                try:
                    msg = json.loads(data.decode("utf-8"))
                except Exception as e:
                    print(f"[recv] invalid packet from {addr}: {e}")
                    continue
                await on_message_cb(msg, addr)
            except (asyncio.CancelledError, KeyboardInterrupt):
                break
            except Exception as e:
                await asyncio.sleep(0.01)

    def close(self):
        try:
            self._sock.close()
        except Exception:
            pass


# ------------------- ONLINE MONITOR -------------------
class OnlineMonitor:
    def __init__(self, storage: Storage, feeds=None, poll_interval=30.0):
        self.storage = storage
        self.feeds = feeds or []
        self.poll_interval = poll_interval
        self._running = True

    @staticmethod
    def is_online():
        try:
            with closing(socket.create_connection(("8.8.8.8", 53), timeout=2)):
                return True
        except Exception:
            return False

    def fetch_feed(self, url) -> Optional[dict]:
        if not requests:
            return None
        try:
            r = requests.get(url, timeout=6)
            ctype = r.headers.get("content-type", "")
            if "application/json" in ctype:
                j = r.json()
                title = j.get("title", url) if isinstance(j, dict) else url
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
                    fetched = await asyncio.get_running_loop().run_in_executor(None, lambda u=url: self.fetch_feed(u))
                    if fetched:
                        self.storage.insert_alert(fetched["id"], fetched["title"], fetched["body"], fetched["source"])
                        print(f"[online-monitor] fetched alert from {url}")
            await asyncio.sleep(self.poll_interval)


# ------------------- SIGNAL NOTIFIER -------------------
class SignalNotifier:
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
                for row in pending:
                    mid = row["id"]
                    print(f"[signal-notifier] forwarding message {mid[:8]} (origin={row['origin_id']}, type={row['type']}) -> (SIMULATED)")
                    self.storage.mark_forwarded(mid)
                self._seen_online = True
            elif not online:
                self._seen_online = False
            await asyncio.sleep(self.check_interval)


# ------------------- MESSAGE HANDLER -------------------
async def on_incoming_message(msg: dict, addr, storage: Storage, udp_peer: UDPPeer):
    t = msg.get("type", "")
    if t == "ANNOUNCE":
        return

    if t == "DM_MSG":
        dest = msg.get("dest_id", "")
        if dest and dest != udp_peer.node_id:
            return

        mid = msg.get("id")
        if not mid:
            return

        hops = int(msg.get("hops", 0))
        ttl = int(msg.get("ttl", MESSAGE_TTL))

        inserted = storage.insert_message({
            "id": mid,
            "origin_id": msg.get("origin_id", ""),
            "origin_label": msg.get("origin_label", ""),
            "dest_id": msg.get("dest_id", ""),
            "type": msg.get("subtype", "SOS"),
            "payload": msg.get("payload", ""),
            "timestamp": msg.get("timestamp", now_iso()),
            "hops": hops,
            "ttl": ttl,
        })
        if inserted:
            print(f"[recv] new msg {mid[:8]} type={msg.get('subtype')} hops={hops} ttl={ttl} from {addr}")
        else:
            return

        # send ACK
        ack = {"type": "DM_ACK", "orig_msg_id": mid, "from_node": udp_peer.node_id, "timestamp": now_iso()}
        udp_peer.send_packet(ack)
        print(f"[ack] sent ack for {mid[:8]}")

        # relay
        if hops < ttl:
            outgoing = dict(msg)
            outgoing["hops"] = hops + 1
            udp_peer.send_packet(outgoing)
            print(f"[relay] relayed {mid[:8]} hop -> {outgoing['hops']}")

    elif t == "DM_ACK":
        orig_mid = msg.get("orig_msg_id")
        if not orig_mid:
            return
        stored = storage.get_message(orig_mid)
        if stored:
            storage.mark_acknowledged(orig_mid)
            print(f"[ack-recv] message {orig_mid[:8]} acknowledged by {msg.get('from_node')}")


# ------------------- OUTGOING MESSAGE -------------------
def build_outgoing_message(origin_id, origin_label, subtype, payload, dest_id="", ttl=MESSAGE_TTL):
    return {
        "type": "DM_MSG",
        "id": str(uuid.uuid4()),
        "origin_id": origin_id,
        "origin_label": origin_label,
        "dest_id": dest_id,
        "subtype": subtype,
        "payload": payload,
        "timestamp": now_iso(),
        "hops": 0,
        "ttl": ttl,
    }


# ------------------- SOS RESENDER -------------------
async def sos_resender_task(storage: Storage, udp_peer: UDPPeer):
    while True:
        rows = storage.get_unacknowledged_sos()
        for row in rows:
            if row["origin_id"] != udp_peer.node_id:
                continue
            resend_count = row["resend_count"]
            if resend_count >= MAX_SOS_RESENDS:
                continue
            msg = {
                "type": "DM_MSG",
                "id": row["id"],
                "origin_id": row["origin_id"],
                "origin_label": row["origin_label"],
                "dest_id": row["dest_id"],
                "subtype": row["type"],
                "payload": row["payload"],
                "timestamp": row["timestamp"],
                "hops": 0,
                "ttl": row["ttl"],
            }
            udp_peer.send_packet(msg)
            storage.increment_resend(row["id"])
            print(f"[sos-resend] resent SOS {row['id'][:8]} (attempt {resend_count + 1})")
        await asyncio.sleep(SOS_RESEND_INTERVAL)


# ------------------- REPL -------------------
async def repl_input(prompt="> "):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: input(prompt))


# ------------------- MAIN LOOP -------------------
async def main_loop(node_id, node_label, feeds):
    db_path = DB_FILE_TEMPLATE.format(node_id=node_id)
    storage = Storage(db_path)

    udp_peer = UDPPeer(node_id, node_label, storage)
    online_monitor = OnlineMonitor(storage, feeds=feeds)
    signal_notifier = SignalNotifier(storage)

    async def message_cb(msg, addr):
        await on_incoming_message(msg, addr, storage, udp_peer)

    loop = asyncio.get_running_loop()
    tasks = [
        loop.create_task(udp_peer.receiver(message_cb)),
        loop.create_task(udp_peer.broadcaster()),
        loop.create_task(online_monitor.run()),
        loop.create_task(signal_notifier.run()),
        loop.create_task(sos_resender_task(storage, udp_peer)),
    ]

    print(f"Node {node_id} ({node_label}) started. DB: {db_path}")
    print("Commands: /send, /ack, /list messages|alerts, /pending, /quit")

    try:
        while True:
            cmd = (await repl_input()).strip()
            if not cmd:
                continue
            # --- handle commands ---
            if cmd.startswith("/send "):
                rest = cmd[len("/send "):].strip()
                if " " not in rest:
                    print("usage: /send TYPE <message>  or /send TYPE@node <message>")
                    continue
                typ_and_maybe_dest, text = rest.split(" ", 1)
                if "@" in typ_and_maybe_dest:
                    subtype, dest = typ_and_maybe_dest.split("@", 1)
                    subtype = subtype.upper()
                    dest_id = dest.strip()
                else:
                    subtype = typ_and_maybe_dest.upper()
                    dest_id = ""
                msg = build_outgoing_message(node_id, node_label, subtype, text, dest_id)
                storage.insert_message({
                    "id": msg["id"],
                    "origin_id": node_id,
                    "origin_label": node_label,
                    "dest_id": dest_id,
                    "type": subtype,
                    "payload": text,
                    "timestamp": msg["timestamp"],
                    "hops": 0,
                    "ttl": msg["ttl"],
                    "forwarded": 0,
                    "acknowledged": 0,
                    "resend_count": 0,
                })
                udp_peer.send_packet(msg)
                print(f"[send] {msg['id'][:8]} subtype={subtype} to={dest_id or '<broadcast>'}")
            elif cmd.startswith("/ack "):
                parts = cmd.split(" ", 1)
                if len(parts) < 2:
                    print("usage: /ack <message_id>")
                    continue
                mid = parts[1].strip()
                ack_msg = {"type": "DM_ACK", "orig_msg_id": mid, "from_node": node_id, "timestamp": now_iso()}
                udp_peer.send_packet(ack_msg)
                print(f"[ack] sent ack for {mid[:8]}")
            elif cmd.startswith("/list"):
                if "messages" in cmd:
                    rows = storage.list_messages()
                    print("--- messages ---")
                    for r in rows[:200]:
                        destpart = f" -> {r['dest_id']}" if r['dest_id'] else ""
                        print(f"{r['id'][:8]} | {r['type']}{destpart} | from={r['origin_id']} hops={r['hops']}/{r['ttl']} fwd={r['forwarded']} ack={r['acknowledged']} resends={r['resend_count']}")
                        print("   ", (r['payload'][:120] + "...") if len(r['payload'])>120 else r['payload'])
                elif "alerts" in cmd:
                    rows = storage.list_alerts()
                    print("--- alerts ---")
                    for r in rows[:50]:
                        print(f"{r['id'][:12]} | {r['title']} | {r['source']} | {r['fetched_at']}")
                        print("   ", (r['body'][:120] + "...") if len(r['body'])>120 else r['body'])
                else:
                    print("Unknown list target")
            elif cmd == "/pending":
                rows = storage.get_pending_messages()
                print("--- pending ---")
                for r in rows:
                    print(f"{r['id'][:8]} | {r['type']} -> {r['dest_id'] or '<broadcast>'} | origin={r['origin_id']} ack={r['acknowledged']} resends={r['resend_count']}")
            elif cmd == "/quit":
                print("Shutting down...")
                break
            else:
                print("Unknown command")
    except (KeyboardInterrupt, EOFError):
        print("\nInterrupted. Shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        udp_peer.close()
        await asyncio.sleep(0.2)


# ------------------- ENTRY POINT -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DM Assistant UDP P2P store-and-forward with SOS")
    parser.add_argument("--id", required=True, help="Node ID (unique per instance)")
    parser.add_argument("--label", default=None, help="Human-friendly label")
    parser.add_argument("--feed", action="append", help="Alert feed URL", default=[])
    args = parser.parse_args()

    node_id = args.id
    label = args.label or node_id
    feeds = args.feed or DEFAULT_ALERT_FEEDS

    try:
        asyncio.run(main_loop(node_id, label, feeds))
    except Exception as e:
        print("Fatal:", e)
        sys.exit(1)
