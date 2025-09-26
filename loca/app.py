from flask import Flask, g, jsonify, request, render_template, send_from_directory
import sqlite3
import os
from datetime import datetime
from flask_cors import CORS

DB_PATH = os.path.join(os.path.dirname(__file__), "shelters.db")

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)


def get_db():
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DB_PATH, check_same_thread=False)
        db.row_factory = sqlite3.Row
    return db


def init_db():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shelters (
            id TEXT PRIMARY KEY,
            name TEXT,
            description TEXT,
            lat REAL,
            lng REAL,
            capacity INTEGER,
            contact TEXT,
            created_at TEXT,
            modified_at TEXT,
            verified INTEGER DEFAULT 0,
            source TEXT DEFAULT 'local',
            version INTEGER DEFAULT 1
        )
        """
    )
    db.commit()


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/shelters", methods=["GET"])
def list_shelters():
    db = get_db()
    cur = db.execute("SELECT * FROM shelters")
    rows = cur.fetchall()
    shelters = []
    for r in rows:
        shelters.append({k: r[k] for k in r.keys()})
    return jsonify({"ok": True, "shelters": shelters})


@app.route("/api/shelters", methods=["POST"])
def add_shelter():
    data = request.get_json(force=True)
    required = ("id", "name", "lat", "lng")
    for k in required:
        if k not in data:
            return jsonify({"ok": False, "error": f"missing {k}"}), 400

    doc_id = data["id"]
    name = data.get("name", "")
    description = data.get("description", "")
    lat = float(data.get("lat"))
    lng = float(data.get("lng"))
    capacity = int(data.get("capacity") or 0)
    contact = data.get("contact", "")
    now = datetime.utcnow().isoformat() + "Z"
    verified = int(bool(data.get("verified")))

    db = get_db()
    # upsert: replace on conflict
    db.execute(
        """
        INSERT INTO shelters (id, name, description, lat, lng, capacity, contact, created_at, modified_at, verified, source, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          name=excluded.name,
          description=excluded.description,
          lat=excluded.lat,
          lng=excluded.lng,
          capacity=excluded.capacity,
          contact=excluded.contact,
          modified_at=excluded.modified_at,
          verified=excluded.verified,
          source=excluded.source,
          version=excluded.version
        """,
        (
            doc_id,
            name,
            description,
            lat,
            lng,
            capacity,
            contact,
            now,
            now,
            verified,
            data.get("source", "client"),
            int(data.get("version") or 1),
        ),
    )
    db.commit()
    return jsonify({"ok": True, "id": doc_id})


# optional static route for service worker (if needed)
@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        open(DB_PATH, "a").close()
    with app.app_context():
        init_db()
    print("Starting Flask server on http://127.0.0.1:5000")
    app.run(debug=True)
