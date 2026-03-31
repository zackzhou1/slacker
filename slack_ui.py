"""
slack_ui.py
Local Slack UI over the extracted SQLite database.

Usage:
    python slack_ui.py --db slack.db
    python slack_ui.py --db slack.db --port 5000
"""

import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, g, jsonify, render_template, request, send_file

app = Flask(__name__)
DB_PATH = "slack.db"

# Module-level user/channel caches (immutable data, safe to cache globally)
_user_cache: dict[str, dict] = {}
_channel_cache: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    if "db" not in g:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db

@app.teardown_appcontext
def close_db(_):
    db = g.pop("db", None)
    if db:
        db.close()

def setup_fts(conn: sqlite3.Connection):
    conn.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            text,
            content=messages,
            content_rowid=rowid,
            tokenize='unicode61'
        )
    """)
    count = conn.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0]
    if count == 0:
        print("Building search index...")
        conn.execute("INSERT INTO messages_fts(rowid, text) SELECT rowid, text FROM messages")
        conn.commit()
        print("Search index ready.")

def prime_caches(conn: sqlite3.Connection):
    """Load all users and channels into memory at startup."""
    for row in conn.execute("SELECT id, name, real_name, display_name FROM users"):
        _user_cache[row["id"]] = {
            "name": row["display_name"] or row["real_name"] or row["name"] or row["id"],
            "real_name": row["real_name"] or row["display_name"] or row["name"],
        }
    for row in conn.execute("SELECT id, name, type FROM channels"):
        _channel_cache[row["id"]] = {"name": row["name"], "type": row["type"]}
    print(f"Cached {len(_user_cache)} users, {len(_channel_cache)} channels.")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def user_display(user_id: str) -> str:
    return _user_cache.get(user_id, {}).get("name", user_id or "Unknown")

def format_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # User mentions: &lt;@U123&gt; → clickable @name
    def replace_mention(m):
        uid = m.group(1).split("|")[0]
        name = user_display(uid)
        return f'<a class="mention user-link" data-user-id="{uid}" href="#">@{name}</a>'
    text = re.sub(r"&lt;@([A-Z0-9|]+)&gt;", replace_mention, text)

    # Channel mentions: &lt;#C123|name&gt; → clickable #name
    def replace_channel(m):
        parts = m.group(1).split("|")
        ch_id = parts[0]
        ch_name = parts[1] if len(parts) > 1 else _channel_cache.get(ch_id, {}).get("name", ch_id)
        return f'<a class="mention ch-link" data-ch-id="{ch_id}" href="#">#{ch_name}</a>'
    text = re.sub(r"&lt;#([A-Z0-9|a-z_\-]+)&gt;", replace_channel, text)

    # URLs
    def replace_url(m):
        parts = m.group(1).split("|")
        url = parts[0]
        label = parts[1] if len(parts) > 1 else url
        return f'<a href="{url}" target="_blank" rel="noopener">{label}</a>'
    text = re.sub(r"&lt;(https?://[^&]+)&gt;", replace_url, text)

    # Slack markdown
    text = re.sub(r"```(.*?)```", r"<pre>\1</pre>", text, flags=re.DOTALL)
    text = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\*([^*\n]+)\*", r"<strong>\1</strong>", text)
    text = re.sub(r"\b_([^_\n]+)_\b", r"<em>\1</em>", text)
    text = re.sub(r"~([^~\n]+)~", r"<del>\1</del>", text)
    text = text.replace("\n", "<br>")

    return text

def format_ts(ts: str) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone()
        return dt.strftime("%-m/%-d/%y %-I:%M %p")
    except Exception:
        return ts

def channel_display_name(ch_id: str, ch_name: str, ch_type: str) -> str:
    if ch_type == "im" and ch_name and ch_name.startswith("U"):
        return user_display(ch_name)
    return ch_name or ch_id

def batch_file_lookup(raw_files_list: list[str]) -> dict:
    """Given a list of raw files JSON strings, return a dict of file_id → DB row."""
    ids = set()
    for raw in raw_files_list:
        try:
            for f in json.loads(raw or "[]"):
                if f.get("id"):
                    ids.add(f["id"])
        except Exception:
            pass
    if not ids:
        return {}
    db = get_db()
    placeholders = ",".join("?" * len(ids))
    rows = db.execute(
        f"SELECT id, local_path, mimetype FROM files WHERE id IN ({placeholders})",
        list(ids)
    ).fetchall()
    return {r["id"]: r for r in rows}


def serialize_files(raw_files: str, file_lookup: dict = None) -> list:
    try:
        files = json.loads(raw_files or "[]")
    except Exception:
        return []
    if not files:
        return []

    result = []
    for f in files:
        fid = f.get("id")
        if not fid:
            continue
        row = (file_lookup or {}).get(fid)
        local_path = row["local_path"] if row else None
        if local_path:
            p = Path(local_path)
            if not p.is_absolute():
                p = Path.cwd() / p
            downloaded = p.exists()
        else:
            downloaded = False
        result.append({
            "id": fid,
            "name": f.get("name", "file"),
            "title": f.get("title") or f.get("name", "file"),
            "mimetype": (row["mimetype"] if row else None) or f.get("mimetype", ""),
            "size": f.get("size", 0),
            "downloaded": downloaded,
            "url": f"/api/files/{fid}" if downloaded else None,
        })
    return result


def serialize_message(r, include_channel=False, file_lookup=None) -> dict:
    uid = r["user_id"]
    display = user_display(uid) if uid else "Unknown"
    keys = r.keys()
    out = {
        "id": r["id"],
        "ts": r["ts"],
        "ts_display": format_ts(r["ts"]),
        "user_id": uid,
        "user_name": display,
        "text": format_text(r["text"]),
        "thread_ts": r["thread_ts"],
        "reply_count": r["reply_count"] or 0 if "reply_count" in keys else 0,
        "reactions": json.loads(r["reactions"] or "[]") if "reactions" in keys else [],
        "files": serialize_files(r["files"], file_lookup) if "files" in keys else [],
    }
    if include_channel:
        ch_id = r["channel_id"]
        ch = _channel_cache.get(ch_id, {})
        out["channel_id"] = ch_id
        out["channel_name"] = channel_display_name(ch_id, ch.get("name", ""), ch.get("type", ""))
        out["channel_type"] = ch.get("type", "channel")
    return out

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    db = get_db()
    workspace = db.execute("SELECT name, domain FROM workspaces LIMIT 1").fetchone()
    return render_template("index.html", workspace=workspace)


@app.route("/api/channels")
def api_channels():
    db = get_db()
    rows = db.execute("""
        SELECT c.id, c.name, c.type, c.is_archived, c.fetched_at,
               COUNT(m.id) as msg_count
        FROM channels c
        LEFT JOIN messages m ON m.channel_id = c.id
        GROUP BY c.id
        HAVING COUNT(m.id) > 0
        ORDER BY
            CASE c.type WHEN 'channel' THEN 0 WHEN 'group' THEN 1 WHEN 'mpim' THEN 2 ELSE 3 END,
            c.name
    """).fetchall()

    channels = []
    for r in rows:
        name = channel_display_name(r["id"], r["name"], r["type"])
        channels.append({
            "id": r["id"],
            "name": name,
            "type": r["type"],
            "is_archived": r["is_archived"],
            "msg_count": r["msg_count"],
        })
    return jsonify(channels)


@app.route("/api/channels/<ch_id>/messages")
def api_messages(ch_id):
    db = get_db()
    before_ts = request.args.get("before")
    limit = int(request.args.get("limit", 50))

    query = """
        SELECT m.id, m.ts, m.user_id, m.text, m.thread_ts, m.reply_count,
               m.reactions, m.files
        FROM messages m
        WHERE m.channel_id = ?
          AND (m.thread_ts IS NULL OR m.thread_ts = m.ts)
    """
    params = [ch_id]

    if before_ts:
        query += " AND m.ts < ?"
        params.append(before_ts)

    query += " ORDER BY m.ts DESC LIMIT ?"
    params.append(limit + 1)

    rows = db.execute(query, params).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]

    file_lookup = batch_file_lookup([r["files"] for r in rows])
    return jsonify({
        "messages": [serialize_message(r, file_lookup=file_lookup) for r in rows],
        "has_more": has_more,
    })


@app.route("/api/channels/<ch_id>/thread/<thread_ts>")
def api_thread(ch_id, thread_ts):
    db = get_db()
    rows = db.execute("""
        SELECT m.id, m.ts, m.user_id, m.text, m.thread_ts,
               m.reply_count, m.reactions, m.files
        FROM messages m
        WHERE m.channel_id = ? AND m.thread_ts = ?
        ORDER BY m.ts ASC
    """, (ch_id, thread_ts)).fetchall()

    file_lookup = batch_file_lookup([r["files"] for r in rows])
    messages = [serialize_message(r, file_lookup=file_lookup) for r in rows]
    if messages:
        messages[0]["is_parent"] = True
    return jsonify(messages)


@app.route("/api/files/<file_id>")
def api_file(file_id):
    db = get_db()
    row = db.execute(
        "SELECT local_path, name, mimetype FROM files WHERE id = ?", (file_id,)
    ).fetchone()
    if not row or not row["local_path"]:
        return ("File not found or not downloaded", 404)
    path = Path(row["local_path"])
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        return ("File missing from disk", 404)
    return send_file(path, mimetype=row["mimetype"] or None,
                     download_name=row["name"], as_attachment=False)


@app.route("/api/channels/<ch_id>/files")
def api_channel_files(ch_id):
    db = get_db()
    rows = db.execute("""
        SELECT f.id, f.name, f.title, f.mimetype, f.size, f.message_ts,
               f.local_path, f.user_id
        FROM files f
        WHERE f.channel_id = ?
        ORDER BY f.message_ts DESC
    """, (ch_id,)).fetchall()

    return jsonify([{
        "id": r["id"],
        "name": r["name"],
        "title": r["title"] or r["name"],
        "mimetype": r["mimetype"] or "",
        "size": r["size"] or 0,
        "message_ts": r["message_ts"],
        "ts_display": format_ts(r["message_ts"]) if r["message_ts"] else "",
        "user_name": user_display(r["user_id"]) if r["user_id"] else "Unknown",
        "downloaded": r["local_path"] is not None,
        "url": f"/api/files/{r['id']}" if r["local_path"] else None,
    } for r in rows])


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    user_id = request.args.get("user_id", "").strip()

    if not q and not user_id:
        return jsonify([])

    db = get_db()
    seen = {}

    # 1. Filter by user ID (from clicking a @mention)
    if user_id:
        rows = db.execute("""
            SELECT m.id, m.ts, m.channel_id, m.user_id, m.text, m.thread_ts,
                   m.reply_count, m.reactions
            FROM messages m
            WHERE m.user_id = ?
              AND (m.thread_ts IS NULL OR m.thread_ts = m.ts)
            ORDER BY m.ts DESC
            LIMIT 100
        """, (user_id,)).fetchall()
        for r in rows:
            seen[r["id"]] = r

    if q and len(q) >= 2:
        # 2. FTS on message text
        try:
            rows = db.execute("""
                SELECT m.id, m.ts, m.channel_id, m.user_id, m.text, m.thread_ts,
                       m.reply_count, m.reactions
                FROM messages_fts f
                JOIN messages m ON m.rowid = f.rowid
                WHERE messages_fts MATCH ?
                ORDER BY rank
                LIMIT 50
            """, (q,)).fetchall()
            for r in rows:
                if r["id"] not in seen:
                    seen[r["id"]] = r
        except Exception:
            pass

        # 3. User name match — find messages sent by matching users
        like = f"%{q}%"
        rows = db.execute("""
            SELECT m.id, m.ts, m.channel_id, m.user_id, m.text, m.thread_ts,
                   m.reply_count, m.reactions
            FROM users u
            JOIN messages m ON m.user_id = u.id
            WHERE (u.real_name LIKE ? OR u.display_name LIKE ? OR u.name LIKE ?)
              AND (m.thread_ts IS NULL OR m.thread_ts = m.ts)
            ORDER BY m.ts DESC
            LIMIT 50
        """, (like, like, like)).fetchall()
        for r in rows:
            if r["id"] not in seen:
                seen[r["id"]] = r

    results = sorted(seen.values(), key=lambda r: float(r["ts"]), reverse=True)[:100]
    return jsonify([serialize_message(r, include_channel=True) for r in results])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="slack.db")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    DB_PATH = args.db

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    setup_fts(conn)
    prime_caches(conn)
    conn.close()

    print(f"Starting Slacker UI at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)
