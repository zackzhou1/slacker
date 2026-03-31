"""
download_files.py
Downloads file attachments from Slack messages stored in the SQLite DB.

Files are saved to ./downloads/<file_id>/<original_filename>
Metadata is stored in the `files` table in SQLite.

Usage:
    python download_files.py
    python download_files.py --channel dev_backend
    python download_files.py --db slack.db --downloads-dir ./downloads
    python download_files.py --limit 100
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

CONFIG_PATH = Path("config.json")
DOWNLOADS_DIR = Path("downloads")
DB_PATH = Path("slack.db")
REQUEST_DELAY = 0.5  # seconds between downloads

SKIP_MIMETYPES = {
    "application/vnd.slack-docs",  # Slack posts (no binary to download)
}

IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp", "image/svg+xml"}


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except Exception:
            pass
    return {}


def get_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_files_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            name TEXT,
            title TEXT,
            mimetype TEXT,
            filetype TEXT,
            size INTEGER,
            channel_id TEXT,
            message_ts TEXT,
            user_id TEXT,
            local_path TEXT,
            downloaded_at TEXT,
            url_private TEXT,
            raw JSON
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_channel ON files(channel_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_files_message ON files(message_ts)")
    conn.commit()


def scan_messages(conn: sqlite3.Connection, channel_filter: str = None) -> list[dict]:
    """Extract all file attachments from messages."""
    query = """
        SELECT m.id as msg_id, m.ts, m.channel_id, m.user_id, m.files,
               c.name as channel_name
        FROM messages m
        LEFT JOIN channels c ON c.id = m.channel_id
        WHERE m.files IS NOT NULL AND m.files != '[]'
    """
    params = []
    if channel_filter:
        query += " AND c.name = ?"
        params.append(channel_filter)

    rows = conn.execute(query, params).fetchall()

    attachments = []
    for row in rows:
        try:
            files = json.loads(row["files"] or "[]")
        except Exception:
            continue
        for f in files:
            if not f.get("id"):
                continue
            attachments.append({
                "file_id": f["id"],
                "name": f.get("name", "unknown"),
                "title": f.get("title", ""),
                "mimetype": f.get("mimetype", ""),
                "filetype": f.get("filetype", ""),
                "size": f.get("size", 0),
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "message_ts": row["ts"],
                "user_id": row["user_id"],
                "url_private": f.get("url_private_download") or f.get("url_private", ""),
                "raw": f,
            })

    return attachments


def already_downloaded(conn: sqlite3.Connection, file_id: str) -> bool:
    row = conn.execute(
        "SELECT local_path FROM files WHERE id = ? AND downloaded_at IS NOT NULL",
        (file_id,)
    ).fetchone()
    if not row:
        return False
    # Verify the file actually exists on disk
    return row["local_path"] and Path(row["local_path"]).exists()


def download_file(token: str, cookie: str, url: str, dest: Path) -> bool:
    """Download a Slack private file. Returns True on success."""
    headers = {"Authorization": f"Bearer {token}"}
    cookies = {"d": cookie} if cookie else {}

    try:
        resp = requests.get(url, headers=headers, cookies=cookies,
                            stream=True, timeout=60)
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", 30))
            print(f"    Rate limited. Waiting {retry}s...")
            time.sleep(retry)
            return download_file(token, cookie, url, dest)

        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}")
            return False

        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        return True

    except Exception as e:
        print(f"    Error: {e}")
        return False


def safe_filename(name: str) -> str:
    """Strip characters unsafe for filenames."""
    return "".join(c for c in name if c.isalnum() or c in "._- ").strip() or "file"


def main():
    cfg = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.environ.get("SLACK_TOKEN") or cfg.get("token"))
    parser.add_argument("--cookie", default=os.environ.get("SLACK_COOKIE") or cfg.get("cookie"))
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--downloads-dir", default=str(DOWNLOADS_DIR))
    parser.add_argument("--channel", help="Only download files from this channel")
    parser.add_argument("--limit", type=int, help="Max number of files to download this run")
    parser.add_argument("--dry-run", action="store_true", help="List files without downloading")
    args = parser.parse_args()

    if not args.token or args.token.endswith("..."):
        sys.exit("ERROR: No token. Set it in config.json or pass --token")

    downloads_dir = Path(args.downloads_dir)
    conn = get_db(args.db)
    ensure_files_table(conn)

    print(f"Scanning messages for attachments...")
    attachments = scan_messages(conn, channel_filter=args.channel)

    # Deduplicate by file_id (same file can appear in multiple messages)
    seen = {}
    for a in attachments:
        if a["file_id"] not in seen:
            seen[a["file_id"]] = a
    attachments = list(seen.values())

    total = len(attachments)
    pending = [a for a in attachments if not already_downloaded(conn, a["file_id"])]
    print(f"  {total} total files, {len(pending)} not yet downloaded")

    if args.dry_run:
        for a in pending[:50]:
            size_kb = (a["size"] or 0) // 1024
            print(f"  [{a['channel_name']}] {a['name']} ({a['mimetype']}, {size_kb}KB)")
        if len(pending) > 50:
            print(f"  ... and {len(pending) - 50} more")
        return

    if args.limit:
        pending = pending[:args.limit]

    downloaded = 0
    skipped = 0
    failed = 0

    for i, a in enumerate(pending, 1):
        file_id = a["file_id"]
        name = safe_filename(a["name"])
        dest = downloads_dir / file_id / name
        size_kb = (a["size"] or 0) // 1024
        mimetype = a["mimetype"] or ""

        print(f"  [{i}/{len(pending)}] {a['channel_name']}/{name} ({size_kb}KB)...")

        # Skip types with no downloadable binary
        if mimetype in SKIP_MIMETYPES or not a["url_private"]:
            print(f"    skipped (no binary)")
            conn.execute("""
                INSERT OR REPLACE INTO files
                    (id, name, title, mimetype, filetype, size, channel_id,
                     message_ts, user_id, local_path, downloaded_at, url_private, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (file_id, a["name"], a["title"], mimetype, a["filetype"],
                  a["size"], a["channel_id"], a["message_ts"], a["user_id"],
                  None, datetime.now(timezone.utc).isoformat(),
                  a["url_private"], json.dumps(a["raw"])))
            conn.commit()
            skipped += 1
            continue

        ok = download_file(args.token, args.cookie, a["url_private"], dest)
        time.sleep(REQUEST_DELAY)

        if ok:
            conn.execute("""
                INSERT OR REPLACE INTO files
                    (id, name, title, mimetype, filetype, size, channel_id,
                     message_ts, user_id, local_path, downloaded_at, url_private, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (file_id, a["name"], a["title"], mimetype, a["filetype"],
                  a["size"], a["channel_id"], a["message_ts"], a["user_id"],
                  str(dest), datetime.now(timezone.utc).isoformat(),
                  a["url_private"], json.dumps(a["raw"])))
            conn.commit()
            downloaded += 1
            print(f"    saved to {dest}")
        else:
            failed += 1

    print(f"\nDone.")
    print(f"  Downloaded: {downloaded}")
    print(f"  Skipped:    {skipped}")
    print(f"  Failed:     {failed}")
    conn.close()


if __name__ == "__main__":
    main()
