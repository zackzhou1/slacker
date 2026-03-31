"""
slack_extract.py
Pulls all Slack data accessible to your user token and stores it in SQLite.

Usage:
    python slack_extract.py --token xoxc-... --cookie xoxd-...

xoxc- tokens (desktop/web client) require the 'd' session cookie from app.slack.com.
To find it: DevTools (F12) -> Application -> Cookies -> https://app.slack.com -> 'd'
"""

import argparse
import os
import sqlite3
import time
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = Path("slack.db")
BASE_URL = "https://slack.com/api"

# Slack rate limits vary by tier; 1 req/sec is safe for all endpoints
REQUEST_DELAY = 1.1  # seconds between requests

# Max messages to fetch per channel (None = all)
MAX_MESSAGES_PER_CHANNEL = None

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

class SlackClient:
    def __init__(self, token: str, cookie: str = None):
        self.token = token
        self.session = requests.Session()
        # xoxc- tokens must be sent in POST body, not Authorization header
        # The 'd' cookie is required alongside the token
        if cookie:
            self.session.cookies.set("d", cookie, domain=".slack.com")

    def call(self, method: str, _skip_delay: bool = False, **params) -> dict:
        if not _skip_delay:
            time.sleep(REQUEST_DELAY)
        params["token"] = self.token
        resp = self.session.post(f"{BASE_URL}/{method}", data=params)
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", 30))
            print(f"  Rate limited. Waiting {retry}s...")
            time.sleep(retry)
            return self.call(method, **params)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("ok"):
            error = data.get("error", "unknown")
            raise RuntimeError(f"Slack API error on {method}: {error}")
        return data

    def paginate(self, method: str, result_key: str, **params):
        """Yields all items across paginated responses."""
        cursor = None
        while True:
            if cursor:
                params["cursor"] = cursor
            data = self.call(method, **params)
            yield from data.get(result_key, [])
            cursor = data.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS workspaces (
            id TEXT PRIMARY KEY,
            name TEXT,
            domain TEXT,
            raw JSON
        );

        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            name TEXT,
            real_name TEXT,
            display_name TEXT,
            email TEXT,
            is_bot INTEGER,
            deleted INTEGER,
            raw JSON
        );

        CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY,
            name TEXT,
            type TEXT,       -- 'channel', 'group', 'im', 'mpim'
            is_private INTEGER,
            is_archived INTEGER,
            member_count INTEGER,
            topic TEXT,
            purpose TEXT,
            members JSON,    -- list of user IDs for DMs
            fetched_at TEXT,         -- NULL = incomplete, ISO timestamp = done
            resume_cursor TEXT,      -- pagination cursor to resume from if interrupted
            raw JSON
        );

        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,  -- channel_id + ts
            channel_id TEXT,
            ts TEXT,
            user_id TEXT,
            text TEXT,
            thread_ts TEXT,       -- non-null if in a thread
            reply_count INTEGER,
            reactions JSON,
            files JSON,
            raw JSON,
            FOREIGN KEY (channel_id) REFERENCES channels(id)
        );

        CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel_id);
        CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
        CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_ts);

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
        );

        CREATE INDEX IF NOT EXISTS idx_files_channel ON files(channel_id);
        CREATE INDEX IF NOT EXISTS idx_files_message ON files(message_ts);
    """)
    # migrate old column name if needed
    cols = [r[1] for r in conn.execute("PRAGMA table_info(channels)").fetchall()]
    if "last_fetched_ts" in cols and "fetched_at" not in cols:
        conn.execute("ALTER TABLE channels RENAME COLUMN last_fetched_ts TO fetched_at")
    if "resume_cursor" not in cols:
        conn.execute("ALTER TABLE channels ADD COLUMN resume_cursor TEXT")
    conn.commit()

# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def fetch_workspace(client: SlackClient, conn: sqlite3.Connection):
    print("Fetching workspace info...")
    data = client.call("auth.test")
    team_id = data["team_id"]

    team_data = client.call("team.info")
    team = team_data.get("team", {})

    conn.execute("""
        INSERT OR REPLACE INTO workspaces (id, name, domain, raw)
        VALUES (?, ?, ?, ?)
    """, (team_id, team.get("name"), team.get("domain"), json.dumps(team)))
    conn.commit()

    print(f"  Workspace: {team.get('name')} ({team_id})")
    return team_id, data.get("user_id")


def fetch_users(client: SlackClient, conn: sqlite3.Connection):
    print("Fetching users...")
    count = 0
    for user in client.paginate("users.list", "members", limit=200):
        profile = user.get("profile", {})
        conn.execute("""
            INSERT OR REPLACE INTO users
                (id, name, real_name, display_name, email, is_bot, deleted, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user["id"],
            user.get("name"),
            user.get("real_name"),
            profile.get("display_name"),
            profile.get("email"),
            int(user.get("is_bot", False)),
            int(user.get("deleted", False)),
            json.dumps(user),
        ))
        count += 1
    conn.commit()
    print(f"  {count} users saved")


def fetch_channels(client: SlackClient, conn: sqlite3.Connection):
    print("Fetching conversations (channels, DMs, group DMs)...")
    count = 0

    # types covers: public channels, private channels, DMs, group DMs
    for ch in client.paginate(
        "conversations.list", "channels",
        types="public_channel,private_channel,im,mpim",
        exclude_archived=False,
        limit=200,
    ):
        ch_type = (
            "im" if ch.get("is_im") else
            "mpim" if ch.get("is_mpim") else
            "group" if ch.get("is_private") else
            "channel"
        )
        members = ch.get("members", [])
        # For IMs, the other user is stored in 'user' field
        if ch_type == "im" and ch.get("user"):
            members = [ch["user"]]

        conn.execute("""
            INSERT OR REPLACE INTO channels
                (id, name, type, is_private, is_archived, member_count, topic, purpose, members, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            ch["id"],
            ch.get("name") or ch.get("user", f"DM-{ch['id']}"),
            ch_type,
            int(ch.get("is_private", False) or ch.get("is_im", False)),
            int(ch.get("is_archived", False)),
            ch.get("num_members", 0),
            ch.get("topic", {}).get("value", ""),
            ch.get("purpose", {}).get("value", ""),
            json.dumps(members),
            json.dumps(ch),
        ))
        count += 1

    conn.commit()
    print(f"  {count} conversations saved")


def save_message(conn: sqlite3.Connection, ch_id: str, msg: dict):
    conn.execute("""
        INSERT OR REPLACE INTO messages
            (id, channel_id, ts, user_id, text, thread_ts,
             reply_count, reactions, files, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        f"{ch_id}:{msg['ts']}",
        ch_id,
        msg["ts"],
        msg.get("user") or msg.get("bot_id"),
        msg.get("text", ""),
        msg.get("thread_ts"),
        msg.get("reply_count", 0),
        json.dumps(msg.get("reactions", [])),
        json.dumps(msg.get("files", [])),
        json.dumps(msg),
    ))


def fetch_messages(client: SlackClient, conn: sqlite3.Connection,
                   only_channels: list = None, include_dms: bool = True,
                   since_ts: str = None):
    channels = conn.execute(
        "SELECT id, name, type, fetched_at, resume_cursor FROM channels"
    ).fetchall()

    if only_channels:
        only_set = set(only_channels)
        channels = [
            c for c in channels
            if c[0] in only_set                          # match by ID
            or c[1] in only_set                          # match by name
            or (include_dms and c[2] in ("im", "mpim")) # optionally include DMs
        ]
        dm_note = " (incl. all DMs)" if include_dms else ""
        print(f"  Filtered to {len(channels)} channel(s){dm_note}")

    total_channels = len(channels)
    skipped = sum(1 for c in channels if c[3])  # fetched_at is set
    if skipped:
        print(f"  {skipped} already complete, skipping")

    for i, (ch_id, ch_name, ch_type, fetched_at, resume_cursor) in enumerate(channels, 1):
        display = ch_name or ch_id

        if fetched_at:
            # Already fully fetched — do an incremental update for new messages only
            newest_ts = conn.execute(
                "SELECT MAX(ts) FROM messages WHERE channel_id = ?", (ch_id,)
            ).fetchone()[0]

            # Use --since if provided and newer than what's in the DB
            oldest_param = newest_ts
            if since_ts and (not newest_ts or float(since_ts) > float(newest_ts)):
                oldest_param = since_ts

            if not oldest_param:
                print(f"  [{i}/{total_channels}] #{display} — skipping (no ts baseline)")
                continue

            ts_dt = datetime.fromtimestamp(float(oldest_param), tz=timezone.utc).astimezone()
            ts_str = ts_dt.strftime("%-m/%-d/%y %-I:%M %p")
            print(f"  [{i}/{total_channels}] #{display} ({ch_type}) [incremental since {ts_str}]...")
            new_count = 0
            try:
                # Single call first — avoids full paginate overhead when nothing is new
                data = client.call("conversations.history", channel=ch_id,
                                   oldest=oldest_param, limit=200)
                messages = [m for m in data.get("messages", []) if m["ts"] != oldest_param]

                if not messages:
                    print(f"    no new messages")
                    conn.execute("UPDATE channels SET fetched_at = ? WHERE id = ?",
                                 (datetime.now(timezone.utc).isoformat(), ch_id))
                    conn.commit()
                    continue

                for msg in messages:
                    save_message(conn, ch_id, msg)
                    new_count += 1
                    if msg.get("reply_count", 0) > 0 and msg.get("thread_ts") == msg["ts"]:
                        fetch_thread(client, conn, ch_id, msg["ts"])

                # Keep paginating if there are more pages
                cursor = data.get("response_metadata", {}).get("next_cursor")
                while cursor:
                    data = client.call("conversations.history", channel=ch_id,
                                       oldest=oldest_param, cursor=cursor, limit=200)
                    for msg in data.get("messages", []):
                        save_message(conn, ch_id, msg)
                        new_count += 1
                        if msg.get("reply_count", 0) > 0 and msg.get("thread_ts") == msg["ts"]:
                            fetch_thread(client, conn, ch_id, msg["ts"])
                    cursor = data.get("response_metadata", {}).get("next_cursor")

                conn.execute("UPDATE channels SET fetched_at = ? WHERE id = ?",
                             (datetime.now(timezone.utc).isoformat(), ch_id))
                conn.commit()
                print(f"    {new_count} new messages")
            except RuntimeError as e:
                print(f"    SKIP: {e}")
            continue

        resuming = resume_cursor is not None
        print(f"  [{i}/{total_channels}] #{display} ({ch_type}){' [resuming]' if resuming else ''}...")

        msg_count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE channel_id = ?", (ch_id,)
        ).fetchone()[0]

        try:
            cursor = resume_cursor
            while True:
                params = dict(channel=ch_id, limit=200)
                if cursor:
                    params["cursor"] = cursor

                data = client.call("conversations.history", **params)
                messages = data.get("messages", [])

                for msg in messages:
                    save_message(conn, ch_id, msg)
                    msg_count += 1
                    if msg.get("reply_count", 0) > 0 and msg.get("thread_ts") == msg["ts"]:
                        fetch_thread(client, conn, ch_id, msg["ts"])
                    if MAX_MESSAGES_PER_CHANNEL and msg_count >= MAX_MESSAGES_PER_CHANNEL:
                        break

                cursor = data.get("response_metadata", {}).get("next_cursor")

                # Save cursor after every page so we can resume if interrupted
                conn.execute(
                    "UPDATE channels SET resume_cursor = ? WHERE id = ?",
                    (cursor or None, ch_id)
                )
                conn.commit()

                if not cursor or (MAX_MESSAGES_PER_CHANNEL and msg_count >= MAX_MESSAGES_PER_CHANNEL):
                    break

            # Mark channel as fully complete
            conn.execute(
                "UPDATE channels SET fetched_at = ?, resume_cursor = NULL WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), ch_id)
            )
            conn.commit()
            print(f"    {msg_count} messages")

        except RuntimeError as e:
            conn.commit()  # save whatever we got and the resume cursor
            print(f"    SKIP: {e}")


def fetch_thread(client: SlackClient, conn: sqlite3.Connection, ch_id: str, thread_ts: str):
    try:
        for msg in client.paginate(
            "conversations.replies", "messages",
            channel=ch_id,
            ts=thread_ts,
            limit=200,
        ):
            if msg["ts"] == thread_ts:
                continue  # parent already saved
            msg_id = f"{ch_id}:{msg['ts']}"
            conn.execute("""
                INSERT OR REPLACE INTO messages
                    (id, channel_id, ts, user_id, text, thread_ts,
                     reply_count, reactions, files, raw)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                msg_id,
                ch_id,
                msg["ts"],
                msg.get("user") or msg.get("bot_id"),
                msg.get("text", ""),
                msg.get("thread_ts"),
                msg.get("reply_count", 0),
                json.dumps(msg.get("reactions", [])),
                json.dumps(msg.get("files", [])),
                json.dumps(msg),
            ))
    except RuntimeError:
        pass  # thread may be inaccessible


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_config() -> dict:
    config_path = Path("config.json")
    if config_path.exists():
        try:
            return json.loads(config_path.read_text())
        except Exception:
            pass
    return {}

def main():
    cfg = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--token", default=os.environ.get("SLACK_TOKEN") or cfg.get("token"))
    parser.add_argument("--cookie", default=os.environ.get("SLACK_COOKIE") or cfg.get("cookie"),
                        help="Value of the 'd' cookie from app.slack.com (required for xoxc- tokens)")
    parser.add_argument("--db", default=cfg.get("db", str(DB_PATH)))
    parser.add_argument("--channels", nargs="+", metavar="NAME_OR_ID",
                        help="Only fetch messages for these channels (names or IDs, space-separated)")
    parser.add_argument("--no-dms", action="store_true",
                        help="Exclude DMs and group DMs when using --channels")
    parser.add_argument("--since", metavar="YYYY-MM-DD",
                        help="Only fetch messages newer than this date (e.g. 2025-01-01)")
    args = parser.parse_args()

    since_ts = None
    if args.since:
        try:
            since_ts = str(datetime.strptime(args.since, "%Y-%m-%d")
                           .replace(tzinfo=timezone.utc).timestamp())
            print(f"Only fetching messages since {args.since}")
        except ValueError:
            sys.exit("ERROR: --since must be in YYYY-MM-DD format")

    if not args.token or args.token == "xoxc-...":
        sys.exit("ERROR: No token set. Add it to config.json or pass --token xoxc-...")

    print(f"Database: {args.db}")
    conn = sqlite3.connect(args.db)
    init_db(conn)

    client = SlackClient(args.token, cookie=args.cookie)

    # Step 1: workspace + auth check
    team_id, user_id = fetch_workspace(client, conn)
    print(f"  Authenticated as user {user_id}\n")

    # Step 2: users
    fetch_users(client, conn)
    print()

    # Step 3: channels/DMs
    fetch_channels(client, conn)
    print()

    # Step 4: messages (slowest step)
    print("Fetching messages (this will take a while)...")
    fetch_messages(client, conn, only_channels=args.channels, include_dms=not args.no_dms, since_ts=since_ts)

    # Summary
    stats = {
        "users": conn.execute("SELECT COUNT(*) FROM users").fetchone()[0],
        "channels": conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0],
        "messages": conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0],
    }
    print(f"\nDone. Saved to {args.db}")
    print(f"  Users:    {stats['users']}")
    print(f"  Channels: {stats['channels']}")
    print(f"  Messages: {stats['messages']}")

    conn.close()


if __name__ == "__main__":
    main()
