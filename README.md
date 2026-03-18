# slacker

Extracts your Slack workspace data into a local SQLite database using your user session — no admin access, no migration tools, no public LLMs.

---

## Prerequisites

- Python 3.8+

```bash
pip install -r requirements.txt
```

---

## Getting your token and cookie

Both are found in your browser's DevTools while logged into Slack.

1. Open **Chrome or Edge**, go to `https://app.slack.com` and log in
2. Press `F12` to open DevTools

**Token (`xoxc-...`)**

- Go to **Application** → **Local Storage** → `https://app.slack.com`
- Find the key `localConfig_v2`
- The value is a JSON blob — copy the `token` field (starts with `xoxc-`)

**Cookie (`xoxd-...`)**

- Go to **Application** → **Cookies** → `https://app.slack.com`
- Find the row where **Name** = `d`
- Copy the **Value** column (starts with `xoxd-`)

Both are required. The token identifies you; the cookie proves your session is active. They typically stay valid for weeks — closing the browser tab is fine, but don't log out of Slack in that browser.

---

## Usage

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-...
```

Or use env vars to avoid pasting credentials each time:

```bash
export SLACK_TOKEN="xoxc-..."
export SLACK_COOKIE="xoxd-..."
python slack_extract.py
```

Output is written to `slack.db` (SQLite).

---

## Options

| Flag         | Default          | Description                                                            |
| ------------ | ---------------- | ---------------------------------------------------------------------- |
| `--token`    | `$SLACK_TOKEN`   | Your Slack `xoxc-` token                                               |
| `--cookie`   | `$SLACK_COOKIE`  | Your Slack `d` session cookie (`xoxd-`)                                |
| `--db`       | `slack.db`       | Output SQLite database path                                            |
| `--channels` | _(all)_          | Only fetch messages for these channels (names or IDs, space-separated) |
| `--no-dms`   | _(DMs included)_ | Exclude DMs and group DMs when using `--channels`                      |

### Fetch specific channels + all DMs

Pass channel names (no `#`) as a space-separated list after `--channels`.
DMs are always included unless you add `--no-dms`.

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-... \
  --channels dev_backend dev_chitchat feature_releases guba_announcements
```

For channel names with hyphens, just pass them as-is:

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-... \
  --channels engagement-ai-focus gubathon-2026-committee icom-reynolds-dev
```

Mix as many as you need — all on one line or split across lines with `\`:

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-... \
  --channels \
  dev_backend dev_chitchat engagement-ai-focus feature_releases \
  guba_announcements gubathon gubathon-2026-committee kengarffcentral \
  octopi_den octopi_ml questions_and_support rdl_labradors immortaliteam \
  feat_dashboard-v2 feat_copilot feat_ca_agent_assistant feat_rms \
  icom-reynolds-dev service-api project-combustion
```

### Fetch specific channels only, no DMs

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-... --no-dms \
  --channels dev_backend dev_chitchat feature_releases
```

### Fetch everything (all channels + all DMs)

```bash
python slack_extract.py --token xoxc-... --cookie xoxd-...
```

---

## Resuming after interruption

The script saves a pagination cursor after every page and marks each channel complete when fully fetched. Re-run the same command to resume — completed channels are skipped and any in-progress channel picks up from the last saved page.

---

## Database schema

```sql
workspaces   -- team name, domain
users        -- id, name, real_name, email, is_bot
channels     -- id, name, type (channel/group/im/mpim), topic, purpose, fetched_at
messages     -- id, channel_id, ts, user_id, text, thread_ts, reactions, files
```

All raw Slack API responses are stored as JSON columns.

### Useful queries

```sql
-- overall counts
SELECT 'users' as t, COUNT(*) FROM users
UNION SELECT 'channels', COUNT(*) FROM channels
UNION SELECT 'messages', COUNT(*) FROM messages;

-- channel progress
SELECT name, type, COUNT(m.id) as msg_count, c.fetched_at
FROM channels c
LEFT JOIN messages m ON m.channel_id = c.id
GROUP BY c.id ORDER BY msg_count DESC;

-- recent messages with names
SELECT u.real_name, m.text, datetime(CAST(m.ts AS REAL), 'unixepoch') as sent_at
FROM messages m
LEFT JOIN users u ON u.id = m.user_id
ORDER BY m.ts DESC LIMIT 20;
```

### Reading the DB while extraction runs

SQLite allows concurrent reads. Copy all three files to avoid lock issues:

```bash
cp slack.db slack_snapshot.db
cp slack.db-wal slack_snapshot.db-wal
cp slack.db-shm slack_snapshot.db-shm
```

Then open `slack_snapshot.db` in DB Browser for SQLite or PyCharm's database panel.

---

## Architecture

```
Slack API (xoxc- token + xoxd- cookie)
    │
    ├── auth.test              verify token
    ├── users.list             all workspace members
    ├── conversations.list     all channels, DMs, group DMs
    ├── conversations.history  messages per channel (paginated, resumable)
    └── conversations.replies  thread replies
    ↓
slack.db (SQLite)
```
