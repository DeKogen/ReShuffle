# ReShuffle

Discord bot that shuffles members in a voice channel, keeps a live order as people join/leave, and can auto-run via scheduled events.

## Features
- Shuffle members from your current voice channel with a hybrid command
- Live-updating list that reacts to joins/leaves
- Schedule a voice event that auto-starts and auto-completes shuffle
- Attach shuffle to existing scheduled events
- List scheduled events and IDs
- Persist voice-channel activity per user in SQLite
- Report recent voice sessions, daily totals, and weekly totals

## Requirements
- Python 3.10+
- Discord bot with Message Content and Server Members intents enabled
- Permissions: View Channels, Send Messages, Read Message History
- For scheduling: Manage Events permission

## Setup
1. Copy `.env.example` to `.env` and set `DISCORD_TOKEN`. Optionally set `RELIABLE_ROLE_ID` and `TRUSTED_ROLE_ID`.
2. Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

3. Run the bot:

```bash
python Shuffle.py
```

## Commands
- `/shuffle [exclude]` (or `!shuffle [exclude]`) - shuffle members in your current voice channel; users with `Товарищ` or `надежный` can exclude members by mention, ID, or exact name
- `/shuffle_exclude_add <users>` - add users to a persistent exclusion list for all future shuffles; only `Товарищ` or `надежный`
- `/shuffle_exclude_remove <users>` - remove users from the persistent exclusion list; only `Товарищ` or `надежный`
- `/shuffle_exclude_list` - show the current persistent exclusion list; only `Товарищ` or `надежный`
- `/shuffle_hot_joiners_on` - allow brand-new late joiners to be added to active shuffles; only `Товарищ` or `надежный`
- `/shuffle_hot_joiners_off` - prevent brand-new late joiners from being added; reconnects still return; only `Товарищ` or `надежный`
- `/shuffle_hot_joiners_status` - show the current hot-joiner setting; only `Товарищ` or `надежный`
- `/schedule_event` - schedule a voice event that auto-runs shuffle
- `/schedule_event_menu` - open a modal to schedule an event
- `/attach_event <event_id>` - attach shuffle to an existing scheduled event
- `/list_events` - list scheduled server events and their IDs
- `/voice_stats [member]` - show today, this week, and all-time voice totals for a member
- `/voice_sessions [member] [limit]` - show recent tracked voice sessions for a member
- `/voice_daily [member] [days]` - show per-day totals for the last N days
- `/ping` - test if the bot is responsive
- `!sync` - sync application commands to the current guild

## Configuration
- `DISCORD_TOKEN` (required)
- `RELIABLE_ROLE_ID` (optional) - numeric role ID; if unset, edit `RELIABLE_ROLE_NAME` in `Shuffle.py`
- `TRUSTED_ROLE_ID` (optional) - numeric role ID; if unset, edit `TRUSTED_ROLE_NAME` in `Shuffle.py`
- `RESHUFFLE_DATA_DIR` (optional) - writable directory for runtime state files; useful in Docker

## Voice Tracking Storage
- Voice activity is stored in `voice_activity.sqlite3` inside the runtime data directory
- Active sessions survive bot restarts and are reconciled on reconnect/startup
- Persistent shuffle exclusions are stored in `persistent_shuffle_exclusions.json` inside the runtime data directory
- Guild hot-joiner settings are stored in `shuffle_settings.json` inside the runtime data directory
- Exclusion and hot-joiner setting operations are audited to `shuffle_admin_audit.jsonl` inside the runtime data directory

## Docker
```bash
docker build -t reshuffle .
docker run --env-file .env reshuffle
```
