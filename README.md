# ReShuffle

Discord bot that shuffles members in a voice channel, keeps a live order as people join/leave, and can auto-run via scheduled events.

## Features
- Shuffle members from your current voice channel with a hybrid command
- Timed SDG breakout shuffle that physically moves members between rooms every 5 minutes
- Live-updating list that reacts to joins/leaves
- Schedule a voice event that auto-starts and auto-completes shuffle
- Attach shuffle to existing scheduled events
- List scheduled events and IDs
- Persist voice-channel activity per user in SQLite
- Report recent voice sessions, daily totals, and weekly totals
- Map Telegram identities to Discord display names for Matterbridge

## Requirements
- Python 3.10+
- Discord bot with Message Content and Server Members intents enabled
- Permissions: View Channels, Send Messages, Read Message History
- For scheduling: Manage Events permission
- For nick mapping: Administrator permission, role `Надежный` (`1434300421647761489`), or a role listed in `ALLOWED_ROLE_IDS`

## Setup
1. Copy `.env.example` to `.env` and set `DISCORD_TOKEN`. Optionally set `RELIABLE_ROLE_ID` and override `TRUSTED_ROLE_ID`.
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
- `/shuffle [exclude]` (or `!shuffle [exclude]`) - shuffle members in your current voice channel and generate the initial order with a `Надежный` member first when available; users with `Товарищ` or `Надежный` can exclude members by mention, ID, or exact name
- `/sdg_shuffle` - start timed breakout-room shuffling from your current voice channel; role `нашедшийся` stays together for 10 minutes, role `core` works like the old `**`, everyone else reshuffles every 5 minutes
- `/sdg_shuffle_stop` - stop the active timed SDG shuffle in the server
- `/shuffle_exclude_add <users>` - add users to a persistent exclusion list for all future shuffles; only `Товарищ` or `Надежный`
- `/shuffle_exclude_remove <users>` - remove users from the persistent exclusion list; only `Товарищ` or `Надежный`
- `/shuffle_exclude_list` - show the current persistent exclusion list; only `Товарищ` or `Надежный`
- `/shuffle_hot_joiners_on` - allow brand-new late joiners to be added to active shuffles; only `Товарищ` or `Надежный`
- `/shuffle_hot_joiners_off` - prevent brand-new late joiners from being added; reconnects still return; only `Товарищ` or `Надежный`
- `/shuffle_hot_joiners_status` - show the current hot-joiner setting; only `Товарищ` or `Надежный`
- `/schedule_event` - schedule a voice event that auto-runs shuffle
- `/schedule_event_menu` - open a modal to schedule an event
- `/attach_event <event_id>` - attach shuffle to an existing scheduled event
- `/list_events` - list scheduled server events and their IDs
- `/voice_stats [member]` - show today, this week, and all-time voice totals for a member
- `/voice_sessions [member] [limit]` - show recent tracked voice sessions for a member
- `/voice_daily [member] [days]` - show per-day totals for the last N days
- `/map set <tg_key> <dc_name> [dc_user] [reason]` - set a Telegram identity to Discord display-name mapping
- `/map del <tg_key> [reason]` - delete a nick mapping
- `/map get <tg_key>` - show one nick mapping
- `/map list [filter] [page]` - list nick mappings with paging
- `/map export` - export `nickmap.json`
- `/ping` - test if the bot is responsive
- `!sync` - sync application commands to the current guild

## Configuration
- `DISCORD_TOKEN` (required)
- `RELIABLE_ROLE_ID` (optional) - numeric role ID; if unset, edit `RELIABLE_ROLE_NAME` in `Shuffle.py`
- `TRUSTED_ROLE_ID` (optional) - numeric role ID for `Надежный`; defaults to `1434300421647761489`
- `SDG_NEWCOMER_ROLE_ID` (optional) - numeric role ID for `нашедшийся`; if unset, the bot matches by role name
- `SDG_CORE_ROLE_ID` (optional) - numeric role ID for `core`; if unset, the bot matches by role name
- `RESHUFFLE_DATA_DIR` (optional) - writable directory for runtime state files; useful in Docker
- `NICKMAP_JSON_PATH` (optional) - mapping JSON path; default is `nickmap.json` inside the runtime data directory
- `NICKMAP_TENGO_PATH` (optional) - generated Matterbridge Tengo script path; default is `nickmap.tengo` inside the runtime data directory
- `NICKMAP_TELEGRAM_ACCOUNT` (optional) - Matterbridge Telegram account name; default is `telegram.mytelegram`
- `AUDIT_LOG_PATH` (optional) - nickmap JSONL audit log path; default is `nickmap_audit.jsonl` inside the runtime data directory
- `AUDIT_CHANNEL_ID` (optional) - Discord channel ID for nickmap change audit messages
- `ALLOWED_ROLE_IDS` (optional) - comma-separated role IDs allowed to use `/map` commands in addition to administrators and `Надежный`

## Voice Tracking Storage
- Voice activity is stored in `voice_activity.sqlite3` inside the runtime data directory
- Active sessions survive bot restarts and are reconciled on reconnect/startup
- Persistent shuffle exclusions are stored in `persistent_shuffle_exclusions.json` inside the runtime data directory
- Guild hot-joiner settings are stored in `shuffle_settings.json` inside the runtime data directory
- Exclusion and hot-joiner setting operations are audited to `shuffle_admin_audit.jsonl` inside the runtime data directory
- In Docker, the runtime data directory must be a volume or bind mount. Without a persistent `/data` mount, exclusions are lost when the container is recreated.

## Matterbridge Nick Mapping
Nick mappings are stored in `nickmap.json` and each `/map set` or `/map del` regenerates `nickmap.tengo` with deterministic, sorted keys. The generated Tengo script only rewrites `msgUsername` when `msgAccount == "telegram.mytelegram"`.

Recommended key formats:
- `u:telegram_username` for a Telegram username without `@`
- `id:123456789` for a Telegram user ID when available

Short forms are accepted by `/map set`: a bare numeric key such as `123456789` is saved as `id:123456789`, and `@telegram_username` is saved as `u:telegram_username`.

Examples:

```bash
/map set tg_key:u:tg_user dc_name:"Discord Nick" reason:"match Telegram username"
/map set tg_key:id:123456789 dc_name:"Discord Nick" dc_user:@DiscordUser
/map list filter:tg_user
/map del tg_key:u:tg_user reason:"old account"
```

In Matterbridge, configure the Telegram bridge or gateway to load the generated script as its `InMessage` script, for example:

```toml
InMessage="/etc/matterbridge/nickmap.tengo"
```

## Docker
```bash
docker build -t reshuffle .
docker run --env-file .env reshuffle
```

For persistent runtime state with Docker Compose:

```bash
docker compose up --build -d
```

The included `docker-compose.yml` mounts a named volume at `/data` and sets `RESHUFFLE_DATA_DIR=/data`, so `persistent_shuffle_exclusions.json`, `shuffle_settings.json`, audit logs, nickmap files, and the SQLite voice activity database survive container recreation. Do not run `docker compose down -v` unless you intentionally want to delete that stored state.

## How to Deploy Nick Mapping
Matterbridge reads scripts from its own filesystem. If Matterbridge uses `/etc/matterbridge/nickmap.tengo` and the host path is `/root/matterbridge/config/nickmap.tengo`, mount that host directory into the bot container and set:

```env
NICKMAP_TENGO_PATH=/root/matterbridge/config/nickmap.tengo
NICKMAP_JSON_PATH=/data/nickmap.json
AUDIT_LOG_PATH=/data/nickmap_audit.jsonl
ALLOWED_ROLE_IDS=123456789012345678,234567890123456789
```

The bot process user must be able to create and replace `nickmap.tengo` in the mounted Matterbridge config directory. In Docker, either chown the mounted directory for UID `1000` or run the bot with a user that has write permission.

## Manual Test Checklist
- Run the bot and sync commands with `!sync`.
- Call `/map set tg_key:u:test_user dc_name:"Discord Test" reason:"manual test"`.
- Confirm `nickmap.json` contains the mapping and `nickmap.tengo` was regenerated.
- Confirm `nickmap_audit.jsonl` has a JSONL entry with actor, action, before/after, guild, channel, and reason.
- Call `/map list filter:test_user` and `/map get tg_key:u:test_user`.
- Call `/map del tg_key:u:test_user reason:"manual cleanup"` and confirm JSON/Tengo/audit updated.
