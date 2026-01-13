# ReShuffle

Discord bot that shuffles members in a voice channel, keeps a live order as people join/leave, and can auto-run via scheduled events.

## Features
- Shuffle members from your current voice channel with a hybrid command
- Live-updating list that reacts to joins/leaves
- Schedule a voice event that auto-starts and auto-completes shuffle
- Attach shuffle to existing scheduled events
- List scheduled events and IDs

## Requirements
- Python 3.10+
- Discord bot with Message Content and Server Members intents enabled
- Permissions: View Channels, Send Messages, Read Message History
- For scheduling: Manage Events permission

## Setup
1. Copy `.env.example` to `.env` and set `DISCORD_TOKEN`. Optionally set `RELIABLE_ROLE_ID`.
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
- `/shuffle` (or `!shuffle`) - shuffle members in your current voice channel
- `/schedule_event` - schedule a voice event that auto-runs shuffle
- `/schedule_event_menu` - open a modal to schedule an event
- `/attach_event <event_id>` - attach shuffle to an existing scheduled event
- `/list_events` - list scheduled server events and their IDs
- `/ping` - test if the bot is responsive
- `!sync` - sync application commands to the current guild

## Configuration
- `DISCORD_TOKEN` (required)
- `RELIABLE_ROLE_ID` (optional) - numeric role ID; if unset, edit `RELIABLE_ROLE_NAME` in `Shuffle.py`

## Docker
```bash
docker build -t reshuffle .
docker run --env-file .env reshuffle
```
