import discord
from discord.ext import commands
import random
import asyncio
import json
import sqlite3
from datetime import datetime, timedelta, timezone
import os
import re
import traceback
from functools import lru_cache
from itertools import combinations_with_replacement
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

# -------- Intents & bot --------

intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True
intents.members = True  # do not forget to enable in Dev Portal
intents.guild_scheduled_events = True

bot = commands.Bot(command_prefix='!', intents=intents)
USE_GUILD_ONLY_APP_COMMANDS = True

# text_channel_id -> shuffle state
active_shuffles: dict[int, dict] = {}
# guild_id -> timed SDG shuffle state
active_sdg_shuffles: dict[int, dict] = {}
# (guild_id, voice_channel_id) -> camera enforcement state
active_camera_enforcements: dict[tuple[int, int], dict] = {}
triggered_event_occurrences: set[tuple[int, int]] = set()
triggering_event_occurrences: set[tuple[int, int]] = set()
scheduled_event_tasks: dict[tuple[int, int], asyncio.Task] = {}
planned_event_messages: dict[tuple[int, int], tuple[int, int]] = {}
event_text_channel_targets: dict[str, int] = {}
persistent_shuffle_exclusions: dict[int, set[int]] = {}
shuffle_settings: dict[int, dict[str, bool]] = {}
PROCESS_STARTED_AT = datetime.now(timezone.utc)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or timezone.utc

REMOVE_DELAY = 30  # seconds after leaving the voice channel
CAMERA_GRACE_SECONDS = 30  # seconds to allow camera-off state before disconnecting
SHUFFLE_TRACKING_WINDOW = 600  # seconds to keep accepting join/leave updates
SDG_ROUND_SECONDS = 300  # 5 minutes
STARTUP_CLOCK_SKEW = timedelta(seconds=5)
PLANNED_SHUFFLE_PREFIX = "⏳ **Shuffle planned:**"
SHUFFLE_LIST_PREFIX = "🎲 **Shuffled list:**"
FROZEN_EVENT_MARKER = "[замороженно]"
CAMERA_SOURCE_MANUAL = "manual"
CAMERA_SOURCE_SHUFFLE_PREFIX = "shuffle:"
SDG_NEWCOMER_DAYS_THRESHOLD = 10
SDG_CATEGORY_KEYS = ("fd", "fo", "sn", "c", "t", "a", "r")


def resolve_data_dir() -> str:
    """Pick a writable directory for runtime state files."""
    configured_dir = os.getenv("RESHUFFLE_DATA_DIR")
    if configured_dir:
        data_dir = os.path.abspath(os.path.expanduser(configured_dir))
        os.makedirs(data_dir, exist_ok=True)
        return data_dir

    if os.access(BASE_DIR, os.W_OK):
        return BASE_DIR

    fallback_dir = os.path.join(os.path.expanduser("~"), ".reshuffle")
    os.makedirs(fallback_dir, exist_ok=True)
    return fallback_dir


DATA_DIR = resolve_data_dir()
EVENT_TARGETS_FILE = os.path.join(DATA_DIR, "event_text_channel_targets.json")
PERSISTENT_EXCLUSIONS_FILE = os.path.join(DATA_DIR, "persistent_shuffle_exclusions.json")
SHUFFLE_SETTINGS_FILE = os.path.join(DATA_DIR, "shuffle_settings.json")
SHUFFLE_AUDIT_LOG_FILE = os.path.join(DATA_DIR, "shuffle_admin_audit.jsonl")
VOICE_STATS_DB_FILE = os.path.join(DATA_DIR, "voice_activity.sqlite3")


def utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


def to_storage_datetime(value: datetime) -> str:
    """Serialize a timezone-aware datetime to ISO-8601 UTC."""
    return value.astimezone(timezone.utc).isoformat()


def from_storage_datetime(value: str) -> datetime:
    """Parse a stored ISO-8601 datetime and normalize it to UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_trackable_voice_channel(channel) -> bool:
    """Track classic voice and stage channels for activity stats."""
    return isinstance(channel, (discord.VoiceChannel, discord.StageChannel))


def get_voice_db_connection() -> sqlite3.Connection:
    """Create a SQLite connection for voice activity data."""
    connection = sqlite3.connect(VOICE_STATS_DB_FILE)
    connection.row_factory = sqlite3.Row
    return connection


def init_voice_tracking_db() -> None:
    """Create the voice tracking tables if they do not exist yet."""
    with get_voice_db_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS voice_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_display_name TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                channel_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL CHECK(duration_seconds >= 0),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_user_started
            ON voice_sessions(guild_id, user_id, started_at DESC);

            CREATE INDEX IF NOT EXISTS idx_voice_sessions_guild_started
            ON voice_sessions(guild_id, started_at DESC);

            CREATE TABLE IF NOT EXISTS active_voice_sessions (
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                user_display_name TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                channel_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );
            """
        )


def format_duration(total_seconds: int) -> str:
    """Render a compact human-readable duration."""
    total_seconds = max(0, int(total_seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes or hours:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def format_timezone_label() -> str:
    """Return a short timezone label for reports."""
    now_local = datetime.now().astimezone(LOCAL_TIMEZONE)
    tz_name = now_local.tzname() or "local time"
    offset = now_local.strftime("%z")
    if not offset:
        return tz_name
    return f"{tz_name} ({offset[:3]}:{offset[3:]})"


def start_of_local_day(reference: Optional[datetime] = None) -> datetime:
    """Return the start of the local day for the provided instant."""
    local_reference = (reference or utc_now()).astimezone(LOCAL_TIMEZONE)
    return local_reference.replace(hour=0, minute=0, second=0, microsecond=0)


def start_of_local_week(reference: Optional[datetime] = None) -> datetime:
    """Return the start of the local week (Monday 00:00 local time)."""
    day_start = start_of_local_day(reference)
    return day_start - timedelta(days=day_start.weekday())


def normalize_report_limit(value: int, *, default: int, minimum: int, maximum: int) -> int:
    """Clamp a numeric report limit into a safe range."""
    if value is None:
        return default
    return max(minimum, min(maximum, value))


def resolve_member_display_name(
    guild: Optional[discord.Guild],
    user_id: int,
    fallback_name: str,
) -> str:
    """Prefer the current member display name and fall back to the stored snapshot."""
    if guild is not None:
        member = guild.get_member(user_id)
        if member is not None:
            return member.display_name
    return fallback_name or f"User {user_id}"


def resolve_voice_channel_label(
    guild: Optional[discord.Guild],
    channel_id: int,
    fallback_name: str,
) -> str:
    """Prefer the current voice channel name and fall back to the stored snapshot."""
    if guild is not None:
        channel = guild.get_channel(channel_id)
        if is_trackable_voice_channel(channel):
            return channel.name
    return fallback_name or f"channel-{channel_id}"


def get_active_voice_session_row(guild_id: int, user_id: int) -> Optional[sqlite3.Row]:
    """Fetch one active voice session for a guild member."""
    with get_voice_db_connection() as conn:
        return conn.execute(
            """
            SELECT guild_id, user_id, user_display_name, channel_id, channel_name, started_at
            FROM active_voice_sessions
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()


def list_active_voice_session_rows(guild_id: Optional[int] = None) -> list[sqlite3.Row]:
    """Fetch active voice sessions, optionally scoped to one guild."""
    query = (
        "SELECT guild_id, user_id, user_display_name, channel_id, channel_name, started_at "
        "FROM active_voice_sessions"
    )
    params: tuple = ()
    if guild_id is not None:
        query += " WHERE guild_id = ?"
        params = (guild_id,)
    query += " ORDER BY started_at DESC"

    with get_voice_db_connection() as conn:
        return conn.execute(query, params).fetchall()


def _finalize_active_voice_session(
    conn: sqlite3.Connection,
    active_row: sqlite3.Row,
    *,
    ended_at: datetime,
) -> dict:
    """Write a completed voice session and remove the active row."""
    started_at = from_storage_datetime(active_row["started_at"])
    ended_at = max(ended_at.astimezone(timezone.utc), started_at)
    duration_seconds = int((ended_at - started_at).total_seconds())

    conn.execute(
        """
        INSERT INTO voice_sessions (
            guild_id,
            user_id,
            user_display_name,
            channel_id,
            channel_name,
            started_at,
            ended_at,
            duration_seconds
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            active_row["guild_id"],
            active_row["user_id"],
            active_row["user_display_name"],
            active_row["channel_id"],
            active_row["channel_name"],
            to_storage_datetime(started_at),
            to_storage_datetime(ended_at),
            duration_seconds,
        ),
    )
    conn.execute(
        "DELETE FROM active_voice_sessions WHERE guild_id = ? AND user_id = ?",
        (active_row["guild_id"], active_row["user_id"]),
    )

    return {
        "guild_id": active_row["guild_id"],
        "user_id": active_row["user_id"],
        "user_display_name": active_row["user_display_name"],
        "channel_id": active_row["channel_id"],
        "channel_name": active_row["channel_name"],
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
    }


def start_voice_session(
    member: discord.Member,
    channel: discord.abc.GuildChannel,
    *,
    started_at: Optional[datetime] = None,
) -> None:
    """Start tracking a member's voice session, closing any stale active row first."""
    if member.bot or not is_trackable_voice_channel(channel):
        return

    session_started_at = (started_at or utc_now()).astimezone(timezone.utc)

    with get_voice_db_connection() as conn:
        active_row = conn.execute(
            """
            SELECT guild_id, user_id, user_display_name, channel_id, channel_name, started_at
            FROM active_voice_sessions
            WHERE guild_id = ? AND user_id = ?
            """,
            (member.guild.id, member.id),
        ).fetchone()

        if active_row is not None:
            if active_row["channel_id"] == channel.id:
                conn.execute(
                    """
                    UPDATE active_voice_sessions
                    SET user_display_name = ?, channel_name = ?
                    WHERE guild_id = ? AND user_id = ?
                    """,
                    (member.display_name, channel.name, member.guild.id, member.id),
                )
                return

            _finalize_active_voice_session(conn, active_row, ended_at=session_started_at)

        conn.execute(
            """
            INSERT OR REPLACE INTO active_voice_sessions (
                guild_id,
                user_id,
                user_display_name,
                channel_id,
                channel_name,
                started_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                member.guild.id,
                member.id,
                member.display_name,
                channel.id,
                channel.name,
                to_storage_datetime(session_started_at),
            ),
        )


def finish_voice_session(
    guild_id: int,
    user_id: int,
    *,
    ended_at: Optional[datetime] = None,
) -> Optional[dict]:
    """Finish and persist one active voice session if it exists."""
    session_ended_at = (ended_at or utc_now()).astimezone(timezone.utc)

    with get_voice_db_connection() as conn:
        active_row = conn.execute(
            """
            SELECT guild_id, user_id, user_display_name, channel_id, channel_name, started_at
            FROM active_voice_sessions
            WHERE guild_id = ? AND user_id = ?
            """,
            (guild_id, user_id),
        ).fetchone()

        if active_row is None:
            return None

        return _finalize_active_voice_session(conn, active_row, ended_at=session_ended_at)


def load_completed_voice_sessions(
    guild_id: int,
    *,
    user_id: Optional[int] = None,
    overlap_start: Optional[datetime] = None,
    overlap_end: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> list[sqlite3.Row]:
    """Load completed voice sessions that overlap a given time range."""
    conditions = ["guild_id = ?"]
    params: list = [guild_id]

    if user_id is not None:
        conditions.append("user_id = ?")
        params.append(user_id)
    if overlap_start is not None:
        conditions.append("ended_at > ?")
        params.append(to_storage_datetime(overlap_start))
    if overlap_end is not None:
        conditions.append("started_at < ?")
        params.append(to_storage_datetime(overlap_end))

    query = (
        "SELECT id, guild_id, user_id, user_display_name, channel_id, channel_name, "
        "started_at, ended_at, duration_seconds "
        "FROM voice_sessions "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY started_at DESC"
    )
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    with get_voice_db_connection() as conn:
        return conn.execute(query, tuple(params)).fetchall()


def load_active_voice_sessions(
    guild_id: int,
    *,
    user_id: Optional[int] = None,
) -> list[sqlite3.Row]:
    """Load active voice sessions for one guild, optionally for one user."""
    conditions = ["guild_id = ?"]
    params: list = [guild_id]

    if user_id is not None:
        conditions.append("user_id = ?")
        params.append(user_id)

    query = (
        "SELECT guild_id, user_id, user_display_name, channel_id, channel_name, started_at "
        "FROM active_voice_sessions "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY started_at DESC"
    )

    with get_voice_db_connection() as conn:
        return conn.execute(query, tuple(params)).fetchall()


def build_session_record_from_row(
    row: sqlite3.Row,
    *,
    active: bool,
    now: Optional[datetime] = None,
) -> dict:
    """Normalize completed and active DB rows into one report-friendly shape."""
    started_at = from_storage_datetime(row["started_at"])
    ended_at = None if active else from_storage_datetime(row["ended_at"])
    effective_end = (now or utc_now()).astimezone(timezone.utc) if active else ended_at

    return {
        "guild_id": row["guild_id"],
        "user_id": row["user_id"],
        "user_display_name": row["user_display_name"],
        "channel_id": row["channel_id"],
        "channel_name": row["channel_name"],
        "started_at": started_at,
        "ended_at": ended_at,
        "effective_end": effective_end,
        "duration_seconds": int((effective_end - started_at).total_seconds()),
        "active": active,
    }


def get_session_overlap_seconds(
    session_start: datetime,
    session_end: datetime,
    *,
    range_start: Optional[datetime] = None,
    range_end: Optional[datetime] = None,
) -> int:
    """Return the number of seconds a session overlaps the requested range."""
    effective_start = max(session_start, range_start) if range_start is not None else session_start
    effective_end = min(session_end, range_end) if range_end is not None else session_end
    return max(0, int((effective_end - effective_start).total_seconds()))


def summarize_voice_usage(
    guild_id: int,
    user_id: int,
    *,
    range_start: Optional[datetime] = None,
    range_end: Optional[datetime] = None,
) -> dict:
    """Summarize a user's voice activity over an optional time range."""
    now = utc_now()
    total_seconds = 0
    session_count = 0
    active_session = None

    for row in load_completed_voice_sessions(
        guild_id,
        user_id=user_id,
        overlap_start=range_start,
        overlap_end=range_end,
    ):
        session = build_session_record_from_row(row, active=False, now=now)
        overlap = get_session_overlap_seconds(
            session["started_at"],
            session["effective_end"],
            range_start=range_start,
            range_end=range_end,
        )
        if overlap <= 0:
            continue
        total_seconds += overlap
        session_count += 1

    for row in load_active_voice_sessions(guild_id, user_id=user_id):
        session = build_session_record_from_row(row, active=True, now=now)
        overlap = get_session_overlap_seconds(
            session["started_at"],
            session["effective_end"],
            range_start=range_start,
            range_end=range_end,
        )
        if overlap <= 0:
            continue
        total_seconds += overlap
        session_count += 1
        active_session = session

    return {
        "total_seconds": total_seconds,
        "session_count": session_count,
        "active_session": active_session,
    }


def build_daily_voice_breakdown(
    guild_id: int,
    user_id: int,
    *,
    days: int,
) -> list[tuple[str, int]]:
    """Return per-day totals for the last N local days."""
    days = normalize_report_limit(days, default=7, minimum=1, maximum=31)
    now = utc_now()
    now_local = now.astimezone(LOCAL_TIMEZONE)
    first_day_local = start_of_local_day(now) - timedelta(days=days - 1)
    period_start_utc = first_day_local.astimezone(timezone.utc)

    buckets: dict[str, int] = {}
    for offset in range(days):
        day_start_local = first_day_local + timedelta(days=offset)
        day_label = day_start_local.strftime("%Y-%m-%d")
        buckets[day_label] = 0

    completed_rows = load_completed_voice_sessions(
        guild_id,
        user_id=user_id,
        overlap_start=period_start_utc,
        overlap_end=now,
    )
    active_rows = load_active_voice_sessions(guild_id, user_id=user_id)

    sessions = [
        build_session_record_from_row(row, active=False, now=now)
        for row in completed_rows
    ]
    sessions.extend(
        build_session_record_from_row(row, active=True, now=now)
        for row in active_rows
    )

    for session in sessions:
        segment_start_local = max(
            session["started_at"].astimezone(LOCAL_TIMEZONE),
            first_day_local,
        )
        segment_end_local = min(
            session["effective_end"].astimezone(LOCAL_TIMEZONE),
            now_local,
        )
        if segment_end_local <= segment_start_local:
            continue

        cursor = segment_start_local
        while cursor < segment_end_local:
            next_day = start_of_local_day(cursor) + timedelta(days=1)
            chunk_end = min(next_day, segment_end_local)
            day_label = start_of_local_day(cursor).strftime("%Y-%m-%d")
            buckets[day_label] += int((chunk_end - cursor).total_seconds())
            cursor = chunk_end

    return list(buckets.items())


async def reconcile_active_voice_sessions() -> None:
    """Reconcile persisted active voice sessions with the current gateway state."""
    now = utc_now()
    live_members: dict[tuple[int, int], tuple[discord.Member, discord.abc.GuildChannel]] = {}

    for guild in bot.guilds:
        channels = list(guild.voice_channels) + list(getattr(guild, "stage_channels", []))
        for channel in channels:
            for member in channel.members:
                if member.bot:
                    continue
                live_members[(guild.id, member.id)] = (member, channel)

    active_rows = list_active_voice_session_rows()
    touched_keys: set[tuple[int, int]] = set()
    closed_count = 0
    opened_count = 0

    for row in active_rows:
        key = (row["guild_id"], row["user_id"])
        live_state = live_members.get(key)

        if live_state is None:
            if finish_voice_session(row["guild_id"], row["user_id"], ended_at=now) is not None:
                closed_count += 1
            continue

        member, channel = live_state
        touched_keys.add(key)

        if row["channel_id"] != channel.id:
            if finish_voice_session(row["guild_id"], row["user_id"], ended_at=now) is not None:
                closed_count += 1
            start_voice_session(member, channel, started_at=now)
            opened_count += 1
            continue

        with get_voice_db_connection() as conn:
            conn.execute(
                """
                UPDATE active_voice_sessions
                SET user_display_name = ?, channel_name = ?
                WHERE guild_id = ? AND user_id = ?
                """,
                (member.display_name, channel.name, row["guild_id"], row["user_id"]),
            )

    for key, live_state in live_members.items():
        if key in touched_keys:
            continue
        member, channel = live_state
        start_voice_session(member, channel, started_at=now)
        opened_count += 1

    if closed_count or opened_count:
        print(
            "Reconciled voice activity state: "
            f"closed={closed_count}, opened={opened_count}"
        )


def normalize_report_period(period: Optional[str], *, default: str = "week") -> str:
    """Map user-friendly report period values to canonical tokens."""
    normalized = (period or default).strip().lower()
    aliases = {
        "today": "today",
        "day": "today",
        "daily": "today",
        "week": "week",
        "weekly": "week",
        "all": "all",
        "alltime": "all",
        "all-time": "all",
    }
    if normalized not in aliases:
        raise ValueError("Use `today`, `week`, or `all`.")
    return aliases[normalized]


def get_report_period_bounds(period: str) -> tuple[str, Optional[datetime], Optional[datetime]]:
    """Return a display label plus UTC bounds for a report period."""
    normalized = normalize_report_period(period)
    if normalized == "today":
        start = start_of_local_day().astimezone(timezone.utc)
        return "today", start, None
    if normalized == "week":
        start = start_of_local_week().astimezone(timezone.utc)
        return "this week", start, None
    return "all time", None, None


def format_session_count(value: int) -> str:
    """Render session count with a stable singular/plural label."""
    suffix = "session" if value == 1 else "sessions"
    return f"{value} {suffix}"


async def clear_remote_global_commands_preserving_local_tree() -> int:
    """
    Clear remote global app commands without losing the in-memory command definitions
    that `copy_global_to()` needs for guild sync.
    """
    preserved_commands = list(bot.tree.get_commands(guild=None))
    bot.tree.clear_commands(guild=None)
    cleared_remote = await bot.tree.sync()

    for command in preserved_commands:
        try:
            bot.tree.add_command(command)
        except Exception as e:
            print(f"Failed to restore app command {command.name}: {e}")

    return len(cleared_remote)

# -------- Role permissions --------

RELIABLE_ROLE_NAME = "Товарищ"
_reliable_role_id_env = os.getenv("RELIABLE_ROLE_ID")
RELIABLE_ROLE_ID = int(_reliable_role_id_env) if _reliable_role_id_env else None
TRUSTED_ROLE_NAME = "надежный"
_trusted_role_id_env = os.getenv("TRUSTED_ROLE_ID")
TRUSTED_ROLE_ID = int(_trusted_role_id_env) if _trusted_role_id_env else None
USER_MENTION_PATTERN = re.compile(r"<@!?(\d+)>")
USER_ID_PATTERN = re.compile(r"\b(\d{15,20})\b")


def has_reliable_role(member: discord.Member) -> bool:
    """Check if member has the 'reliable' role (by ID if set, otherwise by name)."""
    if RELIABLE_ROLE_ID is not None:
        return any(role.id == RELIABLE_ROLE_ID for role in member.roles)
    required_name = RELIABLE_ROLE_NAME.casefold()
    return any(role.name.casefold() == required_name for role in member.roles)


def has_trusted_role(member: discord.Member) -> bool:
    """Check if member has the 'trusted' role (by ID if set, otherwise by name)."""
    return member_has_configured_role(
        member,
        role_id=TRUSTED_ROLE_ID,
        role_name=TRUSTED_ROLE_NAME,
    )


def has_shuffle_exclusion_access(member: discord.Member) -> bool:
    """Allow exclusion management for either the companion or trusted role."""
    allowed_role_ids = {role_id for role_id in (RELIABLE_ROLE_ID, TRUSTED_ROLE_ID) if role_id is not None}
    if allowed_role_ids and any(role.id in allowed_role_ids for role in member.roles):
        return True

    allowed_role_names = {
        RELIABLE_ROLE_NAME.casefold(),
        TRUSTED_ROLE_NAME.casefold(),
    }
    return any(role.name.casefold() in allowed_role_names for role in member.roles)


def get_shuffle_exclusion_access_label() -> str:
    """Return a readable role requirement label for user-facing errors."""
    return f"`{RELIABLE_ROLE_NAME}` or `{TRUSTED_ROLE_NAME}`"


SDG_NEWCOMER_ROLE_NAME = "нашедшийся"
_sdg_newcomer_role_id_env = os.getenv("SDG_NEWCOMER_ROLE_ID")
SDG_NEWCOMER_ROLE_ID = int(_sdg_newcomer_role_id_env) if _sdg_newcomer_role_id_env else None
SDG_CORE_ROLE_NAME = "core"
_sdg_core_role_id_env = os.getenv("SDG_CORE_ROLE_ID")
SDG_CORE_ROLE_ID = int(_sdg_core_role_id_env) if _sdg_core_role_id_env else None


def member_has_configured_role(
    member: discord.Member,
    *,
    role_id: Optional[int],
    role_name: str,
) -> bool:
    """Check for a role by configured ID first, then by case-insensitive name."""
    if role_id is not None:
        return any(role.id == role_id for role in member.roles)
    required_name = role_name.casefold()
    return any(role.name.casefold() == required_name for role in member.roles)


def resolve_excluded_members(
    guild: discord.Guild,
    raw_value: str,
    *,
    allow_unknown_ids: bool = False,
) -> tuple[set[int], list[str]]:
    """Resolve mentions, IDs, or exact names into a set of member IDs."""
    resolved_member_ids: set[int] = set()
    unresolved: list[str] = []
    remaining = raw_value.strip()

    for match in USER_MENTION_PATTERN.finditer(remaining):
        user_id = int(match.group(1))
        member = guild.get_member(user_id)
        if member is None or member.bot:
            unresolved.append(match.group(0))
            continue
        resolved_member_ids.add(member.id)
    remaining = USER_MENTION_PATTERN.sub(" ", remaining)

    for match in USER_ID_PATTERN.finditer(remaining):
        user_id = int(match.group(1))
        member = guild.get_member(user_id)
        if member is None:
            if allow_unknown_ids:
                resolved_member_ids.add(user_id)
                continue
            unresolved.append(match.group(1))
            continue
        if member.bot:
            unresolved.append(match.group(1))
            continue
        resolved_member_ids.add(member.id)
    remaining = USER_ID_PATTERN.sub(" ", remaining)

    for token in [part.strip() for part in re.split(r"[,;\n]+", remaining) if part.strip()]:
        token_cf = token.casefold()
        matches = [
            member
            for member in guild.members
            if not member.bot
            and (
                member.display_name.casefold() == token_cf
                or member.name.casefold() == token_cf
                or str(member).casefold() == token_cf
            )
        ]
        if len(matches) == 1:
            resolved_member_ids.add(matches[0].id)
            continue
        if len(matches) > 1:
            unresolved.append(f"{token} (ambiguous)")
            continue
        unresolved.append(token)

    return resolved_member_ids, unresolved


def load_persistent_shuffle_exclusions() -> dict[int, set[int]]:
    """Load guild-level persistent shuffle exclusions from disk."""
    try:
        with open(PERSISTENT_EXCLUSIONS_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Failed to load persistent shuffle exclusions: {e}")
        return {}

    if not isinstance(raw, dict):
        return {}

    loaded: dict[int, set[int]] = {}
    for guild_id, user_ids in raw.items():
        try:
            guild_id_int = int(guild_id)
        except (TypeError, ValueError):
            continue

        if not isinstance(user_ids, list):
            continue

        normalized_user_ids: set[int] = set()
        for user_id in user_ids:
            try:
                normalized_user_ids.add(int(user_id))
            except (TypeError, ValueError):
                continue

        if normalized_user_ids:
            loaded[guild_id_int] = normalized_user_ids

    return loaded


def save_persistent_shuffle_exclusions() -> None:
    """Persist guild-level shuffle exclusions to disk."""
    serializable = {
        str(guild_id): sorted(user_ids)
        for guild_id, user_ids in persistent_shuffle_exclusions.items()
        if user_ids
    }

    try:
        with open(PERSISTENT_EXCLUSIONS_FILE, "w", encoding="utf-8") as fh:
            json.dump(serializable, fh, indent=2, sort_keys=True)
    except Exception as e:
        print(f"Failed to save persistent shuffle exclusions: {e}")


def get_persistent_excluded_member_ids(guild_id: int) -> set[int]:
    """Return the persistent exclusion set for one guild."""
    return set(persistent_shuffle_exclusions.get(guild_id, set()))


def add_persistent_excluded_members(guild_id: int, member_ids: set[int]) -> int:
    """Add members to the guild-level persistent shuffle exclusions."""
    current = persistent_shuffle_exclusions.setdefault(guild_id, set())
    before_count = len(current)
    current.update(member_ids)
    if current:
        persistent_shuffle_exclusions[guild_id] = current
    save_persistent_shuffle_exclusions()
    return len(current) - before_count


def remove_persistent_excluded_members(guild_id: int, member_ids: set[int]) -> int:
    """Remove members from the guild-level persistent shuffle exclusions."""
    current = persistent_shuffle_exclusions.get(guild_id)
    if not current:
        return 0

    before_count = len(current)
    current.difference_update(member_ids)
    removed_count = before_count - len(current)
    if current:
        persistent_shuffle_exclusions[guild_id] = current
    else:
        persistent_shuffle_exclusions.pop(guild_id, None)
    save_persistent_shuffle_exclusions()
    return removed_count


def format_member_list(guild: discord.Guild, member_ids: set[int]) -> str:
    """Render a stable, readable list of members from IDs."""
    names = []
    for user_id in sorted(member_ids):
        member = guild.get_member(user_id)
        names.append(member.display_name if member else f"Unknown user ({user_id})")
    return ", ".join(names)


def load_shuffle_settings() -> dict[int, dict[str, bool]]:
    """Load guild-level shuffle behavior settings from disk."""
    try:
        with open(SHUFFLE_SETTINGS_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Failed to load shuffle settings: {e}")
        return {}

    if not isinstance(raw, dict):
        return {}

    loaded: dict[int, dict[str, bool]] = {}
    for guild_id, settings in raw.items():
        try:
            guild_id_int = int(guild_id)
        except (TypeError, ValueError):
            continue

        if not isinstance(settings, dict):
            continue

        loaded[guild_id_int] = {
            "allow_hot_joiners": bool(settings.get("allow_hot_joiners", True)),
            "require_camera": bool(settings.get("require_camera", False)),
        }

    return loaded


def save_shuffle_settings() -> None:
    """Persist guild-level shuffle behavior settings to disk."""
    serializable = {
        str(guild_id): settings
        for guild_id, settings in shuffle_settings.items()
        if settings
    }

    try:
        with open(SHUFFLE_SETTINGS_FILE, "w", encoding="utf-8") as fh:
            json.dump(serializable, fh, indent=2, sort_keys=True)
    except Exception as e:
        print(f"Failed to save shuffle settings: {e}")


def get_allow_hot_joiners(guild_id: int) -> bool:
    """Return whether brand-new joiners should be added to active shuffles."""
    return bool(shuffle_settings.get(guild_id, {}).get("allow_hot_joiners", True))


def set_allow_hot_joiners(guild_id: int, enabled: bool) -> None:
    """Persist the hot-joiner behavior toggle for one guild."""
    guild_settings = dict(shuffle_settings.get(guild_id, {}))
    guild_settings["allow_hot_joiners"] = bool(enabled)
    shuffle_settings[guild_id] = guild_settings
    save_shuffle_settings()


def get_require_camera_for_shuffles(guild_id: int) -> bool:
    """Return whether active/future shuffles should enforce camera usage."""
    return bool(shuffle_settings.get(guild_id, {}).get("require_camera", False))


def set_require_camera_for_shuffles(guild_id: int, enabled: bool) -> None:
    """Persist the shuffle camera enforcement toggle for one guild."""
    guild_settings = dict(shuffle_settings.get(guild_id, {}))
    guild_settings.setdefault("allow_hot_joiners", True)
    guild_settings["require_camera"] = bool(enabled)
    shuffle_settings[guild_id] = guild_settings
    save_shuffle_settings()


def append_shuffle_audit_log(
    *,
    action: str,
    guild: Optional[discord.Guild],
    actor: Optional[discord.Member],
    channel_id: Optional[int] = None,
    details: Optional[dict] = None,
) -> None:
    """Append one JSONL audit record for exclusion-related operations."""
    record = {
        "timestamp": utc_now().isoformat(),
        "action": action,
        "guild_id": guild.id if guild else None,
        "guild_name": guild.name if guild else None,
        "actor_id": actor.id if actor else None,
        "actor_name": str(actor) if actor else None,
        "actor_display_name": actor.display_name if actor else None,
        "channel_id": channel_id,
        "details": details or {},
    }

    try:
        with open(SHUFFLE_AUDIT_LOG_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"Failed to write shuffle audit log: {e}")

    print(f"Shuffle audit: {json.dumps(record, ensure_ascii=False)}")


def is_frozen_event(event: discord.ScheduledEvent) -> bool:
    """Frozen events must not auto-post notices or run shuffles."""
    return FROZEN_EVENT_MARKER in event.name.casefold()


async def maybe_delete_thread_created_message(message: discord.Message) -> bool:
    """Delete Discord's automatic 'thread created' system message when possible."""
    if message.type is not discord.MessageType.thread_created:
        return False

    try:
        await message.delete()
    except discord.NotFound:
        pass
    except discord.Forbidden:
        print(
            "Cannot delete thread-created system message "
            f"{message.id} in channel {message.channel.id}: missing permissions"
        )
    except discord.HTTPException as e:
        print(f"Failed to delete thread-created system message {message.id}: {e}")

    return True


def make_event_target_key(event_id: int, start_time: Optional[datetime]) -> str:
    """Build a stable storage key for one event occurrence."""
    start_ts = int(start_time.timestamp()) if start_time else 0
    return f"{event_id}:{start_ts}"


def load_event_text_channel_targets() -> dict[str, int]:
    """Load saved event/occurrence -> text channel bindings from disk."""
    try:
        with open(EVENT_TARGETS_FILE, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Failed to load event text-channel targets: {e}")
        return {}

    if not isinstance(raw, dict):
        return {}

    loaded: dict[str, int] = {}
    for event_key, channel_id in raw.items():
        try:
            loaded[str(event_key)] = int(channel_id)
        except (TypeError, ValueError):
            continue
    return loaded


def save_event_text_channel_targets() -> None:
    """Persist event -> text channel bindings to disk."""
    try:
        with open(EVENT_TARGETS_FILE, "w", encoding="utf-8") as fh:
            json.dump(event_text_channel_targets, fh, indent=2, sort_keys=True)
    except Exception as e:
        print(f"Failed to save event text-channel targets: {e}")


def remember_event_text_channel(
    event_id: int,
    text_channel_id: int,
    *,
    start_time: Optional[datetime],
) -> None:
    """Persist the text channel chosen for one event occurrence."""
    key = make_event_target_key(event_id, start_time)
    event_text_channel_targets[key] = text_channel_id
    save_event_text_channel_targets()
    print(
        f"Saved shuffle target channel {text_channel_id} "
        f"for event occurrence {key}"
    )


def forget_event_text_channel(event_id: int, start_time: Optional[datetime]) -> None:
    """Forget a saved text channel for one event occurrence."""
    key = make_event_target_key(event_id, start_time)
    if event_text_channel_targets.pop(key, None) is not None:
        save_event_text_channel_targets()
        print(f"Removed saved shuffle target for event occurrence {key}")


def move_event_text_channel_target(
    event_id: int,
    before_start_time: Optional[datetime],
    after_start_time: Optional[datetime],
) -> None:
    """Move a saved channel binding when an occurrence is rescheduled."""
    before_key = make_event_target_key(event_id, before_start_time)
    after_key = make_event_target_key(event_id, after_start_time)
    if before_key == after_key or after_key in event_text_channel_targets:
        return

    channel_id = event_text_channel_targets.pop(before_key, None)
    if channel_id is None:
        return

    event_text_channel_targets[after_key] = channel_id
    save_event_text_channel_targets()
    print(f"Moved saved shuffle target from {before_key} to {after_key}")


def resolve_message_channel(
    guild: discord.Guild,
    channel_id: int,
) -> Optional[discord.abc.Messageable]:
    """Resolve a text-capable guild channel or thread by ID."""
    channel = None
    get_channel_or_thread = getattr(guild, "get_channel_or_thread", None)
    if callable(get_channel_or_thread):
        channel = get_channel_or_thread(channel_id)
    if channel is None:
        channel = guild.get_channel(channel_id)
    if channel is None:
        get_thread = getattr(guild, "get_thread", None)
        if callable(get_thread):
            channel = get_thread(channel_id)
    return channel


def is_message_channel(channel) -> bool:
    """Return True for channels/threads we can post shuffle messages into."""
    return channel is not None and callable(getattr(channel, "send", None))


async def fetch_message_channel(
    guild: discord.Guild,
    channel_id: int,
) -> Optional[discord.abc.Messageable]:
    """
    Resolve a text-capable channel or thread, falling back to the API when the cache
    is cold (common after restarts or for threads).
    """
    channel = resolve_message_channel(guild, channel_id)
    if is_message_channel(channel):
        return channel

    try:
        channel = await bot.fetch_channel(channel_id)
    except discord.NotFound:
        return None
    except discord.Forbidden:
        print(
            f"Cannot access saved message channel {channel_id} in guild {guild.id}: "
            "missing permissions"
        )
        return None
    except discord.HTTPException as e:
        print(f"Failed to fetch saved message channel {channel_id}: {e}")
        return None

    if getattr(channel, "guild", None) is not None and channel.guild.id != guild.id:
        print(
            f"Saved message channel {channel_id} belongs to guild "
            f"{channel.guild.id}, expected {guild.id}"
        )
        return None

    if is_message_channel(channel):
        return channel

    print(f"Saved channel {channel_id} is not text-capable: {type(channel).__name__}")
    return None


event_text_channel_targets.update(load_event_text_channel_targets())
persistent_shuffle_exclusions.update(load_persistent_shuffle_exclusions())
shuffle_settings.update(load_shuffle_settings())
init_voice_tracking_db()


# -------- Events --------

@bot.event
async def on_ready():
    print(f'{bot.user} connected. Guilds: {len(bot.guilds)}')
    print(f"Runtime data dir: {DATA_DIR}")
    if USE_GUILD_ONLY_APP_COMMANDS:
        try:
            cleared_count = await clear_remote_global_commands_preserving_local_tree()
            print(f"Cleared global app commands. Remaining global count: {cleared_count}")
        except Exception as e:
            print(f"Failed to clear global app commands: {e}")
    else:
        try:
            synced = await bot.tree.sync()
            print(f'Synced {len(synced)} command(s) globally')
        except Exception as e:
            print(f'Failed to sync commands: {e}')

    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            bot.tree.copy_global_to(guild=guild)
            guild_synced = await bot.tree.sync(guild=guild)
            print(
                f"Synced {len(guild_synced)} command(s) to guild "
                f"{guild.name} ({guild.id})"
            )
        except Exception as e:
            print(f"Failed to sync commands to guild {guild.id}: {e}")

    # Do not replay shuffles for events that were already active before startup.
    # A restart clears in-memory dedupe state, so we only mark those occurrences
    # as seen to prevent duplicate shuffle messages after reconnects/redeploys.
    for guild in bot.guilds:
        try:
            events = await guild.fetch_scheduled_events()
        except Exception as e:
            print(f"Failed to fetch scheduled events for {guild.id}: {e}")
            continue

        for ev in events:
            if is_frozen_event(ev):
                continue

            if ev.status is discord.EventStatus.active:
                start_ts = int(ev.start_time.timestamp()) if ev.start_time else 0
                triggered_event_occurrences.add((ev.id, start_ts))
                continue

            if (
                ev.entity_type is discord.EntityType.voice
                and ev.start_time is not None
                and ev.start_time > datetime.now(timezone.utc)
            ):
                voice_channel = guild.get_channel(ev.channel_id) if ev.channel_id else None
                if not isinstance(voice_channel, discord.VoiceChannel):
                    continue

                text_channel = await pick_saved_or_linked_text_channel(
                    guild,
                    ev.id,
                    voice_channel,
                    ev.start_time,
                )
                if text_channel is None:
                    continue

                end_time = ev.end_time or (ev.start_time + timedelta(hours=2))
                schedule_event_lifecycle(
                    guild.id,
                    ev.id,
                    ev.start_time,
                    end_time,
                    text_channel.id,
                )

    await reconcile_active_voice_sessions()


@bot.event
async def on_message(message: discord.Message):
    if await maybe_delete_thread_created_message(message):
        return
    if message.author == bot.user:
        return
    await bot.process_commands(message)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    """Log command failures and return a visible error instead of timing out."""
    if isinstance(error, commands.CommandNotFound):
        return

    original = getattr(error, "original", error)
    print("Command error:")
    print("".join(traceback.format_exception(type(original), original, original.__traceback__)))

    if ctx.interaction is not None:
        await finish_hybrid_command(ctx, f"Command failed: `{original}`")
        return

    try:
        await ctx.send(f"Command failed: `{original}`")
    except Exception as send_error:
        print(f"Failed to send command error message: {send_error}")


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: discord.app_commands.AppCommandError,
):
    """Log slash command failures and answer the interaction with the exception."""
    original = getattr(error, "original", error)
    print("App command error:")
    print("".join(traceback.format_exception(type(original), original, original.__traceback__)))
    await send_interaction_error(interaction, f"Slash command failed: `{original}`")


@bot.event
async def on_guild_scheduled_event_update(before, after):
    """Auto-trigger shuffle when a voice scheduled event becomes active."""
    before_key = event_occurrence_key(before.id, before.start_time)
    after_key = event_occurrence_key(after.id, after.start_time)
    if before_key != after_key:
        move_event_text_channel_target(before.id, before.start_time, after.start_time)
        task = scheduled_event_tasks.pop(before_key, None)
        if task is not None:
            task.cancel()
        planned_event_messages.pop(before_key, None)

    if is_frozen_event(after):
        task = scheduled_event_tasks.pop(after_key, None)
        if task is not None:
            task.cancel()
        planned_event_messages.pop(after_key, None)
        return

    if after.status is discord.EventStatus.active and before.status is not discord.EventStatus.active:
        if (
            after.start_time is not None
            and after.start_time <= (PROCESS_STARTED_AT + STARTUP_CLOCK_SKEW)
        ):
            start_ts = int(after.start_time.timestamp())
            triggered_event_occurrences.add((after.id, start_ts))
            print(f"Ignoring scheduled event update for already-started event {after.id}")
            return
        await trigger_shuffle_for_event(after)

    if (
        after.entity_type is discord.EntityType.voice
        and after.start_time is not None
        and after.status is discord.EventStatus.scheduled
    ):
        voice_channel = after.guild.get_channel(after.channel_id) if after.channel_id else None
        if isinstance(voice_channel, discord.VoiceChannel):
            text_channel = await pick_saved_or_linked_text_channel(
                after.guild,
                after.id,
                voice_channel,
                after.start_time,
            )
            if text_channel is not None:
                end_time = after.end_time or (after.start_time + timedelta(hours=2))
                schedule_event_lifecycle(
                    after.guild.id,
                    after.id,
                    after.start_time,
                    end_time,
                    text_channel.id,
                )


# -------- Shuffle helpers --------

def normalize_channel_name(name: str) -> str:
    """Normalize names for matching text channels to voice channels."""
    name = name.lower().strip()
    name = name.replace(" ", "-")
    name = re.sub(r"-+", "-", name)
    name = re.sub(r"[^a-z0-9-]", "", name)
    return name.strip("-")


def pick_text_channel_for_voice(
    voice_channel: discord.VoiceChannel,
) -> Optional[discord.TextChannel]:
    """
    Pick a text channel for a voice channel:
    1) Same category + same (normalized) name
    2) Same category + first text channel
    3) System channel, else first guild text channel
    """
    category = voice_channel.category
    if category is not None:
        voice_key = normalize_channel_name(voice_channel.name)
        voice_lower = voice_channel.name.lower()

        for text_ch in category.text_channels:
            if text_ch.name == voice_lower or text_ch.name == voice_key:
                return text_ch

        if category.text_channels:
            return category.text_channels[0]

    guild = voice_channel.guild
    if guild.system_channel is not None:
        return guild.system_channel

    if guild.text_channels:
        return guild.text_channels[0]

    return None


async def pick_saved_or_linked_text_channel(
    guild: discord.Guild,
    event_id: int,
    voice_channel: discord.VoiceChannel,
    start_time: Optional[datetime],
) -> Optional[discord.abc.Messageable]:
    """Prefer the saved event target channel, then fall back to voice-linked lookup."""
    target_keys = [make_event_target_key(event_id, start_time), str(event_id)]
    for target_key in target_keys:
        saved_channel_id = event_text_channel_targets.get(target_key)
        if saved_channel_id is None:
            continue

        saved_channel = await fetch_message_channel(guild, saved_channel_id)
        if saved_channel is not None:
            print(
                f"Using saved shuffle target channel {saved_channel_id} "
                f"for event key {target_key}"
            )
            return saved_channel

        print(
            f"Saved text channel {saved_channel_id} for event key {target_key} "
            "is no longer available"
        )
        event_text_channel_targets.pop(target_key, None)
        save_event_text_channel_targets()

    fallback_channel = pick_text_channel_for_voice(voice_channel)
    if fallback_channel is not None:
        print(
            f"No saved shuffle target for event {event_id}; "
            f"falling back to linked text channel {fallback_channel.id}"
        )
    return fallback_channel


def event_occurrence_key(event_id: int, start_time: Optional[datetime]) -> tuple[int, int]:
    start_ts = int(start_time.timestamp()) if start_time else 0
    return (event_id, start_ts)


def build_planned_shuffle_content(
    event_name: str,
    voice_channel: discord.VoiceChannel,
    start_time: datetime,
) -> str:
    start_ts = int(start_time.timestamp())
    return (
        f"{PLANNED_SHUFFLE_PREFIX} **{event_name}**\n"
        f"Voice channel: {voice_channel.mention}\n"
        f"Starts: <t:{start_ts}:R> (<t:{start_ts}:t>)\n"
        f"This message will be updated with the shuffled list when the event starts."
    )


async def find_reusable_event_message(
    text_channel,
    event: discord.ScheduledEvent,
) -> tuple[Optional[discord.Message], Optional[discord.Message]]:
    """Find recent shuffle or planned messages for this event window."""
    shuffle_message = None
    planned_message = None

    key = event_occurrence_key(event.id, event.start_time)
    stored_message = planned_event_messages.get(key)
    if stored_message is not None:
        stored_channel_id, message_id = stored_message
        if stored_channel_id == text_channel.id:
            try:
                planned_message = await text_channel.fetch_message(message_id)
            except discord.NotFound:
                planned_event_messages.pop(key, None)
            except Exception as e:
                print(f"Could not fetch planned message for event {event.id}: {e}")

    after = (
        event.start_time - timedelta(minutes=2)
        if event.start_time is not None
        else None
    )

    try:
        async for message in text_channel.history(limit=None, after=after):
            if bot.user is None or message.author.id != bot.user.id:
                continue
            if message.content.startswith(SHUFFLE_LIST_PREFIX):
                shuffle_message = message
                break
            if (
                planned_message is None
                and message.content.startswith(PLANNED_SHUFFLE_PREFIX)
            ):
                planned_message = message
    except Exception as e:
        print(f"Could not inspect recent shuffle history for event {event.id}: {e}")

    return shuffle_message, planned_message


async def send_planned_shuffle_notice(
    event: discord.ScheduledEvent,
    text_channel,
    voice_channel: discord.VoiceChannel,
) -> Optional[discord.Message]:
    """Send or reuse the one-minute warning message for an event."""
    if is_frozen_event(event):
        return None

    key = event_occurrence_key(event.id, event.start_time)
    _, planned_message = await find_reusable_event_message(text_channel, event)
    if planned_message is not None:
        try:
            await planned_message.edit(
                content=build_planned_shuffle_content(event.name, voice_channel, event.start_time)
            )
        except discord.NotFound:
            planned_message = None
        except Exception as e:
            print(f"Could not refresh planned message for event {event.id}: {e}")

    if planned_message is not None:
        planned_event_messages[key] = (text_channel.id, planned_message.id)
        return planned_message

    try:
        message = await text_channel.send(
            build_planned_shuffle_content(event.name, voice_channel, event.start_time)
        )
    except Exception as e:
        print(f"Could not send planned shuffle notice for event {event.id}: {e}")
        return None

    planned_event_messages[key] = (text_channel.id, message.id)
    return message


def schedule_event_lifecycle(
    guild_id: int,
    event_id: int,
    start_time: datetime,
    end_time: datetime,
    text_channel_id: int,
    *,
    replace_existing: bool = False,
) -> None:
    """Schedule notice/start/end handling for one event occurrence."""
    key = event_occurrence_key(event_id, start_time)
    existing_task = scheduled_event_tasks.get(key)
    if existing_task is not None and not existing_task.done():
        if not replace_existing:
            return
        existing_task.cancel()

    async def run_event_lifecycle():
        planned_message = None
        try:
            await bot.wait_until_ready()

            notice_time = start_time - timedelta(minutes=1)
            now = datetime.now(timezone.utc)
            if notice_time > now:
                await discord.utils.sleep_until(notice_time)

            guild_obj = bot.get_guild(guild_id)
            if guild_obj is None:
                return

            text_channel = await fetch_message_channel(guild_obj, text_channel_id)
            if text_channel is None:
                return

            try:
                ev = await guild_obj.fetch_scheduled_event(event_id)
            except discord.NotFound:
                return
            except Exception as e:
                print(f"Error fetching scheduled event (notice): {e}")
                ev = None

            if (
                ev is not None
                and ev.start_time is not None
                and ev.status is discord.EventStatus.scheduled
                and ev.channel_id is not None
            ):
                voice_channel = guild_obj.get_channel(ev.channel_id)
                if isinstance(voice_channel, discord.VoiceChannel):
                    planned_message = await send_planned_shuffle_notice(ev, text_channel, voice_channel)

            if start_time > datetime.now(timezone.utc):
                await discord.utils.sleep_until(start_time)

            guild_obj = bot.get_guild(guild_id)
            if guild_obj is None:
                return

            try:
                ev = await guild_obj.fetch_scheduled_event(event_id)
            except discord.NotFound:
                return
            except Exception as e:
                print(f"Error fetching scheduled event (start): {e}")
                ev = None

            if ev is not None:
                try:
                    if ev.status is discord.EventStatus.scheduled:
                        await ev.edit(status=discord.EventStatus.active)
                        print(f"Auto-activated event {ev.name} ({ev.id})")
                except Exception as e:
                    print(f"Error auto-activating event: {e}")

            try:
                await trigger_shuffle_for_event(
                    ev or discord.Object(id=event_id),
                    allow_stale_active=True,
                    preferred_text_channel=text_channel,
                    preferred_message=planned_message,
                )
            except Exception as e:
                print(f"Error running auto-shuffle: {e}")

            if end_time > datetime.now(timezone.utc):
                await discord.utils.sleep_until(end_time)

            guild_obj = bot.get_guild(guild_id)
            if guild_obj is None:
                return

            try:
                ev = await guild_obj.fetch_scheduled_event(event_id)
            except discord.NotFound:
                return
            except Exception as e:
                print(f"Error fetching scheduled event (end): {e}")
                return

            try:
                if ev.status is not discord.EventStatus.completed:
                    await ev.edit(status=discord.EventStatus.completed)
                    print(f"Auto-completed event {ev.name} ({ev.id})")
            except Exception as e:
                print(f"Error auto-completing event: {e}")
        finally:
            if scheduled_event_tasks.get(key) is asyncio.current_task():
                scheduled_event_tasks.pop(key, None)
                forget_event_text_channel(event_id, start_time)

    task = bot.loop.create_task(run_event_lifecycle())
    scheduled_event_tasks[key] = task


async def trigger_shuffle_for_event(
    event: discord.ScheduledEvent,
    *,
    allow_stale_active: bool = False,
    preferred_text_channel=None,
    preferred_message: Optional[discord.Message] = None,
) -> None:
    """Run shuffle once per event occurrence (event_id + start_time)."""
    if not isinstance(event, discord.ScheduledEvent):
        return

    if is_frozen_event(event):
        return

    if event.entity_type != discord.EntityType.voice:
        return

    if event.channel_id is None:
        return

    key = event_occurrence_key(event.id, event.start_time)
    if key in triggered_event_occurrences or key in triggering_event_occurrences:
        return
    triggering_event_occurrences.add(key)

    try:
        # On reconnect/restart, Discord may surface an already-active event again.
        # If this occurrence started before the current process came up, do not
        # replay its shuffle.
        if (
            not allow_stale_active
            and
            event.status is discord.EventStatus.active
            and event.start_time is not None
            and event.start_time <= (PROCESS_STARTED_AT + STARTUP_CLOCK_SKEW)
        ):
            triggered_event_occurrences.add(key)
            print(f"Skipping stale active event {event.id} on startup/reconnect")
            return

        guild = event.guild
        voice_channel = guild.get_channel(event.channel_id)
        if not isinstance(voice_channel, discord.VoiceChannel):
            print(f"Event {event.id} has no valid voice channel")
            return

        text_channel = preferred_text_channel
        if text_channel is None:
            text_channel = await pick_saved_or_linked_text_channel(
                guild,
                event.id,
                voice_channel,
                event.start_time,
            )
        if text_channel is None:
            print(f"No text channel found for voice channel {voice_channel.id}")
            return

        shuffle_message, planned_message = await find_reusable_event_message(text_channel, event)
        if shuffle_message is not None:
            triggered_event_occurrences.add(key)
            print(f"Skipping duplicate shuffle for event {event.id}: message already exists")
            return

        triggered_event_occurrences.add(key)
        planned_message = preferred_message or planned_message
        await start_shuffle_for_channel(
            guild,
            voice_channel,
            text_channel,
            existing_message=planned_message,
        )
    finally:
        triggering_event_occurrences.discard(key)


def build_content(
    guild: discord.Guild,
    order,
    labels=None,
    excluded_member_ids: Optional[set[int]] = None,
    allow_hot_joiners: bool = True,
    require_camera: bool = False,
) -> str:
    """Build the visual text of the shuffled list, using custom labels if present."""
    if labels is None:
        labels = {}
    if excluded_member_ids is None:
        excluded_member_ids = set()

    lines = [SHUFFLE_LIST_PREFIX]
    lines.append(f"Hot joiners: {'on' if allow_hot_joiners else 'off'}")
    if require_camera:
        lines.append("Camera check: on")
    for i, user_id in enumerate(order, 1):
        member = guild.get_member(user_id)
        base = member.display_name if member else f"Unknown user ({user_id})"
        shown = labels.get(user_id, base)
        if i == 1:
            if member is not None:
                if shown == member.display_name:
                    shown = member.mention
                elif shown.startswith(member.display_name):
                    suffix = shown[len(member.display_name):]
                    shown = f"{member.mention}{suffix}"
                else:
                    shown = f"{member.mention} ({shown})"
            else:
                shown = f"@{shown}"
        lines.append(f"{i}. {shown}")

    if excluded_member_ids:
        excluded_names = []
        for user_id in sorted(excluded_member_ids):
            member = guild.get_member(user_id)
            excluded_names.append(member.display_name if member else f"Unknown user ({user_id})")
        lines.append("")
        lines.append(f"Excluded: {', '.join(excluded_names)}")

    return "\n".join(lines)


def build_shuffle_member_label(
    member: discord.Member,
    *,
    hot_joiner: bool = False,
    returned: bool = False,
) -> str:
    """Build a shuffle label with stable status markers."""
    markers = []
    if hot_joiner:
        markers.append("✨")
    if returned:
        markers.append("🌌")
    if not markers:
        return member.display_name
    return f"{member.display_name} {' '.join(markers)}"


def is_shuffle_hot_joiner(state: dict, member_id: int) -> bool:
    """Return whether a member joined after this shuffle was posted."""
    if member_id in state.get("hot_joiner_member_ids", set()):
        return True
    return "✨" in state.get("labels", {}).get(member_id, "")


async def update_shuffle_message(state: dict) -> None:
    """Redraw the message with the current shuffle order."""
    guild = bot.get_guild(state["guild_id"])
    if not guild:
        return

    content = build_content(
        guild,
        state["order"],
        state.get("labels"),
        state.get("ignored_excluded_member_ids", set()),
        state.get("allow_hot_joiners", True),
        state.get("require_camera", False),
    )
    try:
        await state["message"].edit(content=content)
    except discord.NotFound:
        # message deleted -> clean up state
        active_shuffles.pop(state["text_channel_id"], None)
    except Exception as e:
        print(f"Error editing message: {e}")


async def refresh_active_shuffle_exclusions(guild_id: int) -> None:
    """Recompute effective exclusions for active shuffles in one guild."""
    persistent_ids = get_persistent_excluded_member_ids(guild_id)

    for state in list(active_shuffles.values()):
        if state["guild_id"] != guild_id:
            continue

        manual_ids = set(state.get("manual_excluded_member_ids", set()))
        effective_ids = persistent_ids.union(manual_ids)
        changed = effective_ids != state.get("excluded_member_ids", set())
        state["excluded_member_ids"] = effective_ids
        ignored_ids = set(state.get("ignored_excluded_member_ids", set()))
        ignored_ids.intersection_update(effective_ids)

        removed_any = False
        for user_id in list(state["order"]):
            if user_id not in effective_ids:
                continue
            state["order"].remove(user_id)
            state.get("labels", {}).pop(user_id, None)
            pending = state["pending_removals"].pop(user_id, None)
            if pending:
                pending.cancel()
            ignored_ids.add(user_id)
            removed_any = True

        guild = bot.get_guild(state["guild_id"])
        voice_channel = guild.get_channel(state["voice_channel_id"]) if guild else None
        if voice_channel is not None:
            ignored_ids.update(
                member.id
                for member in voice_channel.members
                if not member.bot and member.id in effective_ids
            )

        ignored_changed = ignored_ids != state.get("ignored_excluded_member_ids", set())
        state["ignored_excluded_member_ids"] = ignored_ids

        if changed or removed_any or ignored_changed:
            await update_shuffle_message(state)


async def refresh_active_shuffle_settings(guild_id: int) -> None:
    """Push guild-level shuffle settings into active shuffles for one guild."""
    allow_hot_joiners = get_allow_hot_joiners(guild_id)

    for state in list(active_shuffles.values()):
        if state["guild_id"] != guild_id:
            continue

        if state.get("allow_hot_joiners", True) == allow_hot_joiners:
            continue

        state["allow_hot_joiners"] = allow_hot_joiners
        await update_shuffle_message(state)


def get_shuffle_camera_source(text_channel_id: int) -> str:
    """Return the source token used by a shuffle message's camera enforcement."""
    return f"{CAMERA_SOURCE_SHUFFLE_PREFIX}{text_channel_id}"


def is_camera_on(voice_state) -> bool:
    """Return whether Discord reports the member's camera as enabled."""
    return bool(getattr(voice_state, "self_video", False))


def cancel_camera_kick(state: dict, member_id: int) -> bool:
    """Cancel a pending camera kick timer for one member."""
    pending = state.get("pending_kicks", {}).pop(member_id, None)
    if pending:
        pending.cancel()
        return True
    return False


def start_camera_enforcement(
    guild: discord.Guild,
    voice_channel: discord.abc.GuildChannel,
    text_channel: Optional[discord.abc.Messageable] = None,
    *,
    source: str = CAMERA_SOURCE_MANUAL,
) -> tuple[dict, bool]:
    """Start or extend camera enforcement for one voice channel."""
    key = (guild.id, voice_channel.id)
    state = active_camera_enforcements.get(key)
    created = state is None
    if state is None:
        state = {
            "guild_id": guild.id,
            "voice_channel_id": voice_channel.id,
            "text_channel_id": getattr(text_channel, "id", None),
            "sources": set(),
            "pending_kicks": {},
        }
        active_camera_enforcements[key] = state
    elif text_channel is not None and state.get("text_channel_id") is None:
        state["text_channel_id"] = getattr(text_channel, "id", None)

    state["sources"].add(source)

    for voice_member in getattr(voice_channel, "members", []):
        update_camera_enforcement_for_member(voice_member, voice_channel.id)

    return state, created


def stop_camera_enforcement(
    guild_id: int,
    voice_channel_id: int,
    *,
    source: Optional[str] = None,
) -> bool:
    """Stop camera enforcement, either entirely or for one source token."""
    key = (guild_id, voice_channel_id)
    state = active_camera_enforcements.get(key)
    if state is None:
        return False

    if source is None:
        state["sources"].clear()
    else:
        state["sources"].discard(source)

    if state["sources"]:
        return False

    active_camera_enforcements.pop(key, None)
    for task in list(state.get("pending_kicks", {}).values()):
        task.cancel()
    state.get("pending_kicks", {}).clear()
    return True


def update_camera_enforcement_for_member(
    member: discord.Member,
    voice_channel_id: int,
) -> None:
    """Schedule or cancel camera enforcement for a member in one voice channel."""
    if member.bot:
        return

    state = active_camera_enforcements.get((member.guild.id, voice_channel_id))
    if state is None:
        return

    member_voice = member.voice
    in_enforced_channel = (
        member_voice is not None
        and member_voice.channel is not None
        and member_voice.channel.id == voice_channel_id
    )

    if not in_enforced_channel or is_camera_on(member_voice):
        cancel_camera_kick(state, member.id)
        return

    pending = state["pending_kicks"].get(member.id)
    if pending and not pending.done():
        return

    state["pending_kicks"][member.id] = asyncio.create_task(
        camera_kick_after_grace(member.guild.id, voice_channel_id, member.id)
    )


def handle_camera_voice_state_update(member: discord.Member, before, after) -> None:
    """Apply camera enforcement to any enforced channel touched by this update."""
    affected_channel_ids: set[int] = set()
    before_channel = before.channel if is_trackable_voice_channel(before.channel) else None
    after_channel = after.channel if is_trackable_voice_channel(after.channel) else None

    if before_channel is not None:
        affected_channel_ids.add(before_channel.id)
    if after_channel is not None:
        affected_channel_ids.add(after_channel.id)

    for voice_channel_id in affected_channel_ids:
        update_camera_enforcement_for_member(member, voice_channel_id)


async def send_camera_enforcement_notice(state: dict, message: str) -> None:
    """Best-effort notification for camera enforcement actions."""
    text_channel_id = state.get("text_channel_id")
    if text_channel_id is None:
        return

    channel = bot.get_channel(text_channel_id)
    if channel is None or not hasattr(channel, "send"):
        return

    try:
        await channel.send(message)
    except Exception as e:
        print(f"Failed to send camera enforcement notice: {e}")


async def camera_kick_after_grace(
    guild_id: int,
    voice_channel_id: int,
    member_id: int,
) -> None:
    """Disconnect a member if their camera remains off for the full grace period."""
    try:
        await asyncio.sleep(CAMERA_GRACE_SECONDS)
    except asyncio.CancelledError:
        return

    state = active_camera_enforcements.get((guild_id, voice_channel_id))
    if state is None:
        return

    try:
        guild = bot.get_guild(guild_id)
        member = guild.get_member(member_id) if guild else None
        if member is None or member.bot:
            return

        member_voice = member.voice
        if (
            member_voice is None
            or member_voice.channel is None
            or member_voice.channel.id != voice_channel_id
            or is_camera_on(member_voice)
        ):
            return

        channel_name = member_voice.channel.name
        await member.move_to(
            None,
            reason=f"Camera was off for more than {CAMERA_GRACE_SECONDS} seconds",
        )
        await send_camera_enforcement_notice(
            state,
            f"Disconnected **{member.display_name}** from `{channel_name}` "
            f"because their camera stayed off for {CAMERA_GRACE_SECONDS} seconds.",
        )
    except discord.Forbidden:
        await send_camera_enforcement_notice(
            state,
            "I could not disconnect a member for camera enforcement. "
            "I need the `Move Members` permission and a high enough role.",
        )
    except Exception as e:
        print(f"Error during camera enforcement kick: {e}")
    finally:
        current_state = active_camera_enforcements.get((guild_id, voice_channel_id))
        if current_state is not None:
            current_state.get("pending_kicks", {}).pop(member_id, None)


async def refresh_active_shuffle_camera_enforcement(guild_id: int) -> None:
    """Start or stop camera enforcement for all active shuffles in one guild."""
    require_camera = get_require_camera_for_shuffles(guild_id)
    guild = bot.get_guild(guild_id)

    for state in list(active_shuffles.values()):
        if state["guild_id"] != guild_id:
            continue

        source = get_shuffle_camera_source(state["text_channel_id"])
        if guild is not None:
            voice_channel = guild.get_channel(state["voice_channel_id"])
        else:
            voice_channel = None

        if require_camera and voice_channel is not None:
            start_camera_enforcement(
                guild,
                voice_channel,
                getattr(state.get("message"), "channel", None),
                source=source,
            )
        else:
            stop_camera_enforcement(
                state["guild_id"],
                state["voice_channel_id"],
                source=source,
            )

        if state.get("require_camera", False) != require_camera:
            state["require_camera"] = require_camera
            await update_shuffle_message(state)


def format_camera_enforcement_sources(sources: set[str]) -> str:
    """Render camera enforcement source tokens for status output."""
    labels = []
    if CAMERA_SOURCE_MANUAL in sources:
        labels.append("manual")
    if any(source.startswith(CAMERA_SOURCE_SHUFFLE_PREFIX) for source in sources):
        labels.append("shuffle")
    return ", ".join(labels) if labels else "unknown"


def bot_can_move_members(guild: discord.Guild, voice_channel: discord.abc.GuildChannel) -> bool:
    """Return whether the bot can disconnect members from a voice channel."""
    bot_member = guild.me
    if bot_member is None and bot.user is not None:
        bot_member = guild.get_member(bot.user.id)
    if bot_member is None or not hasattr(voice_channel, "permissions_for"):
        return False
    return bool(voice_channel.permissions_for(bot_member).move_members)


async def schedule_removal(text_channel_id: int, member_id: int) -> None:
    """
    Wait REMOVE_DELAY seconds and then decide whether to remove the member
    from the list based on the formula pos * 2 > elapsed_minutes.
    """
    try:
        await asyncio.sleep(REMOVE_DELAY)
    except asyncio.CancelledError:
        # the member returned earlier, task cancelled
        return

    state = active_shuffles.get(text_channel_id)
    if not state:
        return

    guild = bot.get_guild(state["guild_id"])
    voice_channel = guild.get_channel(state["voice_channel_id"]) if guild else None

    # If they are already back in the voice channel, do nothing
    if voice_channel and any(m.id == member_id for m in voice_channel.members):
        state["pending_removals"].pop(member_id, None)
        return

    # If they are not in the order anymore, someone else already updated the list
    if member_id not in state["order"]:
        state["pending_removals"].pop(member_id, None)
        return

    # === Logic: remove only those for whom position * 2 > elapsed_minutes ===
    elapsed = datetime.now() - state["created_at"]
    elapsed_minutes = elapsed.total_seconds() / 60.0

    pos = state["order"].index(member_id) + 1  # 1-based position

    if pos * 2 > elapsed_minutes:
        # condition satisfied -> remove
        state["order"].remove(member_id)
        state["pending_removals"].pop(member_id, None)
        await update_shuffle_message(state)
    else:
        # condition not satisfied -> keep in list
        state["pending_removals"].pop(member_id, None)


async def start_shuffle_for_channel(
    guild: discord.Guild,
    voice_channel: discord.VoiceChannel,
    text_channel: discord.abc.Messageable,
    *,
    existing_message: Optional[discord.Message] = None,
    excluded_member_ids: Optional[set[int]] = None,
) -> None:
    """
    Common shuffle logic:
    - used in the /shuffle command
    - and when a scheduled event auto-starts.
    """
    manual_excluded_member_ids = set(excluded_member_ids or set())
    excluded_member_ids = get_persistent_excluded_member_ids(guild.id).union(
        manual_excluded_member_ids
    )
    ignored_excluded_member_ids = {
        m.id for m in voice_channel.members
        if not m.bot and m.id in excluded_member_ids
    }
    allow_hot_joiners = get_allow_hot_joiners(guild.id)
    require_camera = get_require_camera_for_shuffles(guild.id)
    members = [
        m for m in voice_channel.members
        if not m.bot and m.id not in excluded_member_ids
    ]

    if not members:
        empty_message = (
            "No eligible members left in the voice channel after exclusions!"
            if ignored_excluded_member_ids else
            "No members in the voice channel!"
        )
        if existing_message is not None:
            try:
                await existing_message.edit(content=empty_message)
            except discord.NotFound:
                await text_channel.send(empty_message)
        else:
            await text_channel.send(empty_message)
        return

    order = [m.id for m in members]
    random.shuffle(order)

    labels: dict[int, str] = {}  # labels for ✨ / 🌌

    content = build_content(
        guild,
        order,
        labels,
        ignored_excluded_member_ids,
        allow_hot_joiners,
        require_camera,
    )
    if existing_message is not None:
        try:
            await existing_message.edit(content=content)
            msg = existing_message
        except discord.NotFound:
            msg = await text_channel.send(content)
    else:
        msg = await text_channel.send(content)

    active_shuffles[text_channel.id] = {
        "created_at": datetime.now(),
        "guild_id": guild.id,
        "voice_channel_id": voice_channel.id,
        "text_channel_id": text_channel.id,
        "order": order,                # current order of ids
        "message": msg,                # message to edit
        "pending_removals": {},        # member_id -> asyncio.Task
        "labels": labels,              # custom labels (with emojis)
        "ever_seen": set(order),       # who has ever been in the list
        "hot_joiner_member_ids": set(),
        "manual_excluded_member_ids": manual_excluded_member_ids,
        "excluded_member_ids": excluded_member_ids,
        "ignored_excluded_member_ids": ignored_excluded_member_ids,
        "allow_hot_joiners": allow_hot_joiners,
        "require_camera": require_camera,
    }

    if require_camera:
        start_camera_enforcement(
            guild,
            voice_channel,
            text_channel,
            source=get_shuffle_camera_source(text_channel.id),
        )

    # Keep the shuffle live long enough for late joiners to be added.
    asyncio.create_task(cleanup_old_shuffle(text_channel.id, SHUFFLE_TRACKING_WINDOW))


async def cleanup_old_shuffle(channel_id: int, delay_sec: int) -> None:
    """Cleanup shuffle state after a delay and cancel all pending removal timers."""
    await asyncio.sleep(delay_sec)
    state = active_shuffles.pop(channel_id, None)
    if state:
        for task in state["pending_removals"].values():
            task.cancel()
        stop_camera_enforcement(
            state["guild_id"],
            state["voice_channel_id"],
            source=get_shuffle_camera_source(channel_id),
        )


def sdg_is_core_member(member: discord.Member) -> bool:
    """Treat the Discord role `core` as the SDG priority-partner marker."""
    return member_has_configured_role(
        member,
        role_id=SDG_CORE_ROLE_ID,
        role_name=SDG_CORE_ROLE_NAME,
    )


def sdg_is_newcomer_member(member: discord.Member) -> bool:
    """Treat the Discord role `нашедшийся` as the SDG newcomer marker."""
    if sdg_is_core_member(member):
        return False
    return member_has_configured_role(
        member,
        role_id=SDG_NEWCOMER_ROLE_ID,
        role_name=SDG_NEWCOMER_ROLE_NAME,
    )


def sdg_get_member_join_days(
    member: discord.Member,
    *,
    reference_time: Optional[datetime] = None,
) -> Optional[int]:
    """Return how many full days the member has been in the guild."""
    if member.joined_at is None:
        return None

    reference = (reference_time or utc_now()).astimezone(timezone.utc)
    joined_at = member.joined_at.astimezone(timezone.utc)
    if joined_at > reference:
        return 0

    return int((reference - joined_at).total_seconds() // 86400)


def sdg_build_member_profile(
    member: discord.Member,
    *,
    reference_time: Optional[datetime] = None,
) -> dict:
    """Classify one member into the SDG pairing buckets used by the round planner."""
    joined_days = sdg_get_member_join_days(member, reference_time=reference_time)
    is_core = sdg_is_core_member(member)
    is_newcomer = sdg_is_newcomer_member(member)
    is_fresh_newcomer = (
        is_newcomer
        and joined_days is not None
        and joined_days <= SDG_NEWCOMER_DAYS_THRESHOLD
    )
    is_seasoned_newcomer = is_newcomer and not is_fresh_newcomer
    is_trusted = not is_core and not is_newcomer and has_trusted_role(member)
    is_reliable = (
        not is_core
        and not is_newcomer
        and not is_trusted
        and has_reliable_role(member)
    )

    if is_core:
        category = "c"
    elif is_fresh_newcomer:
        category = "fo"
    elif is_seasoned_newcomer:
        category = "sn"
    elif is_trusted:
        category = "t"
    elif is_reliable:
        category = "a"
    else:
        category = "r"

    return {
        "category": category,
        "joined_days": joined_days,
        "is_core": is_core,
        "is_newcomer": is_newcomer,
        "is_fresh_newcomer": is_fresh_newcomer,
        "is_seasoned_newcomer": is_seasoned_newcomer,
        "gets_extended_mentor_slot": (
            is_newcomer
            and joined_days is not None
            and joined_days <= SDG_NEWCOMER_DAYS_THRESHOLD
        ),
        "is_trusted": is_trusted,
        "is_reliable": is_reliable,
        "is_priority_partner": category in {"c", "t", "a"},
    }


def sdg_target_core_rounds_after_next_slot(completed_slots: int) -> int:
    """Keep fresh newcomers with `core` in roughly every other round slot."""
    return (completed_slots + 2) // 2


def sdg_member_needs_core_this_round(session: dict, member_id: int) -> bool:
    """Return whether a fresh newcomer is currently below the 50% core target."""
    rounds_seen = session.get("fresh_newcomer_round_counts", {}).get(member_id, 0)
    core_rounds = session.get("fresh_newcomer_core_round_counts", {}).get(member_id, 0)
    return core_rounds < sdg_target_core_rounds_after_next_slot(rounds_seen)


def sdg_get_planning_category(session: dict, member_id: int, profile: dict) -> str:
    """Return the dynamic category used by the current round planner."""
    if profile["is_fresh_newcomer"]:
        return "fd" if sdg_member_needs_core_this_round(session, member_id) else "fo"
    return profile["category"]


def sdg_visible_name(name: str) -> str:
    """Strip legacy star suffixes for stable sorting if old nicknames still remain."""
    if name.endswith("**"):
        return name[:-2]
    if name.endswith("*"):
        return name[:-1]
    return name


def sdg_sort_member_ids(member_ids: list[int], display_names_by_id: dict[int, str]) -> list[int]:
    """Sort member IDs by their display names without SDG marker suffixes."""
    return sorted(
        member_ids,
        key=lambda member_id: sdg_visible_name(
            display_names_by_id.get(member_id, f"user-{member_id}")
        ).lower(),
    )


def sdg_can_apply_template(counts: dict[str, int], template: dict) -> bool:
    """Check whether one template can be applied to the remaining participant buckets."""
    return all(counts[key] >= needed for key, needed in template["counts"].items())


def sdg_newcomer_partner_penalty(category: str, template_counts: dict[str, int]) -> int:
    """Score partner quality for one newcomer category inside a group."""
    if category == "fd":
        if template_counts["c"] > 0:
            return 0
        if template_counts["t"] > 0:
            return 4
        if template_counts["a"] > 0:
            return 6
        if template_counts["r"] > 0:
            return 8
        return 14

    if category == "fo":
        if template_counts["t"] > 0:
            return 0
        if template_counts["a"] > 0:
            return 1
        if template_counts["c"] > 0:
            return 2
        if template_counts["r"] > 0:
            return 3
        return 10

    if template_counts["c"] > 0:
        return 0
    if template_counts["t"] > 0:
        return 1
    if template_counts["a"] > 0:
        return 2
    if template_counts["r"] > 0:
        return 3
    return 10


def sdg_score_group_template(template_counts: dict[str, int]) -> tuple[int, int, int, int, int]:
    """Score one pair/trio template so pairs and strong newcomer partners win first."""
    group_size = sum(template_counts.values())
    newcomer_count = template_counts["fd"] + template_counts["fo"] + template_counts["sn"]
    trio_penalty = 1 if group_size == 3 else 0

    if newcomer_count == 0:
        stacked_priority_roles = max(
            0,
            template_counts["c"] + template_counts["t"] + template_counts["a"] - 1,
        )
        return (trio_penalty, 0, 0, 0, stacked_priority_roles)

    overlap_penalty = 3 * max(0, newcomer_count - 1)
    trio_offset = 4 if group_size == 3 else 0
    stacked_priority_roles = max(
        0,
        template_counts["c"] + template_counts["t"] + template_counts["a"] - newcomer_count,
    )
    due_fresh_penalty = (
        sdg_newcomer_partner_penalty("fd", template_counts) + overlap_penalty + trio_offset
    )
    optional_fresh_penalty = (
        sdg_newcomer_partner_penalty("fo", template_counts) + overlap_penalty + trio_offset
    )
    seasoned_newcomer_penalty = (
        sdg_newcomer_partner_penalty("sn", template_counts) + overlap_penalty + trio_offset
    )
    return (
        trio_penalty,
        template_counts["fd"] * due_fresh_penalty,
        template_counts["fo"] * optional_fresh_penalty,
        template_counts["sn"] * seasoned_newcomer_penalty,
        stacked_priority_roles,
    )


def sdg_build_group_templates() -> list[dict]:
    """Enumerate every possible pair/trio template over the SDG role buckets."""
    templates = []
    for group_size in (2, 3):
        for combination in combinations_with_replacement(SDG_CATEGORY_KEYS, group_size):
            counts = {key: 0 for key in SDG_CATEGORY_KEYS}
            for key in combination:
                counts[key] += 1
            templates.append(
                {
                    "counts": counts,
                    "score": sdg_score_group_template(counts),
                }
            )

    return sorted(
        templates,
        key=lambda template: (
            template["score"],
            sum(template["counts"].values()),
            tuple(template["counts"][key] for key in SDG_CATEGORY_KEYS),
        ),
    )


SDG_GROUP_TEMPLATES = sdg_build_group_templates()


def sdg_subtract_counts(
    counts: dict[str, int],
    template: dict,
) -> tuple[int, int, int, int, int, int]:
    """Subtract a template from the remaining SDG category counts."""
    next_counts = dict(counts)
    for key, needed in template["counts"].items():
        next_counts[key] -= needed
    return tuple(next_counts[key] for key in SDG_CATEGORY_KEYS)


@lru_cache(maxsize=None)
def sdg_find_group_plan(
    due_fresh_newcomers_count: int,
    optional_fresh_newcomers_count: int,
    seasoned_newcomers_count: int,
    core_count: int,
    trusted_count: int,
    reliable_count: int,
    regulars_count: int,
):
    """Find the best group template sequence for one round."""
    counts = dict(
        zip(
            SDG_CATEGORY_KEYS,
            (
                due_fresh_newcomers_count,
                optional_fresh_newcomers_count,
                seasoned_newcomers_count,
                core_count,
                trusted_count,
                reliable_count,
                regulars_count,
            ),
        )
    )

    if all(value == 0 for value in counts.values()):
        return (0, 0, 0, 0, 0), []

    best_score = None
    best_plan = None

    for template in SDG_GROUP_TEMPLATES:
        if not sdg_can_apply_template(counts, template):
            continue

        next_state = sdg_subtract_counts(counts, template)
        child = sdg_find_group_plan(*next_state)
        if child is None:
            continue

        child_score, child_plan = child
        current_score = tuple(
            template["score"][index] + child_score[index]
            for index in range(len(template["score"]))
        )
        current_plan = [template] + child_plan

        if best_score is None or current_score < best_score:
            best_score = current_score
            best_plan = current_plan

    if best_score is None:
        return None

    return best_score, best_plan


def sdg_make_groups(
    member_ids: list[int],
    member_profiles_by_id: dict[int, dict],
) -> list[list[int]]:
    """Build SDG pairs/trios for the active members of one round."""
    pools = {key: [] for key in SDG_CATEGORY_KEYS}
    for member_id in member_ids:
        category = member_profiles_by_id[member_id]["planning_category"]
        pools[category].append(member_id)

    for pool in pools.values():
        random.shuffle(pool)

    plan_result = sdg_find_group_plan(
        *(len(pools[key]) for key in SDG_CATEGORY_KEYS)
    )
    if plan_result is None:
        raise ValueError("Could not build groups for the current participant set.")

    _, plan = plan_result
    groups: list[list[int]] = []

    def pop_from_pool(pool_key: str) -> int:
        pool = pools[pool_key]
        if not pool:
            raise ValueError("The SDG plan could not be fulfilled for this round.")
        return pool.pop()

    for template in plan:
        group: list[int] = []
        for pool_key in SDG_CATEGORY_KEYS:
            amount = template["counts"][pool_key]
            for _ in range(amount):
                group.append(pop_from_pool(pool_key))

        random.shuffle(group)
        groups.append(group)

    random.shuffle(groups)
    return groups


def sdg_assign_voices(groups: list[list[int]], reserved_voices: set[int]) -> list[dict]:
    """Assign logical voice numbers, skipping rooms held by 10-minute groups."""
    assigned_groups = []
    next_voice = 1

    for group in groups:
        while next_voice in reserved_voices:
            next_voice += 1

        assigned_groups.append({"member_ids": group, "voice": next_voice})
        next_voice += 1

    return assigned_groups


def sdg_get_next_round_paused(
    assigned_groups: list[dict],
    member_profiles_by_id: dict[int, dict],
) -> tuple[set[int], set[int]]:
    """Pause eligible newcomer groups with higher-role partners for one extra slot."""
    paused_member_ids: set[int] = set()
    paused_voices: set[int] = set()

    for group in assigned_groups:
        if sdg_should_hold_group(group["member_ids"], member_profiles_by_id):
            paused_member_ids.update(group["member_ids"])
            paused_voices.add(group["voice"])

    return paused_member_ids, paused_voices


def get_sdg_shuffle_access_label() -> str:
    """Human-friendly permission label for timed room shuffles."""
    return f"{get_shuffle_exclusion_access_label()} or `Move Members` permission"


def has_sdg_shuffle_access(member: discord.Member) -> bool:
    """Allow timed room shuffles for trusted roles or members who can move others."""
    return has_shuffle_exclusion_access(member) or member.guild_permissions.move_members


def sdg_extract_trailing_number(value: str) -> Optional[int]:
    """Extract the last numeric suffix from a channel name, if present."""
    match = re.search(r"(\d+)(?!.*\d)", value)
    if match is None:
        return None
    return int(match.group(1))


def sdg_voice_channel_sort_key(channel: discord.VoiceChannel) -> tuple[int, int, int, int]:
    """Prefer numbered breakout rooms, then fall back to channel position."""
    trailing_number = sdg_extract_trailing_number(channel.name)
    if trailing_number is None:
        return (1, 0, channel.position, channel.id)
    return (0, trailing_number, channel.position, channel.id)


def get_sdg_target_voice_channels(
    guild: discord.Guild,
    anchor_channel: discord.VoiceChannel,
) -> list[discord.VoiceChannel]:
    """Use voice channels from the same category as breakout rooms for SDG shuffle."""
    if anchor_channel.category is None:
        channels = [anchor_channel]
    else:
        channels = [
            channel
            for channel in anchor_channel.category.voice_channels
            if channel.id != getattr(guild.afk_channel, "id", None)
        ]
        if anchor_channel.id not in {channel.id for channel in channels}:
            channels.append(anchor_channel)

    return sorted(channels, key=sdg_voice_channel_sort_key)


def resolve_sdg_voice_channel(
    session: dict,
    guild: discord.Guild,
    voice_number: int,
) -> Optional[discord.VoiceChannel]:
    """Map SDG logical room numbers to real Discord voice channels."""
    index = voice_number - 1
    channel_ids = session.get("target_voice_channel_ids", [])
    if index < 0 or index >= len(channel_ids):
        return None

    channel = guild.get_channel(channel_ids[index])
    if isinstance(channel, discord.VoiceChannel):
        return channel
    return None


def resolve_sdg_voice_number(
    session: dict,
    channel_id: int,
) -> Optional[int]:
    """Map a real Discord voice-channel ID back to the logical SDG room number."""
    channel_ids = session.get("target_voice_channel_ids", [])
    try:
        return channel_ids.index(channel_id) + 1
    except ValueError:
        return None


def collect_sdg_present_members(
    session: dict,
    guild: discord.Guild,
) -> tuple[dict[int, discord.Member], dict[int, list[int]]]:
    """Collect all human members currently inside the managed SDG voice rooms."""
    tracked_channel_ids: list[int] = []
    seen_channel_ids: set[int] = set()

    for channel_id in session.get("target_voice_channel_ids", []):
        if channel_id in seen_channel_ids:
            continue
        tracked_channel_ids.append(channel_id)
        seen_channel_ids.add(channel_id)

    anchor_channel_id = session.get("anchor_voice_channel_id")
    if anchor_channel_id is not None and anchor_channel_id not in seen_channel_ids:
        tracked_channel_ids.append(anchor_channel_id)

    present_members: dict[int, discord.Member] = {}
    present_member_ids_by_channel: dict[int, list[int]] = {}

    for channel_id in tracked_channel_ids:
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.VoiceChannel):
            continue

        channel_member_ids: list[int] = []
        for member in channel.members:
            if member.bot:
                continue
            present_members[member.id] = member
            channel_member_ids.append(member.id)

        if channel_member_ids:
            present_member_ids_by_channel[channel.id] = channel_member_ids

    return present_members, present_member_ids_by_channel


def sdg_should_hold_group(
    member_ids: list[int],
    member_profiles_by_id: dict[int, dict],
) -> bool:
    """Hold only eligible newcomer groups that should get a full 10-minute mentor slot."""
    has_extended_slot_newcomer = any(
        member_profiles_by_id.get(member_id, {}).get("gets_extended_mentor_slot", False)
        for member_id in member_ids
    )
    if not has_extended_slot_newcomer:
        return False

    return any(
        member_profiles_by_id.get(member_id, {}).get("is_priority_partner", False)
        for member_id in member_ids
    )


def sdg_record_round_history(
    session: dict,
    round_groups: list[dict],
    member_profiles_by_id: dict[int, dict],
) -> None:
    """Track how often fresh newcomers spend a round slot with `core`."""
    round_counts = session.setdefault("fresh_newcomer_round_counts", {})
    core_round_counts = session.setdefault("fresh_newcomer_core_round_counts", {})

    for group in round_groups:
        member_ids = group["member_ids"]
        has_core = any(
            member_profiles_by_id.get(member_id, {}).get("is_core", False)
            for member_id in member_ids
        )
        for member_id in member_ids:
            profile = member_profiles_by_id.get(member_id)
            if profile is None or not profile["is_fresh_newcomer"]:
                continue

            round_counts[member_id] = round_counts.get(member_id, 0) + 1
            if has_core:
                core_round_counts[member_id] = core_round_counts.get(member_id, 0) + 1


def build_sdg_current_holding_groups(
    session: dict,
    present_member_ids_by_channel: dict[int, list[int]],
    paused_now_ids: set[int],
    member_profiles_by_id: dict[int, dict],
) -> tuple[list[dict], set[int], set[int]]:
    """
    Rebuild active 10-minute holds from the real room layout.

    This lets the next round react to manual room switches, leavers, and
    joiners instead of trusting the previous bot assignment forever.
    """
    holding_groups: list[dict] = []
    held_member_ids: set[int] = set()
    reserved_voice_numbers: set[int] = set()

    for channel_id in session.get("target_voice_channel_ids", []):
        member_ids = present_member_ids_by_channel.get(channel_id, [])
        if len(member_ids) < 2:
            continue

        if not any(member_id in paused_now_ids for member_id in member_ids):
            continue

        if not sdg_should_hold_group(member_ids, member_profiles_by_id):
            continue

        voice_number = resolve_sdg_voice_number(session, channel_id)
        if voice_number is None:
            continue

        holding_groups.append(
            {
                "member_ids": list(member_ids),
                "voice": voice_number,
            }
        )
        held_member_ids.update(member_ids)
        reserved_voice_numbers.add(voice_number)

    return holding_groups, held_member_ids, reserved_voice_numbers


def build_sdg_next_round(
    session: dict,
    present_members: dict[int, discord.Member],
    present_member_ids_by_channel: dict[int, list[int]],
) -> dict:
    """Advance the SDG state machine by one 5-minute round."""
    reference_time = utc_now()
    display_names_by_id = {
        member_id: member.display_name
        for member_id, member in present_members.items()
    }
    member_profiles_by_id = {
        member_id: sdg_build_member_profile(member, reference_time=reference_time)
        for member_id, member in present_members.items()
    }
    for member_id, profile in member_profiles_by_id.items():
        profile["planning_category"] = sdg_get_planning_category(session, member_id, profile)

    present_member_ids = list(present_members.keys())
    paused_now_ids = {
        member_id
        for member_id in session["paused_member_ids"]
        if member_id in present_members
    }

    holding_groups, held_member_ids, reserved_voices_now = build_sdg_current_holding_groups(
        session,
        present_member_ids_by_channel,
        paused_now_ids,
        member_profiles_by_id,
    )

    active_member_ids = [
        member_id
        for member_id in present_member_ids
        if member_id not in held_member_ids
    ]

    if len(active_member_ids) >= 2:
        raw_groups = sdg_make_groups(
            active_member_ids,
            member_profiles_by_id,
        )
    else:
        raw_groups = []

    groups = sdg_assign_voices(raw_groups, reserved_voices_now)
    paused_next, reserved_voices_next = sdg_get_next_round_paused(
        groups,
        member_profiles_by_id,
    )

    round_info = {
        "round_number": session["round_number"],
        "groups": groups,
        "holding_groups": holding_groups,
        "paused_now_ids": sdg_sort_member_ids(
            list(held_member_ids),
            display_names_by_id,
        ),
        "display_names_by_id": display_names_by_id,
        "present_member_ids": set(present_member_ids),
    }

    sdg_record_round_history(
        session,
        holding_groups + groups,
        member_profiles_by_id,
    )
    session["paused_member_ids"] = paused_next
    session["reserved_voice_numbers"] = reserved_voices_next
    session["round_number"] += 1
    session["last_round"] = {"groups": groups}

    return round_info


def format_sdg_group_members(member_ids: list[int], display_names_by_id: dict[int, str]) -> str:
    """Render one SDG group as a stable display-name list."""
    return " — ".join(
        display_names_by_id.get(member_id, f"user-{member_id}")
        for member_id in member_ids
    )


def build_sdg_group_line(
    session: dict,
    guild: discord.Guild,
    group: dict,
    display_names_by_id: dict[int, str],
) -> str:
    """Render one room assignment line for the SDG status message."""
    target_channel = resolve_sdg_voice_channel(session, guild, group["voice"])
    channel_label = target_channel.mention if target_channel is not None else f"`voice{group['voice']}`"
    people = format_sdg_group_members(group["member_ids"], display_names_by_id)
    return f"`voice{group['voice']}` -> {channel_label}: {people}"


def build_sdg_shuffle_content(
    session: dict,
    guild: discord.Guild,
    round_info: Optional[dict] = None,
    *,
    move_errors: Optional[list[str]] = None,
    stopped_reason: Optional[str] = None,
) -> str:
    """Build the live status message for the timed SDG shuffle."""
    lines = ["🎯 **SDG timed shuffle**"]
    tracked_count = len(session.get("participant_ids", set()))

    if stopped_reason is not None:
        lines.append(stopped_reason)
        lines.append(f"Tracked participants: {tracked_count}")
        return "\n".join(lines)

    if round_info is None:
        lines.append("Preparing the first round...")
        lines.append(f"Tracked participants: {tracked_count}")
        return "\n".join(lines)

    present_count = len(round_info["present_member_ids"])
    lines.append(f"Round: `{round_info['round_number']}`")
    lines.append(f"Tracked participants in managed rooms: `{present_count}/{tracked_count}`")

    next_round_at = session.get("next_round_at")
    if next_round_at is not None:
        lines.append(f"Next reshuffle: <t:{int(next_round_at.timestamp())}:R>")

    if round_info["holding_groups"]:
        lines.append(
            f"Holding for the second 5-minute slot "
            f"(role `{SDG_NEWCOMER_ROLE_NAME}` with {SDG_NEWCOMER_DAYS_THRESHOLD} or fewer days "
            f"with `{SDG_CORE_ROLE_NAME}`, `{TRUSTED_ROLE_NAME}`, or `{RELIABLE_ROLE_NAME}`):"
        )
        for group in round_info["holding_groups"]:
            lines.append(
                build_sdg_group_line(
                    session,
                    guild,
                    group,
                    round_info["display_names_by_id"],
                )
            )

    if round_info["groups"]:
        lines.append("New moves this round:")
        for group in round_info["groups"]:
            lines.append(
                build_sdg_group_line(
                    session,
                    guild,
                    group,
                    round_info["display_names_by_id"],
                )
            )
    elif not round_info["holding_groups"]:
        lines.append("No new groups this round.")

    if round_info["paused_now_ids"]:
        paused_names = ", ".join(
            round_info["display_names_by_id"].get(member_id, f"user-{member_id}")
            for member_id in round_info["paused_now_ids"]
        )
        lines.append(f"Skipping reshuffle this slot: {paused_names}")

    if move_errors:
        lines.append("Move issues:")
        lines.extend(f"- {error}" for error in move_errors[:10])

    return "\n".join(lines)


async def edit_sdg_shuffle_message(session: dict, content: str) -> None:
    """Best-effort message edit for the live SDG session status."""
    message = session.get("message")
    if message is None:
        return

    try:
        await message.edit(content=content)
    except discord.NotFound:
        pass
    except Exception as e:
        print(f"Failed to edit SDG shuffle message: {e}")


async def stop_sdg_shuffle_session(
    guild_id: int,
    reason: str,
    *,
    cancel_task: bool = True,
) -> bool:
    """Stop one active timed SDG shuffle session and freeze its status message."""
    session = active_sdg_shuffles.pop(guild_id, None)
    if session is None:
        return False

    session["next_round_at"] = None
    task = session.get("task")
    if (
        cancel_task
        and task is not None
        and task is not asyncio.current_task()
    ):
        task.cancel()

    guild = bot.get_guild(guild_id)
    if guild is not None:
        await edit_sdg_shuffle_message(
            session,
            build_sdg_shuffle_content(
                session,
                guild,
                stopped_reason=reason,
            ),
        )

    return True


async def move_sdg_groups_for_round(
    session: dict,
    guild: discord.Guild,
    round_info: dict,
) -> list[str]:
    """Move each newly assigned group into its target voice room."""
    move_errors: list[str] = []

    for group in round_info["groups"]:
        target_channel = resolve_sdg_voice_channel(session, guild, group["voice"])
        if target_channel is None:
            move_errors.append(
                f"`voice{group['voice']}` has no matching Discord voice channel."
            )
            continue

        if target_channel.user_limit and len(group["member_ids"]) > target_channel.user_limit:
            move_errors.append(
                f"`{target_channel.name}` only allows {target_channel.user_limit} members, "
                f"but this group has {len(group['member_ids'])}."
            )
            continue

        for member_id in group["member_ids"]:
            member = guild.get_member(member_id)
            if member is None or member.bot:
                continue

            member_voice = member.voice
            if member_voice is None or member_voice.channel is None:
                continue
            if member_voice.channel.id == target_channel.id:
                continue

            try:
                await member.move_to(
                    target_channel,
                    reason=f"SDG shuffle round {round_info['round_number']}",
                )
            except discord.Forbidden:
                move_errors.append(
                    f"Could not move **{member.display_name}** to `{target_channel.name}`. "
                    "Check `Move Members` and channel access."
                )
            except discord.HTTPException as error:
                move_errors.append(
                    f"Discord rejected moving **{member.display_name}** to `{target_channel.name}`: {error}"
                )

    return move_errors


async def run_sdg_shuffle_session(guild_id: int) -> None:
    """Drive the repeating 5-minute SDG reshuffle loop for one guild."""
    try:
        while True:
            session = active_sdg_shuffles.get(guild_id)
            if session is None:
                return

            guild = bot.get_guild(guild_id)
            if guild is None:
                await stop_sdg_shuffle_session(
                    guild_id,
                    "Stopped because the bot is no longer connected to the server.",
                    cancel_task=False,
                )
                return

            present_members, present_member_ids_by_channel = collect_sdg_present_members(
                session,
                guild,
            )
            session["participant_ids"] = set(present_members)
            round_info = build_sdg_next_round(
                session,
                present_members,
                present_member_ids_by_channel,
            )
            move_errors = await move_sdg_groups_for_round(session, guild, round_info)
            session["next_round_at"] = utc_now() + timedelta(seconds=SDG_ROUND_SECONDS)

            await edit_sdg_shuffle_message(
                session,
            build_sdg_shuffle_content(
                session,
                guild,
                round_info,
                move_errors=move_errors,
                ),
            )

            if (
                len(round_info["present_member_ids"]) < 2
                and not round_info["groups"]
                and not round_info["holding_groups"]
            ):
                await stop_sdg_shuffle_session(
                    guild_id,
                    "Stopped because fewer than 2 tracked participants remain in the managed rooms.",
                    cancel_task=False,
                )
                return

            await asyncio.sleep(SDG_ROUND_SECONDS)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        traceback.print_exc()
        await stop_sdg_shuffle_session(
            guild_id,
            f"Stopped after an error: `{error}`",
            cancel_task=False,
        )


async def defer_hybrid_command(ctx: commands.Context) -> None:
    """Acknowledge slash invocations that send their real output directly to the channel."""
    if ctx.interaction is None or ctx.interaction.response.is_done():
        return

    await ctx.defer(ephemeral=True)


async def send_hybrid_response(
    ctx: commands.Context,
    content: str,
    *,
    ephemeral: bool = False,
) -> None:
    """Send a response that works for both prefix and slash invocations."""
    if ctx.interaction is None:
        await ctx.send(content)
        return

    if ctx.interaction.response.is_done():
        await ctx.interaction.followup.send(content, ephemeral=ephemeral)
        return

    await ctx.interaction.response.send_message(content, ephemeral=ephemeral)


async def finish_hybrid_command(ctx: commands.Context, content: str) -> None:
    """Finish a deferred slash invocation with an ephemeral confirmation."""
    try:
        await send_hybrid_response(ctx, content, ephemeral=True)
    except Exception as e:
        print(f"Error sending command followup: {e}")


async def send_interaction_error(
    interaction: discord.Interaction,
    message: str,
) -> None:
    """Best-effort error response for slash command failures."""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except Exception as send_error:
        print(f"Failed to send interaction error response: {send_error}")


@bot.event
async def on_voice_state_update(member: discord.Member, before, after) -> None:
    """
    React to members joining/leaving tracked voice channels:
    - schedule removal when they leave
    - cancel removal and update labels when they return.
    """
    previous_channel = before.channel if is_trackable_voice_channel(before.channel) else None
    current_channel = after.channel if is_trackable_voice_channel(after.channel) else None

    if (
        not member.bot
        and (
            (previous_channel is not None and current_channel is None)
            or (previous_channel is None and current_channel is not None)
            or (
                previous_channel is not None
                and current_channel is not None
                and previous_channel.id != current_channel.id
            )
        )
    ):
        event_time = utc_now()
        if previous_channel is not None:
            finish_voice_session(member.guild.id, member.id, ended_at=event_time)
        if current_channel is not None:
            start_voice_session(member, current_channel, started_at=event_time)

    handle_camera_voice_state_update(member, before, after)

    for text_channel_id, state in list(active_shuffles.items()):
        guild = bot.get_guild(state["guild_id"])
        if not guild or guild.id != member.guild.id:
            continue

        voice_channel_id = state["voice_channel_id"]
        excluded_member_ids = state.get("excluded_member_ids", set())
        allow_hot_joiners = state.get("allow_hot_joiners", True)

        # --- MEMBER JOINED the tracked voice channel ---
        if (
            after.channel is not None
            and after.channel.id == voice_channel_id
            and (before.channel is None or before.channel.id != voice_channel_id)
        ):
            if member.id in excluded_member_ids:
                pending = state["pending_removals"].pop(member.id, None)
                if pending:
                    pending.cancel()
                removed_from_order = False
                if member.id in state["order"]:
                    state["order"].remove(member.id)
                    state.get("labels", {}).pop(member.id, None)
                    removed_from_order = True
                ignored_ids = state.setdefault("ignored_excluded_member_ids", set())
                ignored_changed = member.id not in ignored_ids
                ignored_ids.add(member.id)
                if ignored_changed or removed_from_order:
                    await update_shuffle_message(state)
                continue

            pending = state["pending_removals"].get(member.id)

            # They had a removal timer -> they returned "in time"
            if pending:
                pending.cancel()
                state["pending_removals"].pop(member.id, None)

                # If they are still in the list -> mark as returned with 🌌
                if member.id in state["order"]:
                    labels = state.setdefault("labels", {})
                    was_hot_joiner = is_shuffle_hot_joiner(state, member.id)
                    if was_hot_joiner:
                        state.setdefault("hot_joiner_member_ids", set()).add(member.id)
                    labels[member.id] = build_shuffle_member_label(
                        member,
                        hot_joiner=was_hot_joiner,
                        returned=True,
                    )
                    state["ever_seen"].add(member.id)
                    await update_shuffle_message(state)
            else:
                # No timer: either they were removed earlier or they are a completely new member
                if member.id not in state["order"]:
                    if member.id not in state.get("ever_seen", set()) and not allow_hot_joiners:
                        continue

                    state["order"].append(member.id)
                    labels = state.setdefault("labels", {})

                    # If they were in the list before -> this is a "return" -> 🌌
                    if member.id in state.get("ever_seen", set()):
                        was_hot_joiner = is_shuffle_hot_joiner(state, member.id)
                        if was_hot_joiner:
                            state.setdefault("hot_joiner_member_ids", set()).add(member.id)
                        labels[member.id] = build_shuffle_member_label(
                            member,
                            hot_joiner=was_hot_joiner,
                            returned=True,
                        )
                    else:
                        # Completely new member -> ✨
                        labels[member.id] = build_shuffle_member_label(member, hot_joiner=True)
                        state.setdefault("hot_joiner_member_ids", set()).add(member.id)
                        state["ever_seen"].add(member.id)

                    await update_shuffle_message(state)

            continue  # go to the next shuffle state

        # --- MEMBER LEFT the tracked voice channel ---
        if (
            before.channel is not None
            and before.channel.id == voice_channel_id
            and (after.channel is None or after.channel.id != voice_channel_id)
        ):
            # If they are not in the order, nothing to do
            if member.id not in state["order"]:
                continue

            # If they already have a timer, do not duplicate it
            if member.id in state["pending_removals"]:
                continue

            # Create a delayed removal task
            task = asyncio.create_task(schedule_removal(text_channel_id, member.id))
            state["pending_removals"][member.id] = task


# ===============================
#   INTERNAL: event scheduling
# ===============================

async def create_scheduled_shuffle_event(
    guild: discord.Guild,
    text_channel: discord.abc.Messageable,
    voice_channel: discord.VoiceChannel,
    start_in_minutes: int,
    duration_minutes: int,
    name: str
) -> None:
    """Create a scheduled event and set up auto-start shuffle and auto-complete."""
    start_time = datetime.now(timezone.utc) + timedelta(minutes=start_in_minutes)
    end_time = start_time + timedelta(minutes=duration_minutes)

    try:
        event = await guild.create_scheduled_event(
            name=name,
            start_time=start_time,
            end_time=end_time,
            privacy_level=discord.PrivacyLevel.guild_only,
            entity_type=discord.EntityType.voice,
            channel=voice_channel
        )
    except discord.Forbidden:
        await text_channel.send(
            "I do not have permission to create events. "
            "Please grant the bot the `Manage Events` permission."
        )
        return
    except Exception as e:
        await text_channel.send(f"Failed to create event: {e}")
        return

    # Human-friendly time output using Discord timestamp tags
    start_ts = int(start_time.timestamp())
    end_ts = int(end_time.timestamp())
    shuffle_target = text_channel
    remember_event_text_channel(
        event.id,
        shuffle_target.id,
        start_time=start_time,
    )
    shuffle_target_name = getattr(shuffle_target, "mention", "#unknown")
    await text_channel.send(
        f"📅 Scheduled event **{event.name}**\n"
        f"Starts: <t:{start_ts}:F>\n"
        f"Ends:   <t:{end_ts}:F>\n"
        f"Voice channel: {voice_channel.mention}\n"
        f"Shuffle will auto-run here: {shuffle_target_name}"
    )

    guild_id = guild.id
    schedule_event_lifecycle(
        guild_id,
        event.id,
        start_time,
        end_time,
        shuffle_target.id,
        replace_existing=True,
    )


# ===============================
#   COMMAND: shuffle
# ===============================

@bot.hybrid_command(
    name='shuffle',
    description='Shuffle members in your voice channel'
)
@discord.app_commands.describe(
    exclude="Mentions, IDs, or exact names to exclude from the shuffle"
)
async def shuffle_voice_members(
    ctx: commands.Context,
    *,
    exclude: str = "",
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in a voice channel to use this command!")
        return

    excluded_member_ids: set[int] = set()
    if exclude.strip():
        if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
            append_shuffle_audit_log(
                action="shuffle_exclude_denied",
                guild=guild,
                actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
                channel_id=getattr(ctx.channel, "id", None),
                details={"raw_exclude": exclude},
            )
            await ctx.send(
                f"You do not have permission to exclude members from shuffles. "
                f"Required role: {get_shuffle_exclusion_access_label()}.",
            )
            return

        excluded_member_ids, unresolved = resolve_excluded_members(guild, exclude)
        if unresolved:
            await ctx.send(
                "Could not resolve these users: "
                + ", ".join(f"`{value}`" for value in unresolved[:10])
            )
            return

        append_shuffle_audit_log(
            action="shuffle_exclude_once",
            guild=guild,
            actor=ctx.author,
            channel_id=getattr(ctx.channel, "id", None),
            details={
                "voice_channel_id": ctx.author.voice.channel.id,
                "excluded_member_ids": sorted(excluded_member_ids),
                "excluded_members": format_member_list(guild, excluded_member_ids),
            },
        )

    voice_channel = ctx.author.voice.channel
    await start_shuffle_for_channel(
        guild,
        voice_channel,
        ctx.channel,
        excluded_member_ids=excluded_member_ids,
    )
    await finish_hybrid_command(ctx, "Shuffle posted in this channel.")


@bot.hybrid_command(
    name='sdg_shuffle',
    description='Move members into timed pairs/trios using roles `нашедшийся` and `core`'
)
async def sdg_shuffle(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_sdg_shuffle_access(ctx.author):
        await ctx.send(
            f"You do not have permission to start timed room shuffles. "
            f"Required: {get_sdg_shuffle_access_label()}.",
        )
        return

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in a voice channel to start SDG shuffle!")
        return

    anchor_channel = ctx.author.voice.channel
    if not isinstance(anchor_channel, discord.VoiceChannel):
        await ctx.send("SDG shuffle only works from a regular voice channel.")
        return

    participants = [member for member in anchor_channel.members if not member.bot]
    if len(participants) < 2:
        await ctx.send("Need at least 2 human members in your voice channel to start SDG shuffle.")
        return

    target_channels = get_sdg_target_voice_channels(guild, anchor_channel)
    max_concurrent_groups = max(1, len(participants) // 2)
    if len(target_channels) < max_concurrent_groups:
        await ctx.send(
            f"I found only {len(target_channels)} usable voice room(s) in this category, "
            f"but up to {max_concurrent_groups} are needed for {len(participants)} participants.",
        )
        return
    target_channels = target_channels[:max_concurrent_groups]

    blocked_channels = [
        channel.mention
        for channel in target_channels
        if not bot_can_move_members(guild, channel)
    ]
    if blocked_channels:
        await ctx.send(
            "I need the `Move Members` permission in all managed breakout rooms. "
            "Please check: "
            + ", ".join(blocked_channels[:10]),
        )
        return

    existing_session = active_sdg_shuffles.get(guild.id)
    replaced_existing = existing_session is not None
    if existing_session is not None:
        await stop_sdg_shuffle_session(
            guild.id,
            f"Stopped because **{ctx.author.display_name}** started a new SDG shuffle.",
        )

    session = {
        "guild_id": guild.id,
        "text_channel_id": getattr(ctx.channel, "id", None),
        "anchor_voice_channel_id": anchor_channel.id,
        "target_voice_channel_ids": [channel.id for channel in target_channels],
        "participant_ids": {member.id for member in participants},
        "paused_member_ids": set(),
        "reserved_voice_numbers": set(),
        "fresh_newcomer_round_counts": {},
        "fresh_newcomer_core_round_counts": {},
        "round_number": 1,
        "last_round": None,
        "next_round_at": None,
        "message": None,
        "task": None,
    }

    status_message = await ctx.channel.send(build_sdg_shuffle_content(session, guild))
    session["message"] = status_message
    active_sdg_shuffles[guild.id] = session
    session["task"] = asyncio.create_task(run_sdg_shuffle_session(guild.id))

    room_mentions = ", ".join(channel.mention for channel in target_channels[:10])
    if len(target_channels) > 10:
        room_mentions += ", ..."

    confirmation = (
        f"SDG shuffle started from `{anchor_channel.name}`. "
        f"Managed rooms: {room_mentions}. "
        f"Pairs are preferred before trios. "
        f"Role `{SDG_CORE_ROLE_NAME}` has top newcomer priority, then `{TRUSTED_ROLE_NAME}`, then "
        f"`{RELIABLE_ROLE_NAME}`. Members with role `{SDG_NEWCOMER_ROLE_NAME}` who have been here "
        f"{SDG_NEWCOMER_DAYS_THRESHOLD} days or less hold mentor groups for 10 minutes, and the bot aims to give them "
        f"`{SDG_CORE_ROLE_NAME}` in about half of their round slots; the rest reshuffle every 5 minutes."
    )
    if replaced_existing:
        confirmation += " Previous SDG shuffle session was replaced."

    await finish_hybrid_command(ctx, confirmation)


@bot.hybrid_command(
    name='sdg_shuffle_stop',
    description='Stop the active timed SDG shuffle in this server'
)
async def sdg_shuffle_stop(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_sdg_shuffle_access(ctx.author):
        await ctx.send(
            f"You do not have permission to stop timed room shuffles. "
            f"Required: {get_sdg_shuffle_access_label()}.",
        )
        return

    stopped = await stop_sdg_shuffle_session(
        guild.id,
        f"Stopped by **{ctx.author.display_name}**.",
    )
    if not stopped:
        await finish_hybrid_command(ctx, "No active SDG shuffle is running in this server.")
        return

    await finish_hybrid_command(ctx, "SDG shuffle stopped.")


@bot.hybrid_command(
    name='shuffle_exclude_add',
    description='Persistently exclude users from future shuffles'
)
@discord.app_commands.describe(
    users="Mentions, IDs, or exact names to exclude from future shuffles"
)
async def shuffle_exclude_add(
    ctx: commands.Context,
    *,
    users: str,
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_exclude_add_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
            details={"raw_users": users},
        )
        await ctx.send(
            f"You do not have permission to manage persistent shuffle exclusions. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    member_ids, unresolved = resolve_excluded_members(guild, users)
    if unresolved:
        await ctx.send(
            "Could not resolve these users: "
            + ", ".join(f"`{value}`" for value in unresolved[:10])
        )
        return

    if not member_ids:
        await ctx.send("No valid users were provided.")
        return

    added_count = add_persistent_excluded_members(guild.id, member_ids)
    member_list = format_member_list(guild, member_ids)
    status = (
        f"Added to persistent shuffle exclusions: {member_list}"
        if added_count else
        f"Already excluded: {member_list}"
    )
    append_shuffle_audit_log(
        action="shuffle_exclude_add",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "member_ids": sorted(member_ids),
            "members": member_list,
            "added_count": added_count,
        },
    )
    await refresh_active_shuffle_exclusions(guild.id)
    await finish_hybrid_command(ctx, status)


@bot.hybrid_command(
    name='shuffle_exclude_remove',
    description='Remove users from the persistent shuffle exclusion list'
)
@discord.app_commands.describe(
    users="Mentions, IDs, or exact names to allow back into future shuffles"
)
async def shuffle_exclude_remove(
    ctx: commands.Context,
    *,
    users: str,
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_exclude_remove_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
            details={"raw_users": users},
        )
        await ctx.send(
            f"You do not have permission to manage persistent shuffle exclusions. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    member_ids, unresolved = resolve_excluded_members(guild, users, allow_unknown_ids=True)
    if unresolved:
        await ctx.send(
            "Could not resolve these users: "
            + ", ".join(f"`{value}`" for value in unresolved[:10])
        )
        return

    if not member_ids:
        await ctx.send("No valid users were provided.")
        return

    removed_count = remove_persistent_excluded_members(guild.id, member_ids)
    member_list = format_member_list(guild, member_ids)
    status = (
        f"Removed from persistent shuffle exclusions: {member_list}"
        if removed_count else
        f"These users were not persistently excluded: {member_list}"
    )
    append_shuffle_audit_log(
        action="shuffle_exclude_remove",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "member_ids": sorted(member_ids),
            "members": member_list,
            "removed_count": removed_count,
        },
    )
    await refresh_active_shuffle_exclusions(guild.id)
    await finish_hybrid_command(ctx, status)


@bot.hybrid_command(
    name='shuffle_exclude_list',
    description='List users persistently excluded from future shuffles'
)
async def shuffle_exclude_list(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_exclude_list_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to view persistent shuffle exclusions. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    member_ids = get_persistent_excluded_member_ids(guild.id)
    append_shuffle_audit_log(
        action="shuffle_exclude_list",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "member_ids": sorted(member_ids),
            "members": format_member_list(guild, member_ids) if member_ids else "",
        },
    )
    if not member_ids:
        await finish_hybrid_command(ctx, "No persistent shuffle exclusions are set.")
        return

    await send_hybrid_response(
        ctx,
        "**Persistent shuffle exclusions**\n" + format_member_list(guild, member_ids),
    )


@bot.hybrid_command(
    name='shuffle_hot_joiners_on',
    description='Allow brand-new late joiners to be added to active shuffles'
)
async def shuffle_hot_joiners_on(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_hot_joiners_on_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage shuffle hot-joiner behavior. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    set_allow_hot_joiners(guild.id, True)
    append_shuffle_audit_log(
        action="shuffle_hot_joiners_on",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"allow_hot_joiners": True},
    )
    await refresh_active_shuffle_settings(guild.id)
    await finish_hybrid_command(ctx, "Hot joiners are now enabled for future and active shuffles.")


@bot.hybrid_command(
    name='shuffle_hot_joiners_off',
    description='Prevent brand-new late joiners from being added to active shuffles'
)
async def shuffle_hot_joiners_off(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_hot_joiners_off_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage shuffle hot-joiner behavior. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    set_allow_hot_joiners(guild.id, False)
    append_shuffle_audit_log(
        action="shuffle_hot_joiners_off",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"allow_hot_joiners": False},
    )
    await refresh_active_shuffle_settings(guild.id)
    await finish_hybrid_command(ctx, "Hot joiners are now disabled. Reconnects still return to the shuffle.")


@bot.hybrid_command(
    name='shuffle_hot_joiners_status',
    description='Show whether brand-new late joiners are added to active shuffles'
)
async def shuffle_hot_joiners_status(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_hot_joiners_status_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to view shuffle hot-joiner behavior. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    allow_hot_joiners = get_allow_hot_joiners(guild.id)
    append_shuffle_audit_log(
        action="shuffle_hot_joiners_status",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"allow_hot_joiners": allow_hot_joiners},
    )
    await finish_hybrid_command(
        ctx,
        "Hot joiners are currently "
        + ("enabled." if allow_hot_joiners else "disabled. Reconnects are still allowed."),
    )


@bot.hybrid_command(
    name='camera_check_on',
    description='Disconnect users in your voice channel if their camera stays off'
)
async def camera_check_on(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="camera_check_on_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in a voice channel to start camera check mode.")
        return

    voice_channel = ctx.author.voice.channel
    if not is_trackable_voice_channel(voice_channel):
        await ctx.send("Camera check mode can only be used in voice or stage channels.")
        return
    if not bot_can_move_members(guild, voice_channel):
        await ctx.send("I need the `Move Members` permission in that voice channel to use camera check mode.")
        return

    _, created = start_camera_enforcement(
        guild,
        voice_channel,
        ctx.channel,
        source=CAMERA_SOURCE_MANUAL,
    )
    append_shuffle_audit_log(
        action="camera_check_on",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "voice_channel_id": voice_channel.id,
            "created": created,
            "camera_grace_seconds": CAMERA_GRACE_SECONDS,
        },
    )
    await finish_hybrid_command(
        ctx,
        f"Camera check is active in `{voice_channel.name}`. "
        f"Camera-off members will be disconnected after {CAMERA_GRACE_SECONDS} seconds.",
    )


@bot.hybrid_command(
    name='camera_check_off',
    description='Stop standalone camera check mode in your voice channel'
)
async def camera_check_off(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="camera_check_off_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in the voice channel whose standalone camera check should stop.")
        return

    voice_channel = ctx.author.voice.channel
    state = active_camera_enforcements.get((guild.id, voice_channel.id))
    had_manual_source = bool(state and CAMERA_SOURCE_MANUAL in state.get("sources", set()))
    fully_stopped = stop_camera_enforcement(
        guild.id,
        voice_channel.id,
        source=CAMERA_SOURCE_MANUAL,
    )
    append_shuffle_audit_log(
        action="camera_check_off",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "voice_channel_id": voice_channel.id,
            "had_manual_source": had_manual_source,
            "fully_stopped": fully_stopped,
        },
    )

    if not had_manual_source:
        await finish_hybrid_command(ctx, f"Standalone camera check was not active in `{voice_channel.name}`.")
        return

    if fully_stopped:
        await finish_hybrid_command(ctx, f"Camera check stopped in `{voice_channel.name}`.")
        return

    await finish_hybrid_command(
        ctx,
        f"Standalone camera check stopped in `{voice_channel.name}`. "
        "Shuffle camera check is still active there.",
    )


@bot.hybrid_command(
    name='camera_check_status',
    description='Show active camera check modes in this server'
)
async def camera_check_status(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="camera_check_status_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to view camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    lines = []
    for (state_guild_id, voice_channel_id), state in sorted(active_camera_enforcements.items()):
        if state_guild_id != guild.id:
            continue
        voice_channel = guild.get_channel(voice_channel_id)
        channel_label = voice_channel.name if voice_channel else f"channel-{voice_channel_id}"
        pending_count = len(state.get("pending_kicks", {}))
        source_label = format_camera_enforcement_sources(state.get("sources", set()))
        lines.append(
            f"`{channel_label}`: sources `{source_label}`, "
            f"{pending_count} pending kick(s)"
        )

    append_shuffle_audit_log(
        action="camera_check_status",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"active_channel_count": len(lines)},
    )

    if not lines:
        await finish_hybrid_command(ctx, "No active camera checks in this server.")
        return

    await send_hybrid_response(
        ctx,
        "**Active camera checks**\n"
        + f"Grace period: {CAMERA_GRACE_SECONDS} seconds\n"
        + "\n".join(lines),
    )


@bot.hybrid_command(
    name='shuffle_camera_on',
    description='Require cameras during future and active shuffles'
)
async def shuffle_camera_on(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_camera_on_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage shuffle camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    set_require_camera_for_shuffles(guild.id, True)
    append_shuffle_audit_log(
        action="shuffle_camera_on",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"require_camera": True, "camera_grace_seconds": CAMERA_GRACE_SECONDS},
    )
    await refresh_active_shuffle_camera_enforcement(guild.id)
    await finish_hybrid_command(
        ctx,
        f"Shuffle camera check is now enabled. "
        f"Camera-off members will be disconnected after {CAMERA_GRACE_SECONDS} seconds.",
    )


@bot.hybrid_command(
    name='shuffle_camera_off',
    description='Stop requiring cameras during future and active shuffles'
)
async def shuffle_camera_off(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_camera_off_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to manage shuffle camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    set_require_camera_for_shuffles(guild.id, False)
    append_shuffle_audit_log(
        action="shuffle_camera_off",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={"require_camera": False},
    )
    await refresh_active_shuffle_camera_enforcement(guild.id)
    await finish_hybrid_command(ctx, "Shuffle camera check is now disabled.")


@bot.hybrid_command(
    name='shuffle_camera_status',
    description='Show whether shuffles require cameras'
)
async def shuffle_camera_status(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    if not isinstance(ctx.author, discord.Member) or not has_shuffle_exclusion_access(ctx.author):
        append_shuffle_audit_log(
            action="shuffle_camera_status_denied",
            guild=guild,
            actor=ctx.author if isinstance(ctx.author, discord.Member) else None,
            channel_id=getattr(ctx.channel, "id", None),
        )
        await ctx.send(
            f"You do not have permission to view shuffle camera checks. "
            f"Required role: {get_shuffle_exclusion_access_label()}.",
        )
        return

    require_camera = get_require_camera_for_shuffles(guild.id)
    active_shuffle_camera_channels = 0
    for state in active_camera_enforcements.values():
        if state["guild_id"] != guild.id:
            continue
        if any(
            source.startswith(CAMERA_SOURCE_SHUFFLE_PREFIX)
            for source in state.get("sources", set())
        ):
            active_shuffle_camera_channels += 1

    append_shuffle_audit_log(
        action="shuffle_camera_status",
        guild=guild,
        actor=ctx.author,
        channel_id=getattr(ctx.channel, "id", None),
        details={
            "require_camera": require_camera,
            "active_shuffle_camera_channels": active_shuffle_camera_channels,
        },
    )
    await finish_hybrid_command(
        ctx,
        "Shuffle camera check is currently "
        + ("enabled" if require_camera else "disabled")
        + f". Active shuffle camera channel(s): {active_shuffle_camera_channels}.",
    )


# ===============================
#   COMMAND: schedule_event
# ===============================

@bot.hybrid_command(
    name='schedule_event',
    description='Schedule a voice event that auto-starts shuffle and auto-ends'
)
async def schedule_event(
    ctx: commands.Context,
    start_in_minutes: int,
    duration_minutes: int,
    *,
    name: str = "Shuffle session"
):
    await defer_hybrid_command(ctx)

    if not isinstance(ctx.author, discord.Member) or not has_reliable_role(ctx.author):
        await ctx.send(
            f"You do not have permission to schedule events. "
            f"Required role: `{RELIABLE_ROLE_NAME}`.",
        )
        return

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in a voice channel to schedule a voice event!")
        return

    voice_channel = ctx.author.voice.channel
    await create_scheduled_shuffle_event(
        ctx.guild,
        ctx.channel,
        voice_channel,
        start_in_minutes,
        duration_minutes,
        name
    )
    await finish_hybrid_command(ctx, "Scheduled event created.")


# ===============================
#   MENU: Modal for scheduling
# ===============================

class ScheduleEventModal(discord.ui.Modal, title="Schedule shuffle event"):
    start_in = discord.ui.TextInput(
        label="Start in (minutes)",
        default="10",
        required=True,
        max_length=5
    )
    duration = discord.ui.TextInput(
        label="Duration (minutes)",
        default="60",
        required=True,
        max_length=5
    )
    name = discord.ui.TextInput(
        label="Event name",
        default="Shuffle session",
        required=False,
        max_length=100
    )

    async def on_submit(self, interaction: discord.Interaction):
        user = interaction.user
        if not isinstance(user, discord.Member) or not has_reliable_role(user):
            await interaction.response.send_message(
                f"You do not have permission to schedule events. "
                f"Required role: `{RELIABLE_ROLE_NAME}`.",
                ephemeral=True
            )
            return

        if user.voice is None or user.voice.channel is None:
            await interaction.response.send_message(
                "You need to be in a voice channel to schedule a voice event!",
                ephemeral=True
            )
            return

        try:
            start_in_minutes = int(str(self.start_in))
            duration_minutes = int(str(self.duration))
        except ValueError:
            await interaction.response.send_message(
                "Start time and duration must be integers (minutes).",
                ephemeral=True
            )
            return

        event_name = str(self.name).strip() or "Shuffle session"

        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "This command can only be used in a server.",
                ephemeral=True
            )
            return

        voice_channel = user.voice.channel

        # We must respond to the modal before doing long work
        await interaction.response.send_message(
            f"Creating scheduled event **{event_name}**...",
            ephemeral=True
        )

        await create_scheduled_shuffle_event(
            guild,
            interaction.channel,
            voice_channel,
            start_in_minutes,
            duration_minutes,
            event_name
        )


@bot.tree.command(
    name="schedule_event_menu",
    description="Open a menu to schedule a shuffle event"
)
async def schedule_event_menu(interaction: discord.Interaction):
    """Slash command that opens a modal to schedule an event."""
    user = interaction.user
    if not isinstance(user, discord.Member) or not has_reliable_role(user):
        await interaction.response.send_message(
            f"You do not have permission to schedule events. "
            f"Required role: `{RELIABLE_ROLE_NAME}`.",
            ephemeral=True
        )
        return

    await interaction.response.send_modal(ScheduleEventModal())


# ===============================
#   COMMAND: attach_event
# ===============================

@bot.hybrid_command(
    name='attach_event',
    description='Attach shuffle auto-start/auto-end to an existing scheduled voice event'
)
async def attach_event(ctx: commands.Context, event_id: str):
    """Attach auto-shuffle to an already existing (possibly recurring) voice event."""
    await defer_hybrid_command(ctx)

    user = ctx.author

    # Permission check
    if not isinstance(user, discord.Member) or not has_reliable_role(user):
        await ctx.send(
            f"You do not have permission to attach events. "
            f"Required role: `{RELIABLE_ROLE_NAME}`.",
        )
        return

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    # Allow "event_id:123..." format by stripping prefix
    if event_id.lower().startswith("event_id:"):
        event_id = event_id.split(":", 1)[1].strip()

    # Convert snowflake to int safely
    try:
        event_id_int = int(event_id)
    except ValueError:
        await ctx.send(f"`{event_id}` is not a valid event ID (must be a numeric snowflake).")
        return

    # Fetch the event
    try:
        event = await guild.fetch_scheduled_event(event_id_int)
    except discord.NotFound:
        await ctx.send(f"No event found with ID `{event_id}`.")
        return
    except Exception as e:
        await ctx.send(f"Error fetching event: `{e}`")
        return

    # Must be a voice event (shuffle depends on voice members)
    if event.entity_type != discord.EntityType.voice:
        await ctx.send("This event is not a voice event. Shuffle requires a voice channel.")
        return

    voice_channel = guild.get_channel(event.channel_id)
    if not isinstance(voice_channel, discord.VoiceChannel):
        await ctx.send("Could not find the voice channel for this event.")
        return

    # Get start/end times; recurring events still expose a next start_time
    start_time = event.start_time  # timezone-aware datetime
    end_time = event.end_time

    if start_time is None:
        await ctx.send(
            "This event has no start time (API gave `start_time = None`). "
            "I cannot attach shuffle logic to it."
        )
        return

    # If end_time is missing (can happen for some recurring setups), pick a default duration
    if end_time is None:
        end_time = start_time + timedelta(hours=2)

    shuffle_target = ctx.channel
    remember_event_text_channel(
        event.id,
        shuffle_target.id,
        start_time=start_time,
    )

    await ctx.send(
        f"Attached to event **{event.name}** (`{event.id}`).\n"
        f"Status: `{event.status.name}`\n"
        f"Starts: <t:{int(start_time.timestamp())}:F>\n"
        f"Ends:   <t:{int(end_time.timestamp())}:F>\n\n"
        f"Shuffle will run in {shuffle_target.mention} "
        f"for **this upcoming occurrence**."
    )

    guild_id = guild.id
    schedule_event_lifecycle(
        guild_id,
        event.id,
        start_time,
        end_time,
        shuffle_target.id,
        replace_existing=True,
    )


# ===============================
#   COMMAND: list_events
# ===============================

@bot.hybrid_command(
    name='list_events',
    description='List scheduled server events and their IDs'
)
async def list_events(ctx: commands.Context):
    """Show all scheduled server events that this bot can see, with their IDs."""
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await ctx.send("This command can only be used inside a server.")
        return

    try:
        events = await guild.fetch_scheduled_events()
    except Exception as e:
        await ctx.send(f"Failed to fetch scheduled events: `{e}`")
        return

    if not events:
        await ctx.send("No scheduled events found on this server.")
        return

    lines = []
    for ev in events:
        start = ev.start_time
        end = ev.end_time
        start_ts = int(start.timestamp()) if start else None
        end_ts = int(end.timestamp()) if end else None

        line = f"**{ev.name}** — ID: `{ev.id}` — status: `{ev.status.name}`"
        if start_ts:
            line += f"\n  starts: <t:{start_ts}:F>"
        if end_ts:
            line += f"\n  ends:   <t:{end_ts}:F>"

        lines.append(line)

    await ctx.send("\n\n".join(lines))


# ===============================
#   COMMANDS: voice activity
# ===============================

@bot.hybrid_command(
    name='voice_stats',
    description='Show saved voice totals for a server member'
)
async def voice_stats(
    ctx: commands.Context,
    member: Optional[discord.Member] = None,
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await send_hybrid_response(ctx, "This command can only be used inside a server.", ephemeral=True)
        return

    target = member or ctx.author
    if not isinstance(target, discord.Member):
        await send_hybrid_response(ctx, "Could not resolve the target member.", ephemeral=True)
        return

    today_label, today_start, _ = get_report_period_bounds("today")
    week_label, week_start, _ = get_report_period_bounds("week")
    _, all_start, all_end = get_report_period_bounds("all")

    today_summary = summarize_voice_usage(
        guild.id,
        target.id,
        range_start=today_start,
    )
    week_summary = summarize_voice_usage(
        guild.id,
        target.id,
        range_start=week_start,
    )
    all_time_summary = summarize_voice_usage(
        guild.id,
        target.id,
        range_start=all_start,
        range_end=all_end,
    )

    lines = [
        f"**Voice stats for {target.display_name}**",
        f"Timezone: `{format_timezone_label()}`",
        (
            f"{today_label.title()}: {format_duration(today_summary['total_seconds'])} "
            f"across {format_session_count(today_summary['session_count'])}"
        ),
        (
            f"{week_label.title()}: {format_duration(week_summary['total_seconds'])} "
            f"across {format_session_count(week_summary['session_count'])}"
        ),
        (
            f"All time: {format_duration(all_time_summary['total_seconds'])} "
            f"across {format_session_count(all_time_summary['session_count'])}"
        ),
    ]

    active_session = all_time_summary["active_session"]
    if active_session is not None:
        channel_label = resolve_voice_channel_label(
            guild,
            active_session["channel_id"],
            active_session["channel_name"],
        )
        started_ts = int(active_session["started_at"].timestamp())
        lines.append(
            f"Active now: `{channel_label}` since <t:{started_ts}:F> "
            f"({format_duration(active_session['duration_seconds'])})"
        )

    await send_hybrid_response(ctx, "\n".join(lines))


@bot.hybrid_command(
    name='voice_sessions',
    description='Show recent tracked voice sessions for a server member'
)
async def voice_sessions(
    ctx: commands.Context,
    member: Optional[discord.Member] = None,
    limit: int = 10,
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await send_hybrid_response(ctx, "This command can only be used inside a server.", ephemeral=True)
        return

    target = member or ctx.author
    if not isinstance(target, discord.Member):
        await send_hybrid_response(ctx, "Could not resolve the target member.", ephemeral=True)
        return

    limit = normalize_report_limit(limit, default=10, minimum=1, maximum=20)
    now = utc_now()
    sessions = [
        build_session_record_from_row(row, active=True, now=now)
        for row in load_active_voice_sessions(guild.id, user_id=target.id)
    ]
    sessions.extend(
        build_session_record_from_row(row, active=False, now=now)
        for row in load_completed_voice_sessions(
            guild.id,
            user_id=target.id,
            limit=limit,
        )
    )
    sessions.sort(key=lambda session: session["started_at"], reverse=True)
    sessions = sessions[:limit]

    if not sessions:
        await send_hybrid_response(ctx, f"No tracked voice sessions found for **{target.display_name}**.")
        return

    lines = [
        f"**Recent voice sessions for {target.display_name}**",
        f"Timezone: `{format_timezone_label()}`",
    ]

    for index, session in enumerate(sessions, start=1):
        channel_label = resolve_voice_channel_label(
            guild,
            session["channel_id"],
            session["channel_name"],
        )
        started_ts = int(session["started_at"].timestamp())
        if session["active"]:
            lines.append(
                f"{index}. `{channel_label}` | start <t:{started_ts}:F> | "
                f"ongoing | elapsed {format_duration(session['duration_seconds'])}"
            )
            continue

        ended_ts = int(session["ended_at"].timestamp())
        lines.append(
            f"{index}. `{channel_label}` | <t:{started_ts}:F> -> <t:{ended_ts}:F> | "
            f"{format_duration(session['duration_seconds'])}"
        )

    await send_hybrid_response(ctx, "\n".join(lines))


@bot.hybrid_command(
    name='voice_daily',
    description='Show per-day voice totals for a server member'
)
async def voice_daily(
    ctx: commands.Context,
    member: Optional[discord.Member] = None,
    days: int = 7,
):
    await defer_hybrid_command(ctx)

    guild = ctx.guild
    if guild is None:
        await send_hybrid_response(ctx, "This command can only be used inside a server.", ephemeral=True)
        return

    target = member or ctx.author
    if not isinstance(target, discord.Member):
        await send_hybrid_response(ctx, "Could not resolve the target member.", ephemeral=True)
        return

    days = normalize_report_limit(days, default=7, minimum=1, maximum=31)
    breakdown = build_daily_voice_breakdown(guild.id, target.id, days=days)

    lines = [
        f"**Daily voice totals for {target.display_name}**",
        f"Timezone: `{format_timezone_label()}`",
    ]
    for day_label, total_seconds in breakdown:
        lines.append(f"`{day_label}`: {format_duration(total_seconds)}")

    await send_hybrid_response(ctx, "\n".join(lines))


# ===============================
#   Misc commands
# ===============================

@bot.hybrid_command(name='ping', description='Test if the bot is responsive')
async def ping(ctx: commands.Context):
    await send_hybrid_response(ctx, 'Pong!')


@bot.command(name='sync')
async def sync_commands(ctx: commands.Context):
    """Copy global app commands into this guild and sync them immediately."""
    if ctx.guild is None:
        await ctx.send("This command can only be used inside a server.")
        return
    try:
        if USE_GUILD_ONLY_APP_COMMANDS:
            cleared_count = await clear_remote_global_commands_preserving_local_tree()
            print(f"Cleared global app commands before guild sync. Remaining global count: {cleared_count}")
        bot.tree.clear_commands(guild=ctx.guild)
        bot.tree.copy_global_to(guild=ctx.guild)
        synced = await bot.tree.sync(guild=ctx.guild)
        await ctx.send(
            f"Copied global commands and synced {len(synced)} command(s) to this server."
        )
    except Exception as e:
        await ctx.send(f"Failed to sync commands: {e}")


# -------- Entry point --------

if __name__ == "__main__":
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        print("Please set DISCORD_TOKEN in your .env file")
    else:
        bot.run(token)
