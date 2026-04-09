import discord
from discord.ext import commands
import random
import asyncio
from datetime import datetime, timedelta, timezone
import os
import re
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

# text_channel_id -> shuffle state
active_shuffles: dict[int, dict] = {}
triggered_event_occurrences: set[tuple[int, int]] = set()
triggering_event_occurrences: set[tuple[int, int]] = set()
scheduled_event_tasks: dict[tuple[int, int], asyncio.Task] = {}
planned_event_messages: dict[tuple[int, int], tuple[int, int]] = {}
PROCESS_STARTED_AT = datetime.now(timezone.utc)

REMOVE_DELAY = 30  # seconds after leaving the voice channel
STARTUP_CLOCK_SKEW = timedelta(seconds=5)
PLANNED_SHUFFLE_PREFIX = "⏳ **Shuffle planned:**"
SHUFFLE_LIST_PREFIX = "🎲 **Shuffled list:**"
FROZEN_EVENT_MARKER = "[замороженно]"

# -------- Role permissions --------

RELIABLE_ROLE_NAME = "Товарищ"
_reliable_role_id_env = os.getenv("RELIABLE_ROLE_ID")
RELIABLE_ROLE_ID = int(_reliable_role_id_env) if _reliable_role_id_env else None


def has_reliable_role(member: discord.Member) -> bool:
    """Check if member has the 'reliable' role (by ID if set, otherwise by name)."""
    if RELIABLE_ROLE_ID is not None:
        return any(role.id == RELIABLE_ROLE_ID for role in member.roles)
    required_name = RELIABLE_ROLE_NAME.casefold()
    return any(role.name.casefold() == required_name for role in member.roles)


def is_frozen_event(event: discord.ScheduledEvent) -> bool:
    """Frozen events must not auto-post notices or run shuffles."""
    return FROZEN_EVENT_MARKER in event.name.casefold()


# -------- Events --------

@bot.event
async def on_ready():
    print(f'{bot.user} connected. Guilds: {len(bot.guilds)}')
    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} command(s) globally')
    except Exception as e:
        print(f'Failed to sync commands: {e}')

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

                text_channel = pick_text_channel_for_voice(voice_channel)
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


@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return
    await bot.process_commands(message)


@bot.event
async def on_guild_scheduled_event_update(before, after):
    """Auto-trigger shuffle when a voice scheduled event becomes active."""
    before_key = event_occurrence_key(before.id, before.start_time)
    after_key = event_occurrence_key(after.id, after.start_time)
    if before_key != after_key:
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
            text_channel = pick_text_channel_for_voice(voice_channel)
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

            text_channel = guild_obj.get_channel(text_channel_id)
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

        text_channel = preferred_text_channel or pick_text_channel_for_voice(voice_channel)
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


def build_content(guild: discord.Guild, order, labels=None) -> str:
    """Build the visual text of the shuffled list, using custom labels if present."""
    if labels is None:
        labels = {}

    lines = [SHUFFLE_LIST_PREFIX]
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
    return "\n".join(lines)


async def update_shuffle_message(state: dict) -> None:
    """Redraw the message with the current shuffle order."""
    guild = bot.get_guild(state["guild_id"])
    if not guild:
        return

    content = build_content(guild, state["order"], state.get("labels"))
    try:
        await state["message"].edit(content=content)
    except discord.NotFound:
        # message deleted -> clean up state
        active_shuffles.pop(state["text_channel_id"], None)
    except Exception as e:
        print(f"Error editing message: {e}")


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
) -> None:
    """
    Common shuffle logic:
    - used in the /shuffle command
    - and when a scheduled event auto-starts.
    """
    members = [m for m in voice_channel.members if not m.bot]

    if not members:
        if existing_message is not None:
            try:
                await existing_message.edit(content="No members in the voice channel!")
            except discord.NotFound:
                await text_channel.send("No members in the voice channel!")
        else:
            await text_channel.send("No members in the voice channel!")
        return

    order = [m.id for m in members]
    random.shuffle(order)

    labels: dict[int, str] = {}  # labels for ✨ / 🌌

    content = build_content(guild, order, labels)
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
    }

    # auto cleanup after 5 minutes
    asyncio.create_task(cleanup_old_shuffle(text_channel.id, 300))


async def cleanup_old_shuffle(channel_id: int, delay_sec: int) -> None:
    """Cleanup shuffle state after a delay and cancel all pending removal timers."""
    await asyncio.sleep(delay_sec)
    state = active_shuffles.pop(channel_id, None)
    if state:
        for task in state["pending_removals"].values():
            task.cancel()


async def defer_hybrid_command(ctx: commands.Context) -> None:
    """Acknowledge slash invocations that send their real output directly to the channel."""
    if ctx.interaction is None or ctx.interaction.response.is_done():
        return

    await ctx.defer(ephemeral=True)


async def finish_hybrid_command(ctx: commands.Context, content: str) -> None:
    """Finish a deferred slash invocation with an ephemeral confirmation."""
    if ctx.interaction is None:
        return

    try:
        if ctx.interaction.response.is_done():
            await ctx.interaction.followup.send(content, ephemeral=True)
        else:
            await ctx.send(content, ephemeral=True)
    except Exception as e:
        print(f"Error sending command followup: {e}")


@bot.event
async def on_voice_state_update(member: discord.Member, before, after) -> None:
    """
    React to members joining/leaving tracked voice channels:
    - schedule removal when they leave
    - cancel removal and update labels when they return.
    """
    for text_channel_id, state in list(active_shuffles.items()):
        guild = bot.get_guild(state["guild_id"])
        if not guild or guild.id != member.guild.id:
            continue

        voice_channel_id = state["voice_channel_id"]

        # --- MEMBER JOINED the tracked voice channel ---
        if (
            after.channel is not None
            and after.channel.id == voice_channel_id
            and (before.channel is None or before.channel.id != voice_channel_id)
        ):
            pending = state["pending_removals"].get(member.id)

            # They had a removal timer -> they returned "in time"
            if pending:
                pending.cancel()
                state["pending_removals"].pop(member.id, None)

                # If they are still in the list -> mark as returned with 🌌
                if member.id in state["order"]:
                    labels = state.setdefault("labels", {})
                    labels[member.id] = f"{member.display_name} 🌌"
                    state["ever_seen"].add(member.id)
                    await update_shuffle_message(state)
            else:
                # No timer: either they were removed earlier or they are a completely new member
                if member.id not in state["order"]:
                    state["order"].append(member.id)
                    labels = state.setdefault("labels", {})

                    # If they were in the list before -> this is a "return" -> 🌌
                    if member.id in state.get("ever_seen", set()):
                        labels[member.id] = f"{member.display_name} 🌌"
                    else:
                        # Completely new member -> ✨
                        labels[member.id] = f"{member.display_name} ✨"
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
    linked_text_channel = pick_text_channel_for_voice(voice_channel)
    shuffle_target = linked_text_channel or text_channel
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
async def shuffle_voice_members(ctx: commands.Context):
    await defer_hybrid_command(ctx)

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("You need to be in a voice channel to use this command!")
        return

    voice_channel = ctx.author.voice.channel
    await start_shuffle_for_channel(ctx.guild, voice_channel, ctx.channel)
    await finish_hybrid_command(ctx, "Shuffle posted in this channel.")


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

    linked_text_channel = pick_text_channel_for_voice(voice_channel)
    shuffle_target = linked_text_channel or ctx.channel

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
#   Misc commands
# ===============================

@bot.hybrid_command(name='ping', description='Test if the bot is responsive')
async def ping(ctx: commands.Context):
    await ctx.send('Pong!')


@bot.command(name='sync')
async def sync_commands(ctx: commands.Context):
    """Sync application commands only to this guild."""
    if ctx.guild is None:
        await ctx.send("This command can only be used inside a server.")
        return
    try:
        synced = await bot.tree.sync(guild=ctx.guild)
        await ctx.send(f"Synced {len(synced)} command(s) to this server.")
    except Exception as e:
        await ctx.send(f"Failed to sync commands: {e}")


# -------- Entry point --------

if __name__ == "__main__":
    token = os.getenv('DISCORD_TOKEN')
    if not token:
        print("Please set DISCORD_TOKEN in your .env file")
    else:
        bot.run(token)
