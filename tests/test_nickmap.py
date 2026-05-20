import atexit
import json
import os
import shutil
import sys
import tempfile
import types
import unittest


_IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="reshuffle-test-data-")
os.environ["RESHUFFLE_DATA_DIR"] = _IMPORT_DATA_DIR
atexit.register(lambda: shutil.rmtree(_IMPORT_DATA_DIR, ignore_errors=True))


def _identity_decorator(*_args, **_kwargs):
    if len(_args) == 1 and callable(_args[0]) and not _kwargs:
        return _args[0]

    def decorator(func):
        return func

    return decorator


def _install_dotenv_stub_if_needed():
    try:
        import dotenv  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_stub


def _install_discord_stub_if_needed():
    try:
        import discord  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _Dummy:
        def __init__(self, *args, **kwargs):
            pass

        def __getattr__(self, _name):
            return _Dummy()

        def __call__(self, *args, **kwargs):
            return _Dummy()

    class _DummyType:
        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__()

        def __init__(self, *args, **kwargs):
            pass

    class _DummyIntents:
        @classmethod
        def default(cls):
            return cls()

    class _DummyTree:
        command = staticmethod(_identity_decorator)
        error = staticmethod(_identity_decorator)

        def clear_commands(self, *args, **kwargs):
            pass

        def copy_global_to(self, *args, **kwargs):
            pass

        async def sync(self, *args, **kwargs):
            return []

        def get_commands(self, *args, **kwargs):
            return []

        def add_command(self, *args, **kwargs):
            pass

    class _DummyGroup:
        command = staticmethod(_identity_decorator)

        def __call__(self, *args, **kwargs):
            return None

    class _DummyBot:
        def __init__(self, *args, **kwargs):
            self.tree = _DummyTree()
            self.guilds = []
            self.user = None

        event = staticmethod(lambda func: func)
        command = staticmethod(_identity_decorator)
        hybrid_command = staticmethod(_identity_decorator)

        @staticmethod
        def hybrid_group(*args, **kwargs):
            def decorator(_func):
                return _DummyGroup()

            return decorator

        def get_channel(self, *args, **kwargs):
            return None

        async def fetch_channel(self, *args, **kwargs):
            return None

        def run(self, *args, **kwargs):
            pass

        async def process_commands(self, *args, **kwargs):
            pass

        def get_guild(self, *args, **kwargs):
            return None

    discord_stub = types.ModuleType("discord")
    discord_stub.Intents = _DummyIntents
    discord_stub.Member = type("Member", (), {})
    discord_stub.Guild = type("Guild", (), {})
    discord_stub.VoiceChannel = type("VoiceChannel", (), {})
    discord_stub.StageChannel = type("StageChannel", (), {})
    discord_stub.TextChannel = type("TextChannel", (), {})
    discord_stub.ScheduledEvent = type("ScheduledEvent", (), {})
    discord_stub.Message = type("Message", (), {})
    discord_stub.Interaction = type("Interaction", (), {})
    discord_stub.Object = _DummyType
    discord_stub.File = _DummyType
    discord_stub.Forbidden = type("Forbidden", (Exception,), {})
    discord_stub.NotFound = type("NotFound", (Exception,), {})
    discord_stub.HTTPException = type("HTTPException", (Exception,), {})
    discord_stub.MessageType = types.SimpleNamespace(thread_created=object())
    discord_stub.EntityType = types.SimpleNamespace(voice=object())
    discord_stub.EventStatus = types.SimpleNamespace(
        active=object(),
        completed=object(),
        scheduled=object(),
    )
    discord_stub.PrivacyLevel = types.SimpleNamespace(guild_only=object())
    discord_stub.AllowedMentions = types.SimpleNamespace(none=lambda: None)
    discord_stub.utils = types.SimpleNamespace(
        escape_markdown=lambda value: value,
        sleep_until=lambda *_args, **_kwargs: None,
    )
    discord_stub.abc = types.SimpleNamespace(
        GuildChannel=type("GuildChannel", (), {}),
        Messageable=type("Messageable", (), {}),
    )
    discord_stub.ui = types.SimpleNamespace(
        Modal=_DummyType,
        TextInput=_DummyType,
    )
    discord_stub.app_commands = types.SimpleNamespace(
        describe=_identity_decorator,
        AppCommandError=type("AppCommandError", (Exception,), {}),
    )

    commands_stub = types.ModuleType("discord.ext.commands")
    commands_stub.Bot = _DummyBot
    commands_stub.Context = type("Context", (), {})
    commands_stub.CommandError = type("CommandError", (Exception,), {})
    commands_stub.CommandNotFound = type("CommandNotFound", (commands_stub.CommandError,), {})

    ext_stub = types.ModuleType("discord.ext")
    ext_stub.commands = commands_stub

    sys.modules["discord"] = discord_stub
    sys.modules["discord.ext"] = ext_stub
    sys.modules["discord.ext.commands"] = commands_stub


_install_dotenv_stub_if_needed()
_install_discord_stub_if_needed()

import Shuffle  # noqa: E402


class NickmapPersistenceTests(unittest.TestCase):
    def test_normalize_nickmap_key_accepts_short_forms(self):
        self.assertEqual(Shuffle.normalize_nickmap_key("123456789"), "id:123456789")
        self.assertEqual(Shuffle.normalize_nickmap_key("@tg_user"), "u:tg_user")
        self.assertEqual(Shuffle.normalize_nickmap_key("u:tg_user"), "u:tg_user")

    def test_save_and_load_nickmap_file_normalizes_records(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "nickmap.json")
            saved = Shuffle.save_nickmap_file(
                {
                    "mappings": {
                        "123": {"dc_name": "Raw Numeric"},
                        "u:bob": {"dc_name": "Bob"},
                        "id:456": "Alice",
                    }
                },
                path,
            )

            self.assertEqual(list(saved["mappings"].keys()), ["id:123", "id:456", "u:bob"])
            self.assertEqual(saved["mappings"]["id:123"], {"dc_name": "Raw Numeric"})
            self.assertEqual(saved["mappings"]["id:456"], {"dc_name": "Alice"})

            loaded = Shuffle.load_nickmap_file(path)
            self.assertEqual(loaded, saved)

            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            self.assertEqual(raw, saved)
            self.assertFalse(
                any(name.endswith(".tmp") for name in os.listdir(tmp_dir)),
                "atomic temp files should not be left behind",
            )

    def test_atomic_write_text_replaces_existing_file(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "nickmap.tengo")

            Shuffle.atomic_write_text(path, "first\n")
            Shuffle.atomic_write_text(path, "second\n")

            with open(path, "r", encoding="utf-8") as fh:
                self.assertEqual(fh.read(), "second\n")
            self.assertFalse(any(name.endswith(".tmp") for name in os.listdir(tmp_dir)))

    def test_refresh_existing_nickmap_files_normalizes_and_regenerates(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_json_path = Shuffle.NICKMAP_JSON_PATH
            original_tengo_path = Shuffle.NICKMAP_TENGO_PATH
            Shuffle.NICKMAP_JSON_PATH = os.path.join(tmp_dir, "nickmap.json")
            Shuffle.NICKMAP_TENGO_PATH = os.path.join(tmp_dir, "nickmap.tengo")
            try:
                with open(Shuffle.NICKMAP_JSON_PATH, "w", encoding="utf-8") as fh:
                    json.dump({"mappings": {"123": "Alice"}}, fh)

                self.assertEqual(Shuffle.refresh_existing_nickmap_files(), 1)

                with open(Shuffle.NICKMAP_JSON_PATH, "r", encoding="utf-8") as fh:
                    raw_json = json.load(fh)
                with open(Shuffle.NICKMAP_TENGO_PATH, "r", encoding="utf-8") as fh:
                    raw_tengo = fh.read()

                self.assertEqual(list(raw_json["mappings"].keys()), ["id:123"])
                self.assertIn('"id:123": "Alice"', raw_tengo)
            finally:
                Shuffle.NICKMAP_JSON_PATH = original_json_path
                Shuffle.NICKMAP_TENGO_PATH = original_tengo_path

    def test_persistent_shuffle_exclusions_are_saved_atomically(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_path = Shuffle.PERSISTENT_EXCLUSIONS_FILE
            original_exclusions = {
                guild_id: set(user_ids)
                for guild_id, user_ids in Shuffle.persistent_shuffle_exclusions.items()
            }
            Shuffle.PERSISTENT_EXCLUSIONS_FILE = os.path.join(
                tmp_dir,
                "persistent_shuffle_exclusions.json",
            )
            try:
                Shuffle.persistent_shuffle_exclusions.clear()
                Shuffle.persistent_shuffle_exclusions[100] = {3, 1, 2}

                Shuffle.save_persistent_shuffle_exclusions()

                self.assertEqual(
                    Shuffle.load_persistent_shuffle_exclusions(),
                    {100: {1, 2, 3}},
                )
                self.assertFalse(
                    any(name.endswith(".tmp") for name in os.listdir(tmp_dir)),
                    "atomic temp files should not be left behind",
                )
            finally:
                Shuffle.PERSISTENT_EXCLUSIONS_FILE = original_path
                Shuffle.persistent_shuffle_exclusions.clear()
                Shuffle.persistent_shuffle_exclusions.update(original_exclusions)

    def test_event_auto_shuffle_targets_are_saved_atomically(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            original_path = Shuffle.EVENT_AUTO_TARGETS_FILE
            original_targets = dict(Shuffle.event_auto_shuffle_targets)
            Shuffle.EVENT_AUTO_TARGETS_FILE = os.path.join(
                tmp_dir,
                "event_auto_shuffle_targets.json",
            )
            try:
                Shuffle.event_auto_shuffle_targets.clear()

                self.assertIsNone(Shuffle.set_event_auto_shuffle_target(200, 100))
                self.assertEqual(Shuffle.load_event_auto_shuffle_targets(), {200: 100})
                self.assertEqual(Shuffle.set_event_auto_shuffle_target(200, 101), 100)
                self.assertEqual(Shuffle.load_event_auto_shuffle_targets(), {200: 101})
                self.assertEqual(Shuffle.delete_event_auto_shuffle_target(200), 101)
                self.assertEqual(Shuffle.load_event_auto_shuffle_targets(), {})
                self.assertFalse(
                    any(name.endswith(".tmp") for name in os.listdir(tmp_dir)),
                    "atomic temp files should not be left behind",
                )
            finally:
                Shuffle.EVENT_AUTO_TARGETS_FILE = original_path
                Shuffle.event_auto_shuffle_targets.clear()
                Shuffle.event_auto_shuffle_targets.update(original_targets)

    def test_generate_nickmap_tengo_is_stable_and_guarded(self):
        first = Shuffle.generate_nickmap_tengo(
            {
                "mappings": {
                    "u:zeta": {"dc_name": "Zed"},
                    "id:123": {"dc_name": 'Alice "A"'},
                }
            }
        )
        second = Shuffle.generate_nickmap_tengo(
            {
                "mappings": {
                    "id:123": {"dc_name": 'Alice "A"'},
                    "u:zeta": {"dc_name": "Zed"},
                }
            }
        )

        self.assertEqual(first, second)
        self.assertIn('if msgAccount == "telegram.mytelegram" {', first)
        self.assertLess(first.index('"id:123"'), first.index('"u:zeta"'))
        self.assertIn('"id:123": "Alice \\"A\\""', first)
        self.assertIn('msgUsername = mapped', first)

    def test_trusted_role_default_id_can_manage_nickmap(self):
        member = types.SimpleNamespace(
            guild_permissions=types.SimpleNamespace(administrator=False),
            roles=[types.SimpleNamespace(id=1434300421647761489, name="anything")],
        )

        self.assertEqual(Shuffle.TRUSTED_ROLE_ID, 1434300421647761489)
        self.assertTrue(Shuffle.has_trusted_role(member))
        self.assertTrue(Shuffle.member_has_nickmap_access(member))


if __name__ == "__main__":
    unittest.main()
