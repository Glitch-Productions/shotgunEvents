"""
Test harness for the event daemon engine.

The engine runs against a fake Flow Production Tracking server that holds an
in-memory event log, a fake clock and a fake sleep, so a whole event loop pass
runs in microseconds with no network access.
"""

import configparser
import dataclasses
import datetime
import logging
import pickle
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import shotgunEventDaemon as daemon  # noqa: E402

START = datetime.datetime(2026, 9, 30, 22, 0, 0)

PLUGIN_SOURCE = """
PROCESSED = []


def registerCallbacks(reg):
    reg.registerCallback("script", "key", record)


def record(sg, logger, event, args):
    PROCESSED.append(event["id"])
"""


@dataclasses.dataclass
class FindCall:
    pass_number: int
    filters: list
    limit: int
    rows: int


class FakeShotgun:
    """
    Event log of a fake site.

    ``find`` honours the filters the daemon uses on EventLogEntry: ``id
    greater_than``, ``id in``, ascending or descending order on id, and limit.
    """

    def __init__(self, clock):
        self._clock = clock
        self._events = {}
        self.calls = []
        self.pass_number = 0
        self.session_uuid = None

    def add_event(self, event_id, created_at=None):
        if created_at is None:
            created_at = self._clock.now()
        self._events[event_id] = {
            "type": "EventLogEntry",
            "id": event_id,
            "event_type": "Shotgun_Note_Change",
            "attribute_name": "content",
            "meta": {},
            "entity": {"type": "Note", "id": event_id},
            "user": {"type": "HumanUser", "id": 1},
            "project": {"type": "Project", "id": 1},
            "session_uuid": None,
            "created_at": created_at,
        }

    def add_events(self, event_ids):
        for event_id in event_ids:
            self.add_event(event_id)

    def find(
        self,
        entity_type,
        filters,
        fields=None,
        order=None,
        filter_operator=None,
        limit=0,
    ):
        if entity_type != "EventLogEntry" or filter_operator is not None:
            raise ValueError(f"Unsupported query: {entity_type} {filter_operator}")

        rows = [
            event
            for event in self._events.values()
            if all(self._match(event, f) for f in filters)
        ]
        descending = bool(order) and order[0]["direction"] == "desc"
        rows.sort(key=lambda event: event["id"], reverse=descending)
        if limit:
            rows = rows[:limit]
        self.calls.append(FindCall(self.pass_number, filters, limit, len(rows)))
        return [dict(row) for row in rows]

    def find_one(self, entity_type, filters, fields=None, order=None):
        rows = self.find(entity_type, filters, fields, order, limit=1)
        return rows[0] if rows else None

    def set_session_uuid(self, session_uuid):
        self.session_uuid = session_uuid

    @staticmethod
    def _match(event, filter_):
        field, operator, value = filter_
        if field != "id":
            raise ValueError(f"Unsupported filter field: {field}")
        if operator == "greater_than":
            return event["id"] > value
        if operator == "in":
            return event["id"] in value
        raise ValueError(f"Unsupported filter operator: {operator}")


class FakeClock:
    """
    Stands in for ``datetime.datetime`` inside the daemon module.
    """

    def __init__(self, start):
        self.current = start

    def now(self, tz=None):
        return self.current

    def advance(self, seconds):
        self.current += datetime.timedelta(seconds=seconds)


class Harness:
    """
    An engine wired to the fakes, plus the means to run event loop passes.
    """

    def __init__(self, engine, shotgun, clock, sleeps):
        self.engine = engine
        self.shotgun = shotgun
        self.clock = clock
        self.sleeps = sleeps
        self.passes = 0

    def start(self):
        """
        Load the plugins and the state, as L{daemon.Engine.start} does.
        """
        for collection in self.engine._pluginCollections:
            collection.load()
        self.engine._loadEventIdData()

    def run(self, count, before_pass=None, fetch=None):
        """
        Run the real event loop for exactly ``count`` passes.

        @param before_pass: Called with the pass number before each fetch.
        @param fetch: Replaces the engine fetch, to feed chosen events.
        """
        engine = self.engine
        get_new_events = fetch or engine._getNewEvents
        last_pass = self.passes + count

        def counted_get_new_events():
            self.passes += 1
            self.shotgun.pass_number = self.passes
            if before_pass is not None:
                before_pass(self.passes)
            if self.passes >= last_pass:
                engine.stop()
            return get_new_events()

        engine._getNewEvents = counted_get_new_events
        engine._continue = True
        try:
            engine._mainLoop()
        finally:
            del engine._getNewEvents

    @property
    def plugins(self):
        return {
            plugin.getName(): plugin
            for collection in self.engine._pluginCollections
            for plugin in collection
        }

    def processed(self, plugin_name):
        return sys.modules[plugin_name].PROCESSED

    def calls_in_pass(self, pass_number):
        return [call for call in self.shotgun.calls if call.pass_number == pass_number]

    def saved_state(self):
        with open(self.engine.config.getEventIdFile(), "rb") as fh:
            return pickle.load(fh)


@pytest.fixture
def clock(monkeypatch):
    fake_clock = FakeClock(START)
    monkeypatch.setattr(
        daemon,
        "datetime",
        types.SimpleNamespace(datetime=fake_clock, timedelta=datetime.timedelta),
    )
    return fake_clock


@pytest.fixture
def make_harness(tmp_path, monkeypatch, clock):
    """
    Build a L{Harness}. Its loggers and plugin modules are released on teardown.
    """
    shotgun = FakeShotgun(clock)
    monkeypatch.setattr(daemon.sg, "Shotgun", lambda *args, **kwargs: shotgun)

    sleeps = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        clock.advance(seconds)

    monkeypatch.setattr(daemon, "time", types.SimpleNamespace(sleep=fake_sleep))

    loaded_plugin_names = []

    # The plugin path is relative, as in a state file written by a daemon that
    # was configured with "paths = plugins".
    monkeypatch.chdir(tmp_path)

    def factory(plugin_names=("plugin_a", "plugin_b"), batch_size=500, state=None):
        """
        @param state: Bytes of an existing eventIdFile.
        """
        plugins_dir = tmp_path / "plugins"
        plugins_dir.mkdir()
        for name in plugin_names:
            (plugins_dir / f"{name}.py").write_text(PLUGIN_SOURCE)
            loaded_plugin_names.append(name)

        event_id_file = tmp_path / "shotgunEventDaemon.id"
        if state is not None:
            event_id_file.write_bytes(state)

        config = configparser.ConfigParser(interpolation=None)
        config["daemon"] = {
            "pidFile": str(tmp_path / "shotgunEventDaemon.pid"),
            "eventIdFile": str(event_id_file),
            "logMode": "1",
            "logPath": str(tmp_path / "logs"),
            "logFile": "shotgunEventDaemon",
            "logging": str(logging.DEBUG),
            "timing_log": "off",
            "max_conn_retries": "5",
            "conn_retry_sleep": "60",
            "fetch_interval": "1",
            "max_event_batch_size": str(batch_size),
        }
        config["plugins"] = {"paths": "plugins"}
        config["shotgun"] = {
            "server": "https://fake.invalid",
            "name": "daemon",
            "key": "not-a-key",
            "use_session_uuid": "False",
        }
        config["emails"] = {"enabled": "False"}
        config_path = tmp_path / "shotgunEventDaemon.conf"
        with open(config_path, "w") as fh:
            config.write(fh)

        engine = daemon.Engine(str(config_path))
        return Harness(engine, shotgun, clock, sleeps)

    yield factory

    logger_names = ["engine"] + [f"plugin.{name}" for name in loaded_plugin_names]
    for logger_name in logger_names:
        logger = logging.getLogger(logger_name)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    for name in loaded_plugin_names:
        sys.modules.pop(name, None)
