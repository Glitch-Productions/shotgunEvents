"""
Event loop behaviour around event id gaps (the backlog).

Every test drives the real ``Engine._mainLoop`` against the fake site and
clock from ``conftest.py``.
"""

import base64
import collections
import datetime
import pickle

from conftest import START

PLUGINS = ("plugin_a", "plugin_b")
BACKLOG_TIMEOUT = datetime.timedelta(minutes=5)

# eventIdFile written by the untouched v1.0.0-glitch1 engine (upstream cc241fa)
# with "paths = plugins": both plugins at event 1004, with event 1002 in the
# backlog until 2026-09-30 22:05:00.
V1_0_0_GLITCH1_STATE = base64.b64decode(
    "gAWVegAAAAAAAAB9lIwHcGx1Z2luc5R9lCiMCHBsdWdpbl9hlE3sA32UTeoDjAhkYXRldGltZZSM"
    "CGRhdGV0aW1llJOUQwoH6gkeFgUAAAAAlIWUUpRzhpSMCHBsdWdpbl9ilE3sA32UTeoDaAdDCgfq"
    "CR4WBQAAAACUhZRSlHOGlHVzLg=="
)


def too_old_records(caplog):
    return [r for r in caplog.records if "too old" in r.getMessage()]


def messages(caplog, text):
    return [r for r in caplog.records if r.getMessage() == text]


def started_harness(make_harness, **kwargs):
    harness = make_harness(**kwargs)
    harness.shotgun.add_event(1000)
    harness.start()
    return harness


def test_late_event_is_processed_once_and_nothing_is_read_again(make_harness, caplog):
    harness = started_harness(make_harness)
    shotgun = harness.shotgun
    gap_time = harness.clock.now()
    shotgun.add_events([1001, 1002, 1003, 1004, 1006, 1007, 1008, 1009, 1010])

    harness.run(1)
    for plugin in harness.plugins.values():
        assert plugin.getState() == (1010, {1005: gap_time + BACKLOG_TIMEOUT})

    # Quiet site: each pass asks for new events and for the backlogged id only.
    harness.run(10)
    for pass_number in range(2, 12):
        calls = harness.calls_in_pass(pass_number)
        assert [call.filters for call in calls] == [
            [["id", "greater_than", 1010]],
            [["id", "in", [1005]]],
        ]
        assert [call.rows for call in calls] == [0, 0]

    # The missing event shows up late, with new events after it.
    shotgun.add_event(1005, created_at=gap_time)
    shotgun.add_events([1011, 1012])
    harness.run(5)

    expected = [1001, 1002, 1003, 1004, 1006, 1007, 1008, 1009, 1010]
    expected += [1005, 1011, 1012]
    for name in PLUGINS:
        assert harness.processed(name) == expected
        assert harness.plugins[name].getState() == (1012, {})
    assert len(messages(caplog, "Processed id 1005 from backlog.")) == len(PLUGINS)

    # Every event was read from the site exactly once.
    rows = [call.rows for call in shotgun.calls if call.pass_number]
    assert sum(rows) == len(expected)
    # Once resolved, the backlogged id is no longer asked for.
    for pass_number in range(13, 17):
        assert len(harness.calls_in_pass(pass_number)) == 1
    assert too_old_records(caplog) == []


def test_expired_backlog_id_is_warned_once_and_stays_expired(make_harness, caplog):
    harness = started_harness(make_harness)
    shotgun = harness.shotgun
    gap_time = harness.clock.now()
    shotgun.add_events([1001, 1003])
    harness.run(1)
    for plugin in harness.plugins.values():
        assert plugin.getState() == (1003, {1002: gap_time + BACKLOG_TIMEOUT})

    # The gap is never filled and the site stays quiet: no event is processed,
    # so only the expiry itself can save the state.
    harness.clock.advance(BACKLOG_TIMEOUT.total_seconds())
    harness.run(30)

    warning = "Timeout elapsed on backlog event id 1002."
    assert len(messages(caplog, warning)) == len(PLUGINS)
    for plugin in harness.plugins.values():
        assert plugin.getState() == (1003, {})
    assert harness.saved_state() == {"plugins": {name: (1003, {}) for name in PLUGINS}}
    for pass_number in range(3, 32):
        filters = [call.filters for call in harness.calls_in_pass(pass_number)]
        assert filters == [[["id", "greater_than", 1003]]]

    # As before the change, an event that shows up after its backlog entry
    # expired is not processed.
    shotgun.add_event(1002, created_at=gap_time)
    harness.run(3)
    for name in PLUGINS:
        assert harness.processed(name) == [1001, 1003]
    assert len(messages(caplog, warning)) == len(PLUGINS)


def test_more_than_a_batch_after_a_gap_keeps_advancing(make_harness, caplog):
    harness = started_harness(make_harness, batch_size=500)
    shotgun = harness.shotgun
    new_ids = [1001] + list(range(1003, 2201))
    shotgun.add_events(new_ids)

    last_ids = []

    def record_last_id(pass_number):
        last_ids.append(harness.plugins["plugin_a"].getState()[0])

    harness.run(4, before_pass=record_last_id)

    assert last_ids == [1000, 1501, 2001, 2200]
    for name in PLUGINS:
        assert harness.processed(name) == new_ids
        assert harness.plugins[name].getState() == (
            2200,
            {1002: START + BACKLOG_TIMEOUT},
        )
    # Full batches of new events skip the sleep; the rest do not.
    assert harness.sleeps == [1, 1]
    assert too_old_records(caplog) == []


def test_full_batch_without_progress_sleeps(make_harness, caplog):
    harness = started_harness(make_harness, batch_size=500)
    shotgun = harness.shotgun
    shotgun.add_events(range(1001, 1501))
    harness.run(1)
    assert harness.sleeps == []

    # Feed the shape of the old re-read: the same full batch of events that
    # were all processed already. The loop must wait between such passes.
    stale = shotgun.find("EventLogEntry", [["id", "greater_than", 1000]], limit=500)
    harness.run(3, fetch=lambda: [dict(event) for event in stale])

    assert harness.sleeps == [1, 1, 1]
    for name in PLUGINS:
        assert harness.processed(name) == list(range(1001, 1501))
    # One summary line per plugin and pass, not one line per event.
    assert len(too_old_records(caplog)) == 3 * len(PLUGINS)


def test_too_old_events_are_logged_once_per_pass(make_harness, caplog):
    state = {"plugins": {"plugin_a": (1500, {}), "plugin_b": (1000, {})}}
    harness = make_harness(state=pickle.dumps(state))
    harness.shotgun.add_events(range(1001, 1601))
    harness.start()

    harness.run(3)

    records = too_old_records(caplog)
    assert [(r.name, r.getMessage()) for r in records] == [
        (
            "plugin.plugin_a",
            "Skipped 500 too old event(s) (ids 1001 to 1500). "
            "Last event processed was (1500).",
        )
    ]
    assert harness.processed("plugin_a") == list(range(1501, 1601))
    assert harness.processed("plugin_b") == list(range(1001, 1601))


def test_flood_episode_shape(make_harness, caplog):
    """
    Replay the shape of the 2026-09-30 22:49 UTC episode: a gap that is never
    filled, steady traffic for five minutes, and a burst of more than one
    batch in the middle of it.
    """
    harness = started_harness(make_harness, batch_size=500)
    shotgun = harness.shotgun
    next_ids = collections.deque(range(1003, 3000))
    added = [1001]
    shotgun.add_event(1001)  # 1002 never shows up

    def add_traffic(pass_number):
        count = 1200 if pass_number == 100 else 2
        for _ in range(count):
            added.append(next_ids.popleft())
            shotgun.add_event(added[-1])

    harness.run(320, before_pass=add_traffic)

    for name in PLUGINS:
        assert harness.processed(name) == added
    assert too_old_records(caplog) == []
    warning = "Timeout elapsed on backlog event id 1002."
    assert len(messages(caplog, warning)) == len(PLUGINS)

    # Each event is read once; no pass reads more than a batch.
    rows = [call.rows for call in shotgun.calls if call.pass_number]
    assert sum(rows) == len(added)
    assert max(rows) == 500
    # The burst is cleared by consecutive full batches, without waiting
    # between them; every other pass waits.
    burst_rows = [
        sum(call.rows for call in harness.calls_in_pass(n)) for n in range(99, 104)
    ]
    assert burst_rows == [2, 500, 500, 204, 2]
    assert len(harness.sleeps) == 320 - 2


def test_state_file_from_v1_0_0_glitch1_loads(make_harness, caplog):
    harness = make_harness(state=V1_0_0_GLITCH1_STATE)
    harness.start()

    expiry = datetime.datetime(2026, 9, 30, 22, 5)
    for plugin in harness.plugins.values():
        assert plugin.getState() == (1004, {1002: expiry})

    # The backlogged id from the old file is fetched by id and processed once.
    harness.clock.advance(60)
    harness.shotgun.add_event(1002, created_at=START)
    harness.shotgun.add_event(1005)
    harness.run(3)

    for name in PLUGINS:
        assert harness.processed(name) == [1002, 1005]
    assert harness.saved_state() == {"plugins": {name: (1005, {}) for name in PLUGINS}}


def test_state_file_from_moved_plugin_path_loads(make_harness):
    old_state = pickle.loads(V1_0_0_GLITCH1_STATE)
    state = {"/old/location/plugins": old_state["plugins"]}
    harness = make_harness(state=pickle.dumps(state))
    harness.shotgun.add_event(1004)
    harness.start()

    expiry = datetime.datetime(2026, 9, 30, 22, 5)
    for plugin in harness.plugins.values():
        assert plugin.getState() == (1004, {1002: expiry})


def test_legacy_single_id_state_file_loads(make_harness):
    harness = make_harness(state=b"1004\n")
    harness.start()

    for plugin in harness.plugins.values():
        assert plugin.getState() == (1004, {})
