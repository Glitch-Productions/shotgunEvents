"""Plugin loading without ``imp`` (shotgun-events #34)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import importlib_wrapper  # noqa: E402


def test_load_source_runs_the_file_and_registers_it(tmp_path):
    plugin = tmp_path / "wrapper_plugin_a.py"
    plugin.write_text("VALUE = 41 + 1\n")

    module = importlib_wrapper.load_source("wrapper_plugin_a", str(plugin))

    assert module.VALUE == 42
    assert sys.modules["wrapper_plugin_a"] is module


def test_load_source_again_picks_up_edits(tmp_path):
    plugin = tmp_path / "wrapper_plugin_b.py"
    plugin.write_text("VALUE = 'first'\n")
    importlib_wrapper.load_source("wrapper_plugin_b", str(plugin))
    plugin.write_text("VALUE = 'second'\n")

    module = importlib_wrapper.load_source("wrapper_plugin_b", str(plugin))

    assert module.VALUE == "second"
    assert sys.modules["wrapper_plugin_b"] is module


def test_dataclass_in_a_plugin_resolves_its_own_module(tmp_path):
    plugin = tmp_path / "wrapper_plugin_c.py"
    plugin.write_text(
        "from __future__ import annotations\n"
        "import dataclasses\n"
        "import typing\n"
        "\n"
        "@dataclasses.dataclass\n"
        "class Item:\n"
        "    tags: typing.ClassVar[list] = []\n"
        "    name: str = ''\n"
    )

    module = importlib_wrapper.load_source("wrapper_plugin_c", str(plugin))

    assert [f.name for f in module.dataclasses.fields(module.Item)] == ["name"]
