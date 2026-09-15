"""Persist ARM / user settings across restarts."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mark2.config import Mark2Config, load_config, save_config
from mark2.engine import Mark2Engine


def test_set_enabled_persists_to_settings_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "mark2_settings.json"
        eng = Mark2Engine(Mark2Config(), persist_path=path)
        eng.set_enabled(True)
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert raw["MARK2_ENABLED"] is True
        eng.set_enabled(False)
        raw = json.loads(path.read_text(encoding="utf-8"))
        assert raw["MARK2_ENABLED"] is False


def test_load_config_merges_settings_over_defaults():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "mark2_settings.json"
        save_config(Mark2Config(MARK2_ENABLED=False, CONTRACTS=3), path)
        cfg = load_config(path)
        assert cfg.MARK2_ENABLED is False
        assert cfg.CONTRACTS == 3
