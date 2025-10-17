"""Configuration loading utilities."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class ConfigBundle:
    globals: Dict[str, Any]
    params: Dict[str, Any]
    schemas: Dict[str, Any]


def load_configs(root: Path) -> ConfigBundle:
    config_dir = root / "config"
    globals_cfg = _read_yaml(config_dir / "globals.yaml")
    params_cfg = _read_yaml(config_dir / "params.yaml")
    schemas_cfg = _read_yaml(config_dir / "schemas.yaml")
    return ConfigBundle(globals=globals_cfg, params=params_cfg, schemas=schemas_cfg)


def _read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}
