"""Filesystem and hashing utilities for the pipeline."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import hashlib


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def file_signature(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for p in sorted(paths):
        if not p.exists():
            continue
        digest.update(str(p).encode())
        digest.update(str(p.stat().st_mtime_ns).encode())
    return digest.hexdigest()
