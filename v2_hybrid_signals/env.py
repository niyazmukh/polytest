from __future__ import annotations

import os
from pathlib import Path
from typing import Dict


DEFAULT_ENV_FILE = ".env.v2.local"


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and ((text[0] == '"' and text[-1] == '"') or (text[0] == "'" and text[-1] == "'")):
        return text[1:-1]
    return text


def parse_env_file(path: str) -> Dict[str, str]:
    values: Dict[str, str] = {}
    target = Path(path)
    if not target.exists() or not target.is_file():
        return values
    for raw in target.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        values[key] = _strip_quotes(value)
    return values


def load_env_file(path: str, *, override: bool = False) -> Dict[str, str]:
    parsed = parse_env_file(path)
    for key, value in parsed.items():
        if override or key not in os.environ:
            os.environ[key] = value
    return parsed
