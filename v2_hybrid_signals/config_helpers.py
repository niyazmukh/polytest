from __future__ import annotations

import argparse
import os
from typing import Optional

from .env import DEFAULT_ENV_FILE, load_env_file


_Argv = Optional[list[str]]


def bootstrap_env_file(argv: _Argv) -> str:
    """Load env file early so parser defaults consistently come from env."""
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--env-file", default=os.environ.get("ENV_FILE", DEFAULT_ENV_FILE))
    pre_args, _ = pre.parse_known_args(argv)
    env_file = str(pre_args.env_file).strip() or DEFAULT_ENV_FILE
    load_env_file(env_file)
    return env_file


def reload_env_file_if_changed(current_env_file: str, cli_env_file: str) -> str:
    resolved = str(cli_env_file).strip() or DEFAULT_ENV_FILE
    if resolved != current_env_file:
        load_env_file(resolved)
    return resolved


def env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    if raw is None:
        return default
    text = str(raw).strip()
    return text if text else default


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return float(default)
    text = str(raw).strip()
    if not text:
        return float(default)
    try:
        return float(text)
    except ValueError:
        return float(default)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return int(default)
    text = str(raw).strip()
    if not text:
        return int(default)
    try:
        return int(text)
    except ValueError:
        return int(default)


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)
