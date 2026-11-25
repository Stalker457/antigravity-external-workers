#!/usr/bin/env python3
"""Headless streaming wrapper for DeepSeek Harness CLI (`dsh headless --json`)."""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

WORKER_LOG_DIR = Path(tempfile.gettempdir()) / "dsh-worker-logs"
DSH_HEADLESS_PATCH = Path.home() / ".dsh" / "profiles" / "headless" / "cordis.patch.yml"
DSH_PTC_PATCH = Path.home() / ".dsh" / "patches" / "ptc.yml"
DESKTOP_DSH_CANDIDATES = tuple(
    str(p)
    for p in (
        Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
        / "DeepSeek Harness"
        / "resources"
        / "runtime"
        / "cli"
        / "bin"
        / "dsh.cmd",
        Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        / "Programs"
        / "DeepSeek Harness"
        / "resources"
        / "runtime"
        / "cli"
        / "bin"
        / "dsh.cmd",
    )
)
MAX_META_HISTORY = 30
MAX_TASK_PROMPT_CHARS = 65536
LIVE_SYNC_INTERVAL_SEC = 0.25


@dataclass
class WorkerStats:
    """Aggregated state parsed incrementally from `dsh headless --json` NDJSON streams."""

    session_id: str | None = None
    tools: list[str] = field(default_factory=list)
    pending_tools: dict[str, tuple[str, str]] = field(default_factory=dict)
    reasonings: list[dict[str, Any]] = field(default_factory=list)
    texts: list[str] = field(default_factory=list)
    final_text: str = ""
    non_json: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    step_count: int = 0
    turn_count: int = 0
    event_seq: int = 0
    last_step_start_seq: int = 0
    last_error_seq: int = 0
    last_text_seq: int = 0
    last_tool_seq: int = 0
    last_stop_seq: int = 0
    live_reasoning: str = ""
    live_text: str = ""
    step_start_mono: float | None = None
    step_gen_end_mono: float | None = None
    gen_ms: float = 0.0
    last_step_tps: float | None = None
    tokens: dict[str, int] = field(
        default_factory=lambda: {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "cache_read": 0,
            "cache_write": 0,
        }
    )


def now_hms() -> str:
    """Return local HH:MM:SS timestamp for live progress lines."""
    return dt.datetime.now().strftime("%H:%M:%S")


def format_token_count(n: int) -> str:
    """Format token count: exact integer below 100,000; M (1,000,000 = 1M) at 6+ digits."""
    val = max(0, int(n))
    if val < 100_000:
        return str(val)
    formatted = f"{val / 1_000_000:.2f}".rstrip("0").rstrip(".")
    return f"{formatted}M"


def compute_metrics(stats: WorkerStats) -> dict[str, Any]:
    """Compute cache hit rate and pure generation TPS metrics from WorkerStats."""
    tk = stats.tokens
    in_tok = int(tk.get("input") or 0)
    out_tok = int(tk.get("output") or 0)
    rsn_tok = int(tk.get("reasoning") or 0)
    cr_tok = int(tk.get("cache_read") or 0)
    cw_tok = int(tk.get("cache_write") or 0)

    prompt_total = in_tok + cr_tok + cw_tok
    cache_hit_rate = (cr_tok / prompt_total) * 100.0 if prompt_total > 0 else None

    gen_tokens = out_tok + rsn_tok
    avg_tps = (gen_tokens / (stats.gen_ms / 1000.0)) if (gen_tokens > 0 and stats.gen_ms > 0) else None

    return {
        "gen_ms": int(round(stats.gen_ms)),
        "gen_tokens": gen_tokens,
        "avg_tps": round(avg_tps, 2) if avg_tps is not None else None,
        "last_step_tps": round(stats.last_step_tps, 2) if stats.last_step_tps is not None else None,
        "cache_hit_rate": round(cache_hit_rate, 2) if cache_hit_rate is not None else None,
    }


def format_event_hms(event: dict[str, Any]) -> str:
    """Format an event millisecond timestamp as local HH:MM:SS, falling back to now."""
    ts = event.get("timestamp")
    if isinstance(ts, (int, float)) and ts > 0:
        try:
            sec = ts / 1000.0 if ts > 1e11 else float(ts)
            return dt.datetime.fromtimestamp(sec).strftime("%H:%M:%S")
        except (OSError, OverflowError, ValueError):
            pass
    return now_hms()


def extract_error_summary(event: dict[str, Any]) -> tuple[str, str]:
    """Extract concise (error_type, error_message) from an error event or non-completed reason."""
    err = event.get("error")
    if isinstance(err, dict):
        err_type = str(err.get("type") or err.get("name") or err.get("code") or "error").strip()
        raw_msg = err.get("message") or err.get("data") or json.dumps(err, ensure_ascii=False)
    elif err is not None:
        err_type = "error"
        raw_msg = str(err)
    else:
        reason = event.get("reason")
        if isinstance(reason, dict):
            err_type = str(reason.get("kind") or "error").strip()
            raw_msg = reason.get("message") or json.dumps(reason, ensure_ascii=False)
        else:
            err_type = "error"
            raw_msg = str(event.get("message") or "unknown error")
    msg = " ".join(str(raw_msg).splitlines()).strip()
    if len(msg) > 240:
        msg = msg[:240] + "..."
    return err_type, msg


def is_recovered_from_transient_error(
    returncode: int,
    was_stopped: bool,
    timed_out: bool,
    stats: WorkerStats,
) -> bool:
    """Return True when CLI exited 1 solely due to earlier transient `error` event(s) but finished cleanly."""
    if was_stopped or timed_out or returncode != 1:
        return False
    if not stats.errors or stats.stderr_lines:
        return False
    finished_with_stop = (
        stats.last_stop_seq > stats.last_error_seq
        and stats.last_stop_seq > stats.last_step_start_seq
    )
    finished_with_final_text = (
        stats.last_text_seq > stats.last_error_seq
        and stats.last_text_seq > stats.last_tool_seq
        and stats.last_text_seq > stats.last_step_start_seq
    )
    return finished_with_stop or finished_with_final_text


def read_windows_user_env(name: str) -> str | None:
    """Read an environment variable from Windows HKCU/HKLM registry if not yet in process env."""
    if sys.platform != "win32":
        return None
    try:
        import winreg

        hives = (
            (winreg.HKEY_CURRENT_USER, r"Environment"),
            (winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
        )
        for hive, subkey in hives:
            try:
                with winreg.OpenKey(hive, subkey) as key:
                    val, _ = winreg.QueryValueEx(key, name)
                    text = str(val).strip()
                    if text:
                        return text
            except OSError:
                continue
    except Exception:
        return None
    return None


def parse_headless_patch_info(patch_path: Path = DSH_HEADLESS_PATCH) -> dict[str, Any]:
    """Parse default model, provider models, and apiKeyEnv names from cordis.patch.yml using stdlib."""
    info: dict[str, Any] = {
        "default_provider": "",
        "default_model": "",
        "default_effort": "",
        "api_key_envs": [],
        "models": [],
    }
    if not patch_path.is_file():
        return info
    try:
        raw = patch_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return info

    # Collect apiKeyEnv names referenced anywhere in the patch file.
    for match in re.finditer(r"^\s*apiKeyEnv:\s*[\"']?([A-Za-z0-9_]+)[\"']?\s*$", raw, re.MULTILINE):
        env_name = match.group(1)
        if env_name not in info["api_key_envs"]:
            info["api_key_envs"].append(env_name)

    # Parse agent-default-model block and llm-pi-ai providers/models.
    lines = raw.splitlines()
    section = ""
    current_provider = ""
    current_model_id = ""
    current_efforts: list[str] = []
    in_providers = False
    in_models = False
    in_efforts = False
    provider_indent = -1
    efforts_indent = -1

    def flush_model() -> None:
        nonlocal current_model_id, current_efforts
        if current_provider and current_model_id:
            info["models"].append(
                {
                    "provider": current_provider,
                    "model": current_model_id,
                    "efforts": list(current_efforts),
                }
            )
        current_model_id = ""
        current_efforts = []

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))

        if indent == 0 and stripped.startswith("- id:"):
            flush_model()
            section = stripped.split(":", 1)[1].strip().strip("\"'")
            in_providers = False
            in_models = False
            in_efforts = False
            current_provider = ""
            continue

        if section == "agent-default-model":
            if stripped.startswith("provider:"):
                info["default_provider"] = stripped.split(":", 1)[1].strip().strip("\"'")
            elif stripped.startswith("model:"):
                info["default_model"] = stripped.split(":", 1)[1].strip().strip("\"'")
            elif stripped.startswith("reasoningEffort:"):
                info["default_effort"] = stripped.split(":", 1)[1].strip().strip("\"'")
        elif section == "llm-pi-ai":
            if stripped == "providers:":
                in_providers = True
                provider_indent = indent
                in_models = False
                in_efforts = False
                continue
            if not in_providers:
                continue

            if indent == provider_indent + 2 and stripped.endswith(":") and not stripped.startswith("-"):
                flush_model()
                current_provider = stripped[:-1].strip().strip("\"'")
                in_models = False
                in_efforts = False
                continue

            if stripped == "models:":
                in_models = True
                in_efforts = False
                continue

            if in_models:
                if in_efforts and indent <= efforts_indent:
                    in_efforts = False
                if stripped.startswith("- id:"):
                    flush_model()
                    current_model_id = stripped.split(":", 1)[1].strip().strip("\"'")
                    in_efforts = False
                    continue
                if stripped.startswith("reasoningEfforts:"):
                    rest = stripped.split(":", 1)[1].strip()
                    if rest.startswith("{") and rest.endswith("}"):
                        for kv in rest[1:-1].split(","):
                            k = kv.split(":", 1)[0].strip().strip("\"'")
                            if k and k not in current_efforts:
                                current_efforts.append(k)
                        in_efforts = False
                    else:
                        in_efforts = True
                        efforts_indent = indent
                    continue
                if in_efforts and ":" in stripped and not stripped.startswith("-"):
                    k = stripped.split(":", 1)[0].strip().strip("\"'")
                    if k and k not in current_efforts:
                        current_efforts.append(k)

    flush_model()
    return info


def hydrate_provider_env(base_env: dict[str, str], patch_info: dict[str, Any]) -> dict[str, str]:
    """Ensure API keys referenced by cordis.patch.yml are present in the child process env."""
    env = dict(base_env)
    for key_name in patch_info.get("api_key_envs") or []:
        if not env.get(key_name):
            reg_val = read_windows_user_env(key_name)
            if reg_val:
                env[key_name] = reg_val
    return env


def format_default_model(patch_info: dict[str, Any]) -> str:
    """Format default model string from cordis.patch.yml for monitor display."""
    prov = patch_info.get("default_provider") or ""
    mod = patch_info.get("default_model") or ""
    eff = patch_info.get("default_effort") or ""
    if prov and mod:
        base = f"{prov}/{mod}"
        return f"{base}#{eff}" if eff else base
    return mod or "<dsh-default>"


def find_dsh_command() -> list[str]:
    """Locate DeepSeek Harness CLI command prefix, preferring standalone Node `@deepseek-ai/dsh`."""
    node_exe = shutil.which("node") or "node"

    env_cli = os.environ.get("DSH_CLI_PATH", "").strip()
    if env_cli and Path(env_cli).is_file():
        if env_cli.lower().endswith(".js"):
            return [node_exe, env_cli]
        return [env_cli]

    # Prefer pure Node.js global @deepseek-ai/dsh (avoids Electron desktop RUNASADMIN/UAC conflict).
    npm_candidates: list[Path] = []
    appdata = os.environ.get("APPDATA", "").strip()
    if appdata:
        npm_candidates.append(Path(appdata) / "npm" / "node_modules" / "@deepseek-ai" / "dsh" / "lib" / "bin.js")
    npm_prefix = os.environ.get("NPM_CONFIG_PREFIX", "").strip()
    if npm_prefix:
        npm_candidates.append(Path(npm_prefix) / "node_modules" / "@deepseek-ai" / "dsh" / "lib" / "bin.js")
    for cand in npm_candidates:
        if cand.is_file():
            return [node_exe, str(cand)]

    # Check PATH for standalone dsh.
    which_dsh = shutil.which("dsh")
    if which_dsh and "deepseek harness" not in which_dsh.lower():
        return [which_dsh]

    # Fallback to Electron desktop dsh.cmd.
    if which_dsh:
        return [which_dsh]
    for desktop_cand in DESKTOP_DSH_CANDIDATES:
        if Path(desktop_cand).is_file():
            return [desktop_cand]

    raise FileNotFoundError(
        "Could not find DeepSeek Harness CLI (`dsh`). Install via `npm install -g @deepseek-ai/dsh` or set DSH_CLI_PATH."
    )


def summarize_tool_input(tool_input: Any) -> str:
    """Return a concise single-line preview of a tool invocation input."""
    if not isinstance(tool_input, dict):
        return str(tool_input)[:120]

    for key in ("description", "filePath", "file_path", "path", "file", "command", "cmd", "pattern", "query", "url", "code"):
        val = tool_input.get(key)
        if val is not None:
            text = " ".join(str(val).splitlines())
            return text[:140] + ("..." if len(text) > 140 else "")

    compact = json.dumps(tool_input, ensure_ascii=False)
    return compact[:140] + ("..." if len(compact) > 140 else "")


def run_list_models(patch_info: dict[str, Any]) -> int:
    """Display the default model and configured provider models from cordis.patch.yml."""
    print(f"ProfilePatch: {DSH_HEADLESS_PATCH}")
    print(f"Default: {format_default_model(patch_info)}")
    models = patch_info.get("models") or []
    if not models:
        print("(No models parsed from cordis.patch.yml)")
        return 0
    print("\nAvailable models:")
    for item in models:
        base = f"{item['provider']}/{item['model']}"
        efforts = item.get("efforts") or []
        if efforts:
            print(f"  {base}  [efforts: {', '.join(efforts)}]")
        else:
            print(f"  {base}")
    return 0


def load_task_prompt(args: argparse.Namespace) -> str:
    """Read the task prompt from --task-file, --task, or standard input."""
    if args.task_file:
        return Path(args.task_file).read_text(encoding="utf-8")
    if args.task:
        return args.task
    if not sys.stdin.isatty():
        data = sys.stdin.read()
        if data.strip():
            return data
    raise ValueError("Provide a task via --task-file, --task, or stdin.")


def resolve_model_override(cli_model: str | None) -> str | None:
    """Resolve optional model override from --model or DSH_WORKER_MODEL."""
    if cli_model and cli_model.strip():
        return cli_model.strip()
    env_model = os.environ.get("DSH_WORKER_MODEL", "").strip()
    if env_model:
        return env_model
    reg_model = read_windows_user_env("DSH_WORKER_MODEL")
    return reg_model or None


def create_model_patch_file(model_spec: str, patch_info: dict[str, Any], run_id: str) -> Path:
    """Write a temporary --patch YAML file overriding `agent-default-model` for this run."""
    effort = ""
    base = model_spec
    if "#" in model_spec:
        base, effort = model_spec.rsplit("#", 1)
        base = base.strip()
        effort = effort.strip()

    provider = patch_info.get("default_provider") or ""
    model_id = base
    known_providers = {m["provider"] for m in (patch_info.get("models") or [])}
    if provider:
        known_providers.add(provider)

    if "/" in base:
        first, rest = base.split("/", 1)
        if first in known_providers or rest.count("/") >= 1:
            provider = first
            model_id = rest

    cfg: dict[str, Any] = {"model": model_id}
    if provider:
        cfg["provider"] = provider
    if effort:
        cfg["reasoningEffort"] = effort

    patch_payload = [
        {
            "id": "agent-default-model",
            "name": "@deepseek-ai/dsh-agent-default-model",
            "config": cfg,
        }
    ]
    patch_path = WORKER_LOG_DIR / f"worker-{run_id}.patch.yml"
    patch_path.write_text(json.dumps(patch_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return patch_path


def resolve_log_paths(
    raw_log_arg: str | None, progress_arg: str | None
) -> tuple[str, Path, Path, Path]:
    """Create parent directories and return (run_id, raw_jsonl, progress_log, meta_json)."""
    run_id = f"{dt.datetime.now().strftime('%Y%m%d-%H%M%S-%f')}-p{os.getpid()}"
    WORKER_LOG_DIR.mkdir(parents=True, exist_ok=True)

    raw_path = Path(raw_log_arg) if raw_log_arg else WORKER_LOG_DIR / f"worker-{run_id}.jsonl"
    progress_path = (
        Path(progress_arg)
        if progress_arg
        else WORKER_LOG_DIR / f"worker-{run_id}.progress.log"
    )
    meta_path = WORKER_LOG_DIR / f"worker-{run_id}.meta.json"

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    return run_id, raw_path, progress_path, meta_path


def extract_task_title(explicit_title: str | None, prompt: str) -> str:
    """Derive a readable short title for the monitor header."""
    if explicit_title and explicit_title.strip():
        return explicit_title.strip()[:100]
    for raw_line in prompt.splitlines():
        line = raw_line.strip().lstrip("#").strip()
        if line:
            return line[:100]
    return "DSH Worker Run"


def write_meta_atomic(meta_path: Path, payload: dict[str, Any]) -> None:
    """Atomically write run metadata JSON for the DSH Monitor sidecar."""
    tmp_path = meta_path.with_suffix(".tmp")
    try:
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, meta_path)
    except OSError:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _is_within_dir(target: Path, base_dir: Path) -> bool:
    """Return True if target resolves strictly inside base_dir."""
    try:
        target.resolve().relative_to(base_dir.resolve())
        return True
    except ValueError:
        return False


def prune_old_meta(log_dir: Path, keep: int = MAX_META_HISTORY) -> None:
    """Keep only the most recent `keep` completed runs in WORKER_LOG_DIR, never pruning running ones."""
    try:
        metas = sorted(
            log_dir.glob("worker-*.meta.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return

    completed: list[tuple[Path, dict[str, Any]]] = []
    for meta_file in metas:
        try:
            data = json.loads(meta_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            completed.append((meta_file, {}))
            continue
        if data.get("status") == "running":
            continue
        completed.append((meta_file, data))

    for stale_meta, data in completed[keep:]:
        run_id = str(data.get("run_id") or "")
        candidates: list[Path] = [stale_meta]
        if run_id:
            candidates.extend(
                [
                    log_dir / f"worker-{run_id}.stop",
                    log_dir / f"worker-{run_id}.meta.tmp",
                    log_dir / f"worker-{run_id}.meta.json.stop.tmp",
                    log_dir / f"worker-{run_id}.patch.yml",
                    log_dir / f"worker-{run_id}.jsonl",
                    log_dir / f"worker-{run_id}.progress.log",
                ]
            )
        for key in ("raw_log", "progress_log"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                candidates.append(Path(val))

        seen: set[str] = set()
        for target in candidates:
            norm = str(target).lower() if sys.platform == "win32" else str(target)
            if norm in seen:
                continue
            seen.add(norm)
            if not _is_within_dir(target, log_dir):
                continue
            try:
                target.unlink(missing_ok=True)
            except OSError:
                pass


def get_final_output_text(stats: WorkerStats) -> str:
    """Return authoritative final.text when present, falling back to accumulated stream texts."""
    if stats.final_text:
        return stats.final_text
    return "\n\n".join(stats.texts).strip()


def build_run_command(
    cli_cmd: list[str],
    args: argparse.Namespace,
    patch_files: list[Path],
) -> list[str]:
    """Construct the `dsh [--patch ...] --profile headless --json` command line.

    Note: `--patch` is a launcher option and MUST precede `--profile`.
    """
    cmd = list(cli_cmd)
    for pf in patch_files:
        cmd.extend(["--patch", str(pf)])
    cmd.extend(["--profile", "headless", "--json"])
    if args.session:
        cmd.extend(["--session-id", args.session])
    if args.permission_mode:
        cmd.extend(["--permission-mode", args.permission_mode])
    if args.approval_policy:
        cmd.extend(["--approval-policy", args.approval_policy])
    for extra_dir in args.add_dir or []:
        cmd.extend(["--add-dir", extra_dir])
    # Read task prompt cleanly from stdin.
    cmd.append("-")
    return cmd


def emit_progress(
    progress_fh: TextIO,
    lock: threading.RLock,
    message: str,
    *,
    echo_stdout: bool = True,
) -> None:
    """Thread-safely append a timestamped progress line and flush immediately."""
    line = f"[{now_hms()}] {message}"
    with lock:
        progress_fh.write(line + "\n")
        progress_fh.flush()
    if echo_stdout:
        print(line, flush=True)


def flush_live_reasoning(stats: WorkerStats, progress_fh: TextIO, lock: threading.RLock) -> bool:
    """Commit accumulated streaming thinking chunks into stats.reasonings and progress log."""
    text = stats.live_reasoning.strip()
    stats.live_reasoning = ""
    if not text:
        return False
    stats.reasonings.append({"step": stats.step_count, "time": now_hms(), "text": text})
    preview = " ".join(text.splitlines())
    if len(preview) > 120:
        preview = preview[:120] + "..."
    emit_progress(progress_fh, lock, f"[think] {preview}")
    return True


def flush_live_text(stats: WorkerStats, progress_fh: TextIO, lock: threading.RLock) -> bool:
    """Commit accumulated streaming text chunks into stats.texts and progress log."""
    text = stats.live_text.strip()
    stats.live_text = ""
    if not text:
        return False
    stats.texts.append(text)
    emit_progress(progress_fh, lock, f"[output] 收到回复 ({len(text)} chars)")
    return True


def handle_stdout_line(
    raw_line: str,
    stats: WorkerStats,
    raw_fh: TextIO,
    progress_fh: TextIO,
    lock: threading.RLock,
) -> tuple[bool, bool]:
    """Persist one stdout line to raw log and return (immediate_sync, throttled_sync)."""
    with lock:
        raw_fh.write(raw_line)
        raw_fh.flush()

    line = raw_line.strip()
    if not line:
        return False, False

    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        with lock:
            stats.non_json.append(line)
        emit_progress(progress_fh, lock, f"[raw] {line[:200]}")
        return True, False

    immediate = False
    throttled = False
    with lock:
        stats.event_seq += 1
        sid = event.get("sessionId") or event.get("sessionID")
        if not stats.session_id and sid:
            stats.session_id = str(sid)
            emit_progress(progress_fh, lock, f"[session] {stats.session_id}")
            immediate = True

        ev_type = event.get("type")

        if ev_type == "session":
            immediate = True
        elif ev_type == "status":
            phase = str(event.get("phase") or "")
            if phase == "turn_start":
                stats.turn_count = int(event.get("turn") or (stats.turn_count + 1))
                immediate = True
            elif phase == "step_start":
                flush_live_reasoning(stats, progress_fh, lock)
                flush_live_text(stats, progress_fh, lock)
                stats.step_count += 1
                stats.last_step_start_seq = stats.event_seq
                stats.step_start_mono = time.monotonic()
                stats.step_gen_end_mono = None
                emit_progress(progress_fh, lock, f"[step {stats.step_count}] 推理中...")
                immediate = True
            elif phase == "step_end":
                flush_live_reasoning(stats, progress_fh, lock)
                flush_live_text(stats, progress_fh, lock)
                usage = event.get("usage") or {}
                in_tok = int(usage.get("inputTokens") or 0)
                out_tok = int(usage.get("outputTokens") or 0)
                rsn_tok = int(usage.get("reasoningTokens") or 0)
                cr_tok = int(usage.get("cacheReadTokens") or 0)
                cw_tok = int(usage.get("cacheWriteTokens") or 0)
                stats.tokens["input"] += in_tok
                stats.tokens["output"] += out_tok
                stats.tokens["reasoning"] += rsn_tok
                stats.tokens["cache_read"] += cr_tok
                stats.tokens["cache_write"] += cw_tok
                step_tps_str = ""
                step_out = out_tok + rsn_tok
                if stats.step_start_mono is not None:
                    end_mono = stats.step_gen_end_mono if stats.step_gen_end_mono is not None else time.monotonic()
                    pure_ms = max(0.0, (end_mono - stats.step_start_mono) * 1000.0)
                    if step_out > 0 and pure_ms > 0:
                        stats.gen_ms += pure_ms
                        stats.last_step_tps = step_out / (pure_ms / 1000.0)
                        step_tps_str = f", tps={stats.last_step_tps:.1f}"
                    stats.step_start_mono = None
                    stats.step_gen_end_mono = None
                emit_progress(
                    progress_fh,
                    lock,
                    f"[step {stats.step_count} done] in={format_token_count(in_tok)}, out={format_token_count(out_tok)}, cache_read={format_token_count(cr_tok)}{step_tps_str}",
                )
                immediate = True
            elif phase == "turn_end":
                flush_live_reasoning(stats, progress_fh, lock)
                flush_live_text(stats, progress_fh, lock)
                reason = event.get("reason") or {}
                kind = str(reason.get("kind") or "") if isinstance(reason, dict) else str(reason)
                if kind == "completed":
                    stats.last_stop_seq = stats.event_seq
                elif kind and kind not in ("tool_calls", "tool-calls"):
                    stats.last_error_seq = stats.event_seq
                    err_time = format_event_hms(event)
                    err_type, err_msg = extract_error_summary(event)
                    stats.errors.append(
                        {"time": err_time, "step": stats.step_count, "type": err_type, "message": err_msg}
                    )
                    emit_progress(progress_fh, lock, f"[error] {err_type}: {err_msg}")
                immediate = True
        elif ev_type in ("thinking", "reasoning"):
            delta = str(event.get("text") or event.get("delta") or "")
            if delta:
                stats.live_reasoning += delta
                throttled = True
        elif ev_type == "text":
            if stats.live_reasoning:
                flush_live_reasoning(stats, progress_fh, lock)
                immediate = True
            delta = str(event.get("text") or event.get("delta") or "")
            if delta:
                stats.last_text_seq = stats.event_seq
                stats.live_text += delta
                throttled = True
        elif ev_type == "tool_call":
            if stats.step_start_mono is not None and stats.step_gen_end_mono is None:
                stats.step_gen_end_mono = time.monotonic()
            if stats.live_reasoning:
                flush_live_reasoning(stats, progress_fh, lock)
            if stats.live_text:
                flush_live_text(stats, progress_fh, lock)
            stats.last_tool_seq = stats.event_seq
            call_id = str(event.get("callId") or event.get("id") or "")
            tool_name = str(event.get("tool") or "unknown")
            preview = summarize_tool_input(event.get("input"))
            if call_id:
                stats.pending_tools[call_id] = (tool_name, preview)
            entry = f"[running] {tool_name}: {preview}" if preview else f"[running] {tool_name}"
            emit_progress(progress_fh, lock, f"[tool] {entry}")
            immediate = True
        elif ev_type == "tool_result":
            call_id = str(event.get("callId") or event.get("id") or "")
            status = str(event.get("status") or "completed")
            tool_name, preview = stats.pending_tools.pop(
                call_id, (str(event.get("tool") or "tool"), "")
            )
            entry = f"[{status}] {tool_name}: {preview}" if preview else f"[{status}] {tool_name}"
            stats.tools.append(entry)
            emit_progress(progress_fh, lock, f"[tool] {entry}")
            immediate = True
        elif ev_type == "final":
            flush_live_reasoning(stats, progress_fh, lock)
            flush_live_text(stats, progress_fh, lock)
            reason = event.get("reason") or {}
            kind = str(reason.get("kind") or "") if isinstance(reason, dict) else str(reason)
            if kind == "completed":
                stats.last_stop_seq = stats.event_seq
            final_text = str(event.get("text") or "").strip()
            if final_text:
                stats.last_text_seq = stats.event_seq
                stats.final_text = final_text
                if not stats.texts or stats.texts[-1] != final_text:
                    emit_progress(progress_fh, lock, f"[output] 收到最终回复 ({len(final_text)} chars)")
            immediate = True
        elif ev_type == "error":
            if stats.live_reasoning:
                flush_live_reasoning(stats, progress_fh, lock)
            stats.last_error_seq = stats.event_seq
            err_time = format_event_hms(event)
            err_type, err_msg = extract_error_summary(event)
            stats.errors.append(
                {
                    "time": err_time,
                    "step": stats.step_count,
                    "type": err_type,
                    "message": err_msg,
                }
            )
            emit_progress(progress_fh, lock, f"[error] {err_type}: {err_msg}")
            immediate = True

    return immediate, throttled


class WindowsJobObject:
    """Windows Job Object configured with KILL_ON_JOB_CLOSE to prevent orphan child trees."""

    def __init__(self) -> None:
        self._handle: int | None = None
        self._k32 = None
        if sys.platform != "win32":
            return
        try:
            from ctypes import wintypes

            class _BasicLimits(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD),
                ]

            class _IoCounters(ctypes.Structure):
                _fields_ = [(name, ctypes.c_uint64) for name in ("r", "w", "o", "rt", "wt", "ot")]

            class _ExtendedLimits(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation", _BasicLimits),
                    ("IoInfo", _IoCounters),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            k32 = ctypes.WinDLL("kernel32", use_last_error=True)
            k32.CreateJobObjectW.restype = wintypes.HANDLE
            k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            k32.SetInformationJobObject.restype = wintypes.BOOL
            k32.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
            ]
            k32.AssignProcessToJobObject.restype = wintypes.BOOL
            k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            k32.OpenProcess.restype = wintypes.HANDLE
            k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k32.CloseHandle.restype = wintypes.BOOL
            k32.CloseHandle.argtypes = [wintypes.HANDLE]

            handle = k32.CreateJobObjectW(None, None)
            if not handle:
                return
            info = _ExtendedLimits()
            info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not k32.SetInformationJobObject(handle, 9, ctypes.byref(info), ctypes.sizeof(info)):
                k32.CloseHandle(handle)
                return
            self._handle = handle
            self._k32 = k32
        except Exception:
            self._handle = None
            self._k32 = None

    def assign_pid(self, pid: int) -> None:
        if not self._handle or not self._k32 or pid <= 0:
            return
        try:
            proc_handle = self._k32.OpenProcess(0x001F0FFF, False, int(pid))
            if proc_handle:
                try:
                    self._k32.AssignProcessToJobObject(self._handle, proc_handle)
                finally:
                    self._k32.CloseHandle(proc_handle)
        except Exception:
            pass

    def close(self) -> None:
        if self._handle and self._k32:
            try:
                self._k32.CloseHandle(self._handle)
            except Exception:
                pass
            self._handle = None


def terminate_process_tree(proc: subprocess.Popen[str] | None) -> None:
    """Force-terminate a subprocess and all its descendants on Windows or POSIX."""
    if proc is None or proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        proc.kill()


def stream_worker(
    cli_cmd: list[str],
    args: argparse.Namespace,
    patch_info: dict[str, Any],
    model_override: str | None,
    prompt: str,
    workdir: Path,
    idle_timeout: int,
    max_timeout: int,
    run_id: str,
    raw_path: Path,
    progress_path: Path,
    meta_path: Path,
    effective_model: str,
    exec_mode: str,
    extra_patches: list[Path],
) -> tuple[int, bool, WorkerStats]:
    """Run `dsh headless --json` with live line-by-line streaming, throttled meta sync, and idle timeout."""
    stats = WorkerStats()
    lock = threading.RLock()
    timer_lock = threading.Lock()
    timed_out = False
    idle_timer: threading.Timer | None = None
    max_timer: threading.Timer | None = None
    finished = False
    started_ms = int(time.time() * 1000)
    started_iso = dt.datetime.now().isoformat(timespec="seconds")
    title = extract_task_title(args.title, prompt)
    batch_id = (args.batch_id or "").strip() or run_id
    batch_title = (args.batch_title or "").strip()[:100] or title
    stop_marker = WORKER_LOG_DIR / f"worker-{run_id}.stop"
    job_obj = WindowsJobObject()
    worker_pid = os.getpid()
    cli_pid: int | None = None
    prompt_stored = (
        prompt
        if len(prompt) <= MAX_TASK_PROMPT_CHARS
        else prompt[:MAX_TASK_PROMPT_CHARS] + "\n\n... (truncated)"
    )

    def sync_meta(status: str, exit_code: int | None = None, ended_ms: int | None = None) -> None:
        with lock:
            effective_status = "stopped" if stop_marker.exists() else status
            payload: dict[str, Any] = {
                "run_id": run_id,
                "batch_id": batch_id,
                "batch_title": batch_title,
                "title": title,
                "status": effective_status,
                "started_at": started_iso,
                "updated_at": dt.datetime.now().isoformat(timespec="seconds"),
                "started_ms": started_ms,
                "ended_ms": ended_ms,
                "model": effective_model,
                "exec_mode": exec_mode,
                "workdir": str(workdir),
                "pids": {
                    "worker": worker_pid,
                    "server": None,
                    "cli": cli_pid,
                },
                "session_id": stats.session_id,
                "step_count": stats.step_count,
                "tokens": dict(stats.tokens),
                "metrics": compute_metrics(stats),
                "tools_count": len(stats.tools) + len(stats.pending_tools),
                "last_tool": stats.tools[-1] if stats.tools else None,
                "reasonings": list(stats.reasonings[-30:]),
                "errors": list(stats.errors[-30:]),
                "live_reasoning": "" if ended_ms else stats.live_reasoning,
                "live_text": "" if ended_ms else stats.live_text,
                "task_file": str(Path(args.task_file).resolve()) if args.task_file else None,
                "task_prompt": prompt_stored,
                "progress_log": str(progress_path),
                "raw_log": str(raw_path),
                "output_text": get_final_output_text(stats),
                "exit_code": exit_code,
            }
            write_meta_atomic(meta_path, payload)

    prune_old_meta(WORKER_LOG_DIR)

    run_env = hydrate_provider_env(os.environ, patch_info)
    patch_files: list[Path] = list(extra_patches)
    temp_patch_file: Path | None = None
    if model_override:
        temp_patch_file = create_model_patch_file(model_override, patch_info, run_id)
        patch_files.append(temp_patch_file)

    cmd = build_run_command(cli_cmd, args, patch_files)

    with (
        raw_path.open("w", encoding="utf-8", errors="replace") as raw_fh,
        progress_path.open("w", encoding="utf-8", errors="replace") as progress_fh,
    ):
        header_lines = [
            "=== DSH Worker Started ===",
            f"RunID: {run_id}",
            f"BatchID: {batch_id}",
            f"Model: {effective_model}",
            f"Mode: dsh-headless-json ({exec_mode})",
            f"Workdir: {workdir}",
            f"ProgressLog: {progress_path}",
            f"RawLog: {raw_path}",
        ]
        for hline in header_lines:
            emit_progress(progress_fh, lock, hline)

        sync_meta("running")

        proc: subprocess.Popen[str] | None = None

        def on_timeout(reason_msg: str) -> None:
            nonlocal timed_out
            timed_out = True
            emit_progress(progress_fh, lock, reason_msg)
            terminate_process_tree(proc)

        def reset_idle_timer() -> None:
            nonlocal idle_timer
            if idle_timeout <= 0:
                return
            with timer_lock:
                if finished:
                    return
                if idle_timer is not None:
                    idle_timer.cancel()
                idle_timer = threading.Timer(
                    idle_timeout,
                    on_timeout,
                    args=(f"[timeout] 连续 {idle_timeout}s 无新输出，判定卡死并终止子进程...",),
                )
                idle_timer.daemon = True
                idle_timer.start()

        proc = subprocess.Popen(
            cmd,
            cwd=str(workdir),
            env=run_env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        cli_pid = proc.pid
        job_obj.assign_pid(cli_pid)
        sync_meta("running")

        def write_stdin() -> None:
            try:
                if proc and proc.stdin:
                    proc.stdin.write(prompt)
                    proc.stdin.close()
            except OSError:
                pass

        def drain_stderr() -> None:
            if not proc or not proc.stderr:
                return
            for err_line in proc.stderr:
                cleaned = err_line.rstrip("\r\n")
                with lock:
                    raw_fh.write(f"[STDERR] {cleaned}\n")
                    raw_fh.flush()
                if cleaned:
                    reset_idle_timer()
                    # Ignore benign dsh diagnostic lines like `dsh: reasoning: ...` when counting fatal stderr.
                    if not cleaned.startswith("dsh: reasoning:"):
                        with lock:
                            stats.stderr_lines.append(cleaned)
                    emit_progress(progress_fh, lock, f"[stderr] {cleaned[:200]}", echo_stdout=False)

        stdin_thread = threading.Thread(target=write_stdin, daemon=True)
        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        if max_timeout > 0:
            max_timer = threading.Timer(
                max_timeout,
                on_timeout,
                args=(f"[timeout] 总运行时长超过 {max_timeout}s 上限，正在终止子进程...",),
            )
            max_timer.daemon = True
            max_timer.start()

        reset_idle_timer()
        stdin_thread.start()
        stderr_thread.start()

        last_throttled_sync = 0.0
        try:
            if proc.stdout:
                for raw_line in proc.stdout:
                    reset_idle_timer()
                    immediate, throttled = handle_stdout_line(raw_line, stats, raw_fh, progress_fh, lock)
                    now = time.monotonic()
                    if immediate or (throttled and now - last_throttled_sync >= LIVE_SYNC_INTERVAL_SEC):
                        last_throttled_sync = now
                        sync_meta("running")
            returncode = proc.wait()
        finally:
            with timer_lock:
                finished = True
                if idle_timer is not None:
                    idle_timer.cancel()
                if max_timer is not None:
                    max_timer.cancel()
            job_obj.close()
            stdin_thread.join(timeout=2)
            stderr_thread.join(timeout=2)
            if temp_patch_file is not None:
                try:
                    temp_patch_file.unlink(missing_ok=True)
                except OSError:
                    pass

        with lock:
            flush_live_reasoning(stats, progress_fh, lock)
            flush_live_text(stats, progress_fh, lock)

        was_stopped = stop_marker.exists()
        was_recovered = is_recovered_from_transient_error(returncode, was_stopped, timed_out, stats)
        if was_stopped:
            final_code = 130
            final_key = "stopped"
            final_status = "STOPPED"
        elif timed_out:
            final_code = 124
            final_key = "timeout"
            final_status = "TIMEOUT"
        elif returncode == 0 or was_recovered:
            final_code = 0
            final_key = "ok"
            final_status = "OK"
        else:
            final_code = returncode
            final_key = "error"
            final_status = f"ERROR (exit={returncode})"

        if stats.errors:
            emit_progress(progress_fh, lock, f"=== Mid-Run Errors ({len(stats.errors)}) ===")
            for item in stats.errors:
                step_label = f"step {item['step']}" if item.get("step") else "init"
                emit_progress(
                    progress_fh,
                    lock,
                    f"  - [{item['time']}] ({step_label}) {item['type']}: {item['message']}",
                )
        emit_progress(progress_fh, lock, f"=== Worker Finished: {final_status} ===")
        final_output = get_final_output_text(stats)
        if final_output:
            with lock:
                progress_fh.write("\n=== Worker Output ===\n" + final_output + "\n")
                progress_fh.flush()
        sync_meta(final_key, exit_code=final_code, ended_ms=int(time.time() * 1000))

    return final_code, timed_out, stats


def main() -> int:
    """Entry point for the DeepSeek Harness (`dsh`) worker wrapper."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Run DeepSeek Harness CLI (`dsh headless`) as a subagent worker.")
    parser.add_argument("--list-models", action="store_true", help="List configured DSH headless models and exit.")
    parser.add_argument("--workdir", default=".", help="Working directory for the task.")
    parser.add_argument("--task", help="Task prompt text.")
    parser.add_argument("--task-file", help="Path to a UTF-8 file containing the task prompt.")
    parser.add_argument("--model", "-m", help="Optional model override in format provider/model#effort.")
    parser.add_argument(
        "--mode",
        choices=("native", "ptc"),
        default="native",
        help="Tool execution mode: native (default, simple tasks) or ptc (Programmatic Tool Calling, complex multi-file tasks).",
    )
    parser.add_argument(
        "--ptc",
        action="store_true",
        help="Shorthand for --mode ptc (loads ~/.dsh/patches/ptc.yml before --profile headless).",
    )
    parser.add_argument(
        "--ptc-patch",
        help=f"Optional custom PTC patch YAML path (default: {DSH_PTC_PATCH}).",
    )
    parser.add_argument(
        "--patch",
        action="append",
        help="Extra launcher --patch YAML path (repeatable; always placed before --profile headless).",
    )
    parser.add_argument("--session", "-s", help="Continue a persisted DSH session ID (`--session-id`).")
    parser.add_argument("--title", help="Optional worker title displayed in DSH Monitor tabs.")
    parser.add_argument("--batch-id", help="Optional batch/group ID shared by concurrent workers in the same dispatch round.")
    parser.add_argument("--batch-title", help="Optional group title displayed for this dispatch round in DSH Monitor.")
    parser.add_argument(
        "--permission-mode",
        default="danger-full-access",
        help="DSH permission preset (default: danger-full-access so Windows pwsh/python run without AppContainer 0xC0000142).",
    )
    parser.add_argument(
        "--approval-policy",
        default="never",
        choices=("never", "ask"),
        help="Approval behavior for headless execution (default: never).",
    )
    parser.add_argument("--add-dir", action="append", help="Extra writable directory under workspace-write (repeatable).")
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=900,
        help="Abort only if no output is received for N seconds (default: 900; 0 disables).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="Optional hard total runtime limit in seconds (default: 0 = unlimited while active).",
    )
    parser.add_argument("--raw-log", help="Optional path to save the raw NDJSON event log.")
    parser.add_argument("--progress-file", help="Optional path to write live human-readable progress log.")
    args = parser.parse_args()

    patch_info = parse_headless_patch_info()
    if args.list_models:
        return run_list_models(patch_info)

    cli_cmd = find_dsh_command()
    workdir = Path(args.workdir).resolve()
    if not workdir.is_dir():
        print(f"Error: workdir does not exist: {workdir}", file=sys.stderr)
        return 2

    prompt = load_task_prompt(args)
    exec_mode = "ptc" if (args.ptc or args.mode == "ptc") else "native"
    extra_patches: list[Path] = []
    if exec_mode == "ptc":
        ptc_patch = Path(args.ptc_patch).expanduser() if args.ptc_patch else DSH_PTC_PATCH
        if not ptc_patch.is_file():
            print(f"Error: PTC patch file not found: {ptc_patch}", file=sys.stderr)
            return 2
        extra_patches.append(ptc_patch)
    for raw_patch in args.patch or []:
        p = Path(raw_patch).expanduser()
        if not p.is_file():
            print(f"Error: patch file not found: {p}", file=sys.stderr)
            return 2
        if p not in extra_patches:
            extra_patches.append(p)

    model_override = resolve_model_override(args.model)
    run_id, raw_path, progress_path, meta_path = resolve_log_paths(args.raw_log, args.progress_file)
    effective_model = model_override or format_default_model(patch_info)

    returncode, timed_out, stats = stream_worker(
        cli_cmd=cli_cmd,
        args=args,
        patch_info=patch_info,
        model_override=model_override,
        prompt=prompt,
        workdir=workdir,
        idle_timeout=args.idle_timeout,
        max_timeout=args.timeout,
        run_id=run_id,
        raw_path=raw_path,
        progress_path=progress_path,
        meta_path=meta_path,
        effective_model=effective_model,
        exec_mode=exec_mode,
        extra_patches=extra_patches,
    )

    tk = stats.tokens
    status_label = "TIMEOUT" if timed_out else ("OK" if returncode == 0 else f"ERROR (exit={returncode})")

    print("\n=== DSH Worker Summary ===")
    print(f"Status: {status_label}")
    if stats.errors:
        print(f"MidRunErrors ({len(stats.errors)}):")
        for item in stats.errors:
            step_label = f"step {item['step']}" if item.get("step") else "init"
            print(f"  - [{item['time']}] ({step_label}) {item['type']}: {item['message']}")
    print(f"SessionID: {stats.session_id or '<unknown>'}")
    print(f"Model: {effective_model}")
    print(f"ExecMode: {exec_mode}")
    print(f"Workdir: {workdir}")
    metrics = compute_metrics(stats)
    hit_str = f"{metrics['cache_hit_rate']:.1f}%" if metrics["cache_hit_rate"] is not None else "--"
    tps_str = f"{metrics['avg_tps']:.1f} tok/s" if metrics["avg_tps"] is not None else "--"
    print(
        "Tokens: "
        f"input={format_token_count(tk['input'])}, output={format_token_count(tk['output'])}, "
        f"cache_read={format_token_count(tk['cache_read'])} "
        f"(cache_hit={hit_str}, tps={tps_str})"
    )
    print(f"Tools ({len(stats.tools)}):")
    for item in stats.tools[:40]:
        print(f"  - {item}")
    if len(stats.tools) > 40:
        print(f"  ... ({len(stats.tools) - 40} more in log)")
    print(f"ProgressLog: {progress_path}")
    print(f"RawLog: {raw_path}")

    final_output = get_final_output_text(stats)
    if final_output:
        print("\n=== Worker Output ===")
        print(final_output)
    elif stats.non_json:
        print("\n=== Raw Output ===")
        print("\n".join(stats.non_json[-30:]))

    if stats.stderr_lines and returncode != 0:
        print("\n=== Stderr ===", file=sys.stderr)
        print("\n".join(stats.stderr_lines[-30:]), file=sys.stderr)

    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
