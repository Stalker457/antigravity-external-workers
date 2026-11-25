#!/usr/bin/env python3
"""Headless streaming wrapper for OpenCode CLI (`opencode-cli.exe`) with live SSE deltas."""

from __future__ import annotations

import argparse
import base64
import ctypes
import datetime as dt
import json
import os
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TextIO

DEFAULT_CLI_CANDIDATES = tuple(
    str(p)
    for p in (
        Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local")))
        / "Programs"
        / "@opencodedesktop"
        / "resources"
        / "opencode-cli.exe",
        Path.home()
        / "AppData"
        / "Local"
        / "Programs"
        / "@opencodedesktop"
        / "resources"
        / "opencode-cli.exe",
    )
)
WORKER_LOG_DIR = Path(tempfile.gettempdir()) / "opencode-worker-logs"
MAX_META_HISTORY = 30
MAX_TASK_PROMPT_CHARS = 65536
SSE_SYNC_INTERVAL_SEC = 0.25


@dataclass
class WorkerStats:
    """Aggregated state parsed incrementally from OpenCode JSONL and SSE streams."""

    session_id: str | None = None
    tools: list[str] = field(default_factory=list)
    reasonings: list[dict[str, Any]] = field(default_factory=list)
    texts: list[str] = field(default_factory=list)
    non_json: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    step_count: int = 0
    event_seq: int = 0
    last_step_start_seq: int = 0
    last_error_seq: int = 0
    last_text_seq: int = 0
    last_tool_seq: int = 0
    last_stop_seq: int = 0
    live_reasoning: str = ""
    live_reasoning_id: str | None = None
    live_text: str = ""
    live_text_id: str | None = None
    step_start_ms: float | None = None
    step_last_llm_end_ms: float | None = None
    step_tool_intervals: list[tuple[float, float]] = field(default_factory=list)
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


def event_ts_ms(event: dict[str, Any]) -> float:
    """Return millisecond timestamp from an event, falling back to local wall clock."""
    ts = event.get("timestamp")
    if isinstance(ts, (int, float)) and ts > 0:
        return float(ts) if ts > 1e11 else float(ts) * 1000.0
    return time.time() * 1000.0


def merge_interval_ms(
    intervals: list[tuple[float, float]],
    win_start: float,
    win_end: float,
) -> float:
    """Return total non-overlapping milliseconds of intervals clipped to [win_start, win_end]."""
    if win_end <= win_start or not intervals:
        return 0.0
    clipped: list[tuple[float, float]] = []
    for s, e in intervals:
        cs = max(win_start, s)
        ce = min(win_end, e)
        if ce > cs:
            clipped.append((cs, ce))
    if not clipped:
        return 0.0
    clipped.sort()
    total = 0.0
    cur_s, cur_e = clipped[0]
    for s, e in clipped[1:]:
        if s <= cur_e:
            if e > cur_e:
                cur_e = e
        else:
            total += cur_e - cur_s
            cur_s, cur_e = s, e
    total += cur_e - cur_s
    return total


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
    """Format an OpenCode event millisecond timestamp as local HH:MM:SS, falling back to now."""
    ts = event.get("timestamp")
    if isinstance(ts, (int, float)) and ts > 0:
        try:
            sec = ts / 1000.0 if ts > 1e11 else float(ts)
            return dt.datetime.fromtimestamp(sec).strftime("%H:%M:%S")
        except (OSError, OverflowError, ValueError):
            pass
    return now_hms()


def extract_error_summary(event: dict[str, Any]) -> tuple[str, str]:
    """Extract concise (error_type, error_message) from an OpenCode `type=error` event."""
    err = event.get("error")
    if isinstance(err, dict):
        err_type = str(err.get("type") or err.get("name") or "error").strip()
        raw_msg = err.get("message") or err.get("data") or json.dumps(err, ensure_ascii=False)
    elif err is not None:
        err_type = "error"
        raw_msg = str(err)
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


def find_opencode_cli() -> str:
    """Locate the OpenCode CLI binary on disk or in PATH."""
    env_cli = os.environ.get("OPENCODE_CLI_PATH", "").strip()
    if env_cli and Path(env_cli).is_file():
        return env_cli

    for candidate in DEFAULT_CLI_CANDIDATES:
        if Path(candidate).is_file():
            return candidate

    for name in ("opencode-cli", "opencode"):
        found = shutil.which(name)
        if found:
            return found

    raise FileNotFoundError(
        "Could not find opencode-cli.exe. Set OPENCODE_CLI_PATH or add opencode to PATH."
    )


def summarize_tool_input(tool_input: Any) -> str:
    """Return a concise single-line preview of a tool invocation input."""
    if not isinstance(tool_input, dict):
        return str(tool_input)[:120]

    for key in ("filePath", "path", "file", "command", "cmd", "pattern", "query", "url"):
        val = tool_input.get(key)
        if val is not None:
            text = " ".join(str(val).splitlines())
            return text[:140] + ("..." if len(text) > 140 else "")

    compact = json.dumps(tool_input, ensure_ascii=False)
    return compact[:140] + ("..." if len(compact) > 140 else "")


def run_list_models(cli_path: str, server_url: str | None) -> int:
    """List available models configured in OpenCode."""
    cmd = [cli_path, "models"]
    cmd.extend(["--server", server_url] if server_url else ["--standalone"])
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.stderr and proc.returncode != 0:
        print(proc.stderr.strip(), file=sys.stderr)
    return proc.returncode


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
        else raw_path.with_name(f"{raw_path.stem}.progress.log")
    )
    meta_path = WORKER_LOG_DIR / f"worker-{run_id}.meta.json"

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    return run_id, raw_path, progress_path, meta_path


def _is_within_dir(target: Path, base_dir: Path) -> bool:
    """Return True if target resolves strictly inside base_dir."""
    try:
        resolved_target = target.resolve()
        resolved_base = base_dir.resolve()
        return resolved_base in resolved_target.parents
    except OSError:
        return False


def prune_old_meta(meta_dir: Path, keep: int = MAX_META_HISTORY) -> None:
    """Keep only the most recent finished runs in WORKER_LOG_DIR, pruning their temp logs too."""
    try:
        files = sorted(meta_dir.glob("worker-*.meta.json"), key=lambda p: p.name, reverse=True)
        finished_seen = 0
        for meta_file in files:
            meta: dict[str, Any] | None = None
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                meta = None

            if isinstance(meta, dict) and meta.get("status") == "running":
                continue

            finished_seen += 1
            if finished_seen <= keep:
                continue

            run_id = meta.get("run_id") if isinstance(meta, dict) else None
            candidates: list[Path] = [
                meta_file,
                meta_file.with_suffix("").with_suffix(".stop"),
                meta_file.with_suffix(".meta.tmp"),
            ]
            if isinstance(run_id, str) and run_id:
                candidates.extend(
                    [
                        meta_dir / f"worker-{run_id}.stop",
                        meta_dir / f"worker-{run_id}.jsonl",
                        meta_dir / f"worker-{run_id}.progress.log",
                    ]
                )
            if isinstance(meta, dict):
                for key in ("raw_log", "progress_log"):
                    val = meta.get(key)
                    if isinstance(val, str) and val:
                        p = Path(val)
                        if _is_within_dir(p, meta_dir):
                            candidates.append(p)

            for path in candidates:
                try:
                    if _is_within_dir(path, meta_dir):
                        path.unlink(missing_ok=True)
                except OSError:
                    pass
    except OSError:
        pass


def write_meta_atomic(meta_path: Path, payload: dict[str, Any]) -> None:
    """Atomically write run metadata JSON with brief retry for Windows file contention."""
    tmp_path = meta_path.with_suffix(".meta.tmp")
    encoded = json.dumps(payload, ensure_ascii=False, indent=2)
    for attempt in range(3):
        try:
            tmp_path.write_text(encoded, encoding="utf-8")
            os.replace(tmp_path, meta_path)
            return
        except OSError:
            if attempt < 2:
                time.sleep(0.015)


def extract_task_title(explicit_title: str | None, prompt: str) -> str:
    """Pick a concise single-line title for the monitor list."""
    if explicit_title and explicit_title.strip():
        return explicit_title.strip()[:100]
    for line in prompt.splitlines():
        clean = line.strip().lstrip("#*- ").strip()
        if clean:
            return clean[:100]
    return "OpenCode Worker Run"


def resolve_model(cli_model: str | None) -> str:
    """Resolve model from CLI flag, process env, or Windows HKCU Environment registry."""
    if cli_model and cli_model.strip():
        return cli_model.strip()

    env_val = os.environ.get("OPENCODE_WORKER_MODEL", "").strip()
    if env_val:
        return env_val

    if sys.platform == "win32":
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Environment") as key:
                reg_val, _ = winreg.QueryValueEx(key, "OPENCODE_WORKER_MODEL")
                if isinstance(reg_val, str) and reg_val.strip():
                    return reg_val.strip()
        except OSError:
            pass

    return ""


def build_run_command(
    cli_path: str,
    args: argparse.Namespace,
    model: str,
    server_url: str | None,
) -> list[str]:
    """Construct the `opencode run` command line."""
    cmd = [cli_path, "run"]
    cmd.extend(["--server", server_url] if server_url else ["--standalone"])
    cmd.extend(["--auto", "--thinking", "--format", "json"])

    if model:
        cmd.extend(["--model", model])
    if args.agent:
        cmd.extend(["--agent", args.agent])
    if args.session:
        cmd.extend(["--session", args.session])
    elif args.continue_last:
        cmd.append("--continue")
    if args.title:
        cmd.extend(["--title", args.title])
    for file_path in args.file or []:
        cmd.extend(["--file", file_path])

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


def handle_stdout_line(
    raw_line: str,
    stats: WorkerStats,
    raw_fh: TextIO,
    progress_fh: TextIO,
    lock: threading.RLock,
) -> bool:
    """Persist one stdout line to raw log and return True if worker stats changed."""
    with lock:
        raw_fh.write(raw_line)
        raw_fh.flush()

    line = raw_line.strip()
    if not line:
        return False

    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        with lock:
            stats.non_json.append(line)
        emit_progress(progress_fh, lock, f"[raw] {line[:200]}")
        return True

    changed = False
    with lock:
        stats.event_seq += 1
        if not stats.session_id and event.get("sessionID"):
            stats.session_id = str(event["sessionID"])
            emit_progress(progress_fh, lock, f"[session] {stats.session_id}")
            changed = True

        ev_type = event.get("type")
        part = event.get("part") or {}

        if ev_type == "step_start":
            stats.step_count += 1
            stats.last_step_start_seq = stats.event_seq
            stats.step_start_ms = event_ts_ms(event)
            stats.step_last_llm_end_ms = None
            stats.step_tool_intervals = []
            emit_progress(progress_fh, lock, f"[step {stats.step_count}] 推理中...")
            changed = True
        elif ev_type == "tool_use":
            stats.last_tool_seq = stats.event_seq
            tool_name = str(part.get("tool") or "unknown")
            state = part.get("state") or {}
            status = str(state.get("status") or "unknown")
            t_info = state.get("time") or {}
            t_start = t_info.get("start")
            t_end = t_info.get("end")
            if isinstance(t_start, (int, float)) and isinstance(t_end, (int, float)) and t_end > t_start:
                stats.step_tool_intervals.append((float(t_start), float(t_end)))
            preview = summarize_tool_input(state.get("input"))
            entry = f"[{status}] {tool_name}: {preview}" if preview else f"[{status}] {tool_name}"
            stats.tools.append(entry)
            emit_progress(progress_fh, lock, f"[tool] {entry}")
            changed = True
        elif ev_type == "step_finish":
            tk = part.get("tokens") or {}
            in_tok = int(tk.get("input") or 0)
            out_tok = int(tk.get("output") or 0)
            rsn_tok = int(tk.get("reasoning") or 0)
            cache = tk.get("cache") or {}
            cr_tok = int(cache.get("read") or 0)
            cw_tok = int(cache.get("write") or 0)
            stats.tokens["input"] += in_tok
            stats.tokens["output"] += out_tok
            stats.tokens["reasoning"] += rsn_tok
            stats.tokens["cache_read"] += cr_tok
            stats.tokens["cache_write"] += cw_tok
            stats.live_reasoning = ""
            stats.live_text = ""
            reason = part.get("reason") or "unknown"
            if reason == "stop":
                stats.last_stop_seq = stats.event_seq
            step_tps_str = ""
            step_out = out_tok + rsn_tok
            if stats.step_start_ms is not None:
                end_ms = event_ts_ms(event)
                total_ms = max(0.0, end_ms - stats.step_start_ms)
                # Only subtract tool execution tails that occur after LLM reasoning/text finished streaming.
                tool_clip_start = (
                    max(stats.step_start_ms, stats.step_last_llm_end_ms)
                    if stats.step_last_llm_end_ms is not None
                    else end_ms
                )
                post_llm_tool_ms = merge_interval_ms(stats.step_tool_intervals, tool_clip_start, end_ms)
                pure_ms = max(0.0, total_ms - post_llm_tool_ms)
                if step_out > 0 and pure_ms > 0:
                    stats.gen_ms += pure_ms
                    stats.last_step_tps = step_out / (pure_ms / 1000.0)
                    step_tps_str = f", tps={stats.last_step_tps:.1f}"
                stats.step_start_ms = None
                stats.step_last_llm_end_ms = None
                stats.step_tool_intervals = []
            emit_progress(
                progress_fh,
                lock,
                f"[step {stats.step_count} done] reason={reason}, in={format_token_count(in_tok)}, out={format_token_count(out_tok)}, rsn={format_token_count(rsn_tok)}, cache_read={format_token_count(cr_tok)}{step_tps_str}",
            )
            changed = True
        elif ev_type in ("reasoning", "thinking"):
            p_time = part.get("time") or {}
            p_end = p_time.get("end")
            if isinstance(p_end, (int, float)) and p_end > 0:
                if stats.step_last_llm_end_ms is None or float(p_end) > stats.step_last_llm_end_ms:
                    stats.step_last_llm_end_ms = float(p_end)
            text = str(part.get("text") or part.get("thinking") or "").strip()
            stats.live_reasoning = ""
            stats.live_reasoning_id = None
            if text:
                stats.reasonings.append(
                    {"step": stats.step_count, "time": now_hms(), "text": text}
                )
                preview = " ".join(text.splitlines())
                if len(preview) > 120:
                    preview = preview[:120] + "..."
                emit_progress(progress_fh, lock, f"[think] {preview}")
                changed = True
        elif ev_type == "text":
            p_time = part.get("time") or {}
            p_end = p_time.get("end")
            if isinstance(p_end, (int, float)) and p_end > 0:
                if stats.step_last_llm_end_ms is None or float(p_end) > stats.step_last_llm_end_ms:
                    stats.step_last_llm_end_ms = float(p_end)
            text = part.get("text")
            stats.live_text = ""
            stats.live_text_id = None
            if text:
                stats.last_text_seq = stats.event_seq
                text_str = str(text)
                stats.texts.append(text_str)
                emit_progress(progress_fh, lock, f"[output] 收到回复 ({len(text_str)} chars)")
                changed = True
        elif ev_type == "error":
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
            changed = True

    return changed


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
                _fields_ = [
                    (n, ctypes.c_uint64)
                    for n in (
                        "ReadOperationCount",
                        "WriteOperationCount",
                        "OtherOperationCount",
                        "ReadTransferCount",
                        "WriteTransferCount",
                        "OtherTransferCount",
                    )
                ]

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
            k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
            k32.SetInformationJobObject.restype = wintypes.BOOL
            k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            k32.OpenProcess.restype = wintypes.HANDLE
            k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
            k32.AssignProcessToJobObject.restype = wintypes.BOOL
            k32.CloseHandle.argtypes = [wintypes.HANDLE]

            job = k32.CreateJobObjectW(None, None)
            if not job:
                return
            info = _ExtendedLimits()
            info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not k32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
                k32.CloseHandle(job)
                return
            self._handle = job
            self._k32 = k32
        except Exception:
            self._handle = None

    def assign_pid(self, pid: int | None) -> None:
        """Bind a child PID to this job so it terminates automatically when the parent exits."""
        if not self._handle or not self._k32 or not pid:
            return
        try:
            proc_handle = self._k32.OpenProcess(0x1F0FFF, False, int(pid))
            if proc_handle:
                try:
                    self._k32.AssignProcessToJobObject(self._handle, proc_handle)
                finally:
                    self._k32.CloseHandle(proc_handle)
        except Exception:
            pass

    def close(self) -> None:
        """Close the job object handle, triggering termination of all assigned processes."""
        if self._handle and self._k32:
            try:
                self._k32.CloseHandle(self._handle)
            except Exception:
                pass
            self._handle = None


def terminate_process_tree(proc: subprocess.Popen[str] | None) -> None:
    """Terminate a subprocess (and its process tree on Windows)."""
    if proc is None or proc.poll() is not None:
        return
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
            check=False,
        )
    else:
        proc.kill()


def pick_free_port() -> int:
    """Bind an ephemeral loopback socket briefly to pick an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class PrivateOpenCodeServer:
    """Isolated per-run `opencode serve` process exposing `/api/event` SSE deltas."""

    def __init__(self, cli_path: str, workdir: Path, base_env: dict[str, str]) -> None:
        self.cli_path = cli_path
        self.workdir = workdir
        self.port = pick_free_port()
        self.password = secrets.token_urlsafe(16)
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.auth_header = "Basic " + base64.b64encode(f"opencode:{self.password}".encode("ascii")).decode("ascii")
        self.env = dict(base_env)
        self.env["OPENCODE_SERVER_PASSWORD"] = self.password
        self.proc: subprocess.Popen[str] | None = None
        self._stop = threading.Event()

    def start(self, timeout_sec: float = 15.0) -> bool:
        """Start `opencode serve` and wait until its HTTP endpoint responds."""
        self.proc = subprocess.Popen(
            [self.cli_path, "serve", "--port", str(self.port)],
            cwd=str(self.workdir),
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        ready_event = threading.Event()

        def drain_stdout() -> None:
            if not self.proc or not self.proc.stdout:
                return
            for line in self.proc.stdout:
                if "listening on" in line.lower():
                    ready_event.set()
                if self._stop.is_set():
                    break

        def drain_srv_stderr() -> None:
            if self.proc and self.proc.stderr:
                for _ in self.proc.stderr:
                    if self._stop.is_set():
                        break

        threading.Thread(target=drain_stdout, daemon=True).start()
        threading.Thread(target=drain_srv_stderr, daemon=True).start()

        deadline = time.monotonic() + timeout_sec
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        probe_req = urllib.request.Request(
            f"{self.base_url}/",
            headers={"Authorization": self.auth_header},
        )
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                return False
            if ready_event.is_set():
                return True
            try:
                with opener.open(probe_req, timeout=1.0) as resp:
                    if resp.status < 500:
                        return True
            except Exception:
                time.sleep(0.15)
        return False

    def stop(self) -> None:
        """Stop the private server process tree."""
        self._stop.set()
        terminate_process_tree(self.proc)


def start_sse_listener(
    base_url: str,
    auth_header: str,
    stats: WorkerStats,
    lock: threading.RLock,
    target_session_id: str | None,
    reset_idle_timer: Callable[[], None],
    sync_meta: Callable[[str], None],
    stop_event: threading.Event,
) -> threading.Thread:
    """Subscribe to `/api/event` and stream live reasoning/text deltas into WorkerStats."""

    def worker_loop() -> None:
        locked_sid = target_session_id
        last_sync = 0.0
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(
            f"{base_url}/api/event",
            headers={"Accept": "text/event-stream", "Authorization": auth_header},
        )
        try:
            with opener.open(req, timeout=3600) as resp:
                for raw in resp:
                    if stop_event.is_set():
                        break
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        ev = json.loads(line[5:].strip())
                    except json.JSONDecodeError:
                        continue

                    ev_type = ev.get("type") or ""
                    if not ev_type.startswith("session."):
                        continue
                    data = ev.get("data") or {}
                    ev_sid = data.get("sessionID")
                    if not locked_sid and ev_sid:
                        locked_sid = str(ev_sid)
                    if locked_sid and ev_sid and str(ev_sid) != locked_sid:
                        continue

                    reset_idle_timer()
                    need_immediate_sync = False
                    need_throttled_sync = False

                    with lock:
                        if not stats.session_id and locked_sid:
                            stats.session_id = locked_sid
                            need_immediate_sync = True

                        part_key = f"{data.get('assistantMessageID', '')}:{data.get('ordinal', 0)}"

                        if ev_type == "session.reasoning.started":
                            stats.live_reasoning = ""
                            stats.live_reasoning_id = part_key
                            need_immediate_sync = True
                        elif ev_type == "session.reasoning.delta":
                            delta = str(data.get("delta") or data.get("text") or "")
                            if delta:
                                if stats.live_reasoning_id != part_key:
                                    stats.live_reasoning = ""
                                    stats.live_reasoning_id = part_key
                                stats.live_reasoning += delta
                                need_throttled_sync = True
                        elif ev_type == "session.text.started":
                            stats.live_text = ""
                            stats.live_text_id = part_key
                            need_immediate_sync = True
                        elif ev_type == "session.text.delta":
                            delta = str(data.get("delta") or data.get("text") or "")
                            if delta:
                                if stats.live_text_id != part_key:
                                    stats.live_text = ""
                                    stats.live_text_id = part_key
                                stats.live_text += delta
                                need_throttled_sync = True

                    now = time.monotonic()
                    if need_immediate_sync or (need_throttled_sync and now - last_sync >= SSE_SYNC_INTERVAL_SEC):
                        last_sync = now
                        sync_meta("running")
        except Exception:
            pass

    thread = threading.Thread(target=worker_loop, daemon=True)
    thread.start()
    return thread


def stream_worker(
    cli_path: str,
    args: argparse.Namespace,
    resolved_model: str,
    prompt: str,
    workdir: Path,
    idle_timeout: int,
    max_timeout: int,
    run_id: str,
    raw_path: Path,
    progress_path: Path,
    meta_path: Path,
    effective_model: str,
) -> tuple[int, bool, WorkerStats]:
    """Run OpenCode CLI with live line-by-line streaming, optional SSE deltas, and idle timeout."""
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
    server_pid: int | None = None
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
                "workdir": str(workdir),
                "pids": {
                    "worker": worker_pid,
                    "server": server_pid,
                    "cli": cli_pid,
                },
                "session_id": stats.session_id,
                "step_count": stats.step_count,
                "tokens": dict(stats.tokens),
                "metrics": compute_metrics(stats),
                "tools_count": len(stats.tools),
                "last_tool": stats.tools[-1] if stats.tools else None,
                "reasonings": list(stats.reasonings[-30:]),
                "errors": list(stats.errors[-30:]),
                "live_reasoning": "" if ended_ms else stats.live_reasoning,
                "live_text": "" if ended_ms else stats.live_text,
                "task_file": str(Path(args.task_file).resolve()) if args.task_file else None,
                "task_prompt": prompt_stored,
                "progress_log": str(progress_path),
                "raw_log": str(raw_path),
                "output_text": "\n\n".join(stats.texts).strip(),
                "exit_code": exit_code,
            }
            write_meta_atomic(meta_path, payload)

    prune_old_meta(WORKER_LOG_DIR)

    private_srv: PrivateOpenCodeServer | None = None
    sse_stop = threading.Event()
    sse_thread: threading.Thread | None = None
    run_env = os.environ.copy()
    server_url = args.server

    if not server_url and not args.no_sse:
        candidate_srv = PrivateOpenCodeServer(cli_path, workdir, run_env)
        if candidate_srv.start():
            private_srv = candidate_srv
            server_url = candidate_srv.base_url
            run_env = candidate_srv.env
            if candidate_srv.proc:
                server_pid = candidate_srv.proc.pid
                job_obj.assign_pid(server_pid)
        else:
            candidate_srv.stop()

    cmd = build_run_command(cli_path, args, resolved_model, server_url)

    with (
        raw_path.open("w", encoding="utf-8", errors="replace") as raw_fh,
        progress_path.open("w", encoding="utf-8", errors="replace") as progress_fh,
    ):
        mode_label = "live-sse" if private_srv else ("server" if server_url else "standalone")
        header_lines = [
            "=== OpenCode Worker Started ===",
            f"RunID: {run_id}",
            f"BatchID: {batch_id}",
            f"Model: {effective_model}",
            f"Mode: {mode_label}",
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

        if private_srv:
            sse_thread = start_sse_listener(
                base_url=private_srv.base_url,
                auth_header=private_srv.auth_header,
                stats=stats,
                lock=lock,
                target_session_id=args.session,
                reset_idle_timer=reset_idle_timer,
                sync_meta=sync_meta,
                stop_event=sse_stop,
            )

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

        try:
            if proc.stdout:
                for raw_line in proc.stdout:
                    reset_idle_timer()
                    if handle_stdout_line(raw_line, stats, raw_fh, progress_fh, lock):
                        sync_meta("running")
            returncode = proc.wait()
        finally:
            with timer_lock:
                finished = True
                if idle_timer is not None:
                    idle_timer.cancel()
                if max_timer is not None:
                    max_timer.cancel()
            sse_stop.set()
            if private_srv:
                private_srv.stop()
            job_obj.close()
            stdin_thread.join(timeout=2)
            stderr_thread.join(timeout=2)
            if sse_thread:
                sse_thread.join(timeout=2)

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
        if stats.texts:
            with lock:
                progress_fh.write("\n=== Worker Output ===\n" + "\n\n".join(stats.texts).strip() + "\n")
                progress_fh.flush()
        sync_meta(final_key, exit_code=final_code, ended_ms=int(time.time() * 1000))

    return final_code, timed_out, stats


def main() -> int:
    """Entry point for the OpenCode worker wrapper."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Run OpenCode CLI headlessly as a subagent worker.")
    parser.add_argument("--list-models", action="store_true", help="List available OpenCode models and exit.")
    parser.add_argument("--workdir", default=".", help="Working directory for the task.")
    parser.add_argument("--task", help="Task prompt text.")
    parser.add_argument("--task-file", help="Path to a UTF-8 file containing the task prompt.")
    parser.add_argument("--model", "-m", help="Model in format provider/model#variant (defaults to OpenCode config).")
    parser.add_argument("--agent", help="Specific OpenCode agent name.")
    parser.add_argument("--session", "-s", help="Continue a specific OpenCode session ID.")
    parser.add_argument("--continue", dest="continue_last", action="store_true", help="Continue the last session.")
    parser.add_argument("--title", help="Optional session title.")
    parser.add_argument("--batch-id", help="Optional batch/group ID shared by concurrent workers in the same dispatch round.")
    parser.add_argument("--batch-title", help="Optional group title displayed for this dispatch round in OpenCode Monitor.")
    parser.add_argument("--file", "-f", action="append", help="Attach file path to the message (repeatable).")
    parser.add_argument("--server", help="Connect to an existing `opencode serve` URL instead of starting a private server.")
    parser.add_argument("--no-sse", action="store_true", help="Disable private server SSE streaming and fall back to --standalone.")
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
    parser.add_argument("--raw-log", help="Optional path to save the raw JSONL event log.")
    parser.add_argument("--progress-file", help="Optional path to write live human-readable progress log.")
    args = parser.parse_args()

    cli_path = find_opencode_cli()
    if args.list_models:
        return run_list_models(cli_path, args.server)

    workdir = Path(args.workdir).resolve()
    if not workdir.is_dir():
        print(f"Error: workdir does not exist: {workdir}", file=sys.stderr)
        return 2

    prompt = load_task_prompt(args)
    resolved_model = resolve_model(args.model)
    run_id, raw_path, progress_path, meta_path = resolve_log_paths(args.raw_log, args.progress_file)
    effective_model = resolved_model or "<opencode-default>"

    returncode, timed_out, stats = stream_worker(
        cli_path=cli_path,
        args=args,
        resolved_model=resolved_model,
        prompt=prompt,
        workdir=workdir,
        idle_timeout=args.idle_timeout,
        max_timeout=args.timeout,
        run_id=run_id,
        raw_path=raw_path,
        progress_path=progress_path,
        meta_path=meta_path,
        effective_model=effective_model,
    )

    tk = stats.tokens
    status_label = "TIMEOUT" if timed_out else ("OK" if returncode == 0 else f"ERROR (exit={returncode})")

    print("\n=== OpenCode Worker Summary ===")
    print(f"Status: {status_label}")
    if stats.errors:
        print(f"MidRunErrors ({len(stats.errors)}):")
        for item in stats.errors:
            step_label = f"step {item['step']}" if item.get("step") else "init"
            print(f"  - [{item['time']}] ({step_label}) {item['type']}: {item['message']}")
    print(f"SessionID: {stats.session_id or '<unknown>'}")
    print(f"Model: {effective_model}")
    print(f"Workdir: {workdir}")
    metrics = compute_metrics(stats)
    hit_str = f"{metrics['cache_hit_rate']:.1f}%" if metrics["cache_hit_rate"] is not None else "--"
    tps_str = f"{metrics['avg_tps']:.1f} tok/s" if metrics["avg_tps"] is not None else "--"
    print(
        "Tokens: "
        f"input={format_token_count(tk['input'])}, output={format_token_count(tk['output'])}, "
        f"reasoning={format_token_count(tk['reasoning'])}, cache_read={format_token_count(tk['cache_read'])} "
        f"(cache_hit={hit_str}, tps={tps_str})"
    )
    print(f"Tools ({len(stats.tools)}):")
    for item in stats.tools[:40]:
        print(f"  - {item}")
    if len(stats.tools) > 40:
        print(f"  ... ({len(stats.tools) - 40} more in log)")
    print(f"ProgressLog: {progress_path}")
    print(f"RawLog: {raw_path}")

    if stats.texts:
        print("\n=== Worker Output ===")
        print("\n\n".join(stats.texts).strip())
    elif stats.non_json:
        print("\n=== Raw Output ===")
        print("\n".join(stats.non_json[-30:]))

    if stats.stderr_lines and returncode != 0:
        print("\n=== Stderr ===", file=sys.stderr)
        print("\n".join(stats.stderr_lines[-30:]), file=sys.stderr)

    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
