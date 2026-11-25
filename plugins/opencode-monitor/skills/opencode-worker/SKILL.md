---
name: opencode-worker
description: >-
  Delegate code implementation, refactoring, multi-file exploration, test writing,
  and command execution to the local OpenCode CLI (`opencode-cli.exe`) as an
  external subagent worker. Activate ONLY when the user explicitly asks to use
  OpenCode (`opencode` / `opencode-worker`). Do NOT activate when the user only
  mentions DeepSeek or DSH (those route to `dsh-worker`).
---

# OpenCode External Worker (`opencode-worker`)

Use this skill to split work between **Antigravity (Architect & Reviewer)** and **OpenCode CLI (Execution Worker)** so heavy multi-turn reading, editing, and test-fixing consume OpenCode's configured model quota instead of Antigravity's.

## 1. Division of Responsibilities

- **Antigravity (Planner & Reviewer)**:
  1. Focus strictly on **architectural thinking, root-cause reasoning, task planning, and final quality review** — that is what the primary model is valuable for.
  2. Read only the minimal entry files / directory tree needed to understand the architecture.
  3. Write a self-contained **Task Spec** to a temporary task file (e.g., `<scratch>/opencode_task.md`).
  4. Dispatch the task via [run_worker.py](./scripts/run_worker.py).
  5. Verify results afterward using a single consolidated command (`git status` / `git diff --stat` / validation checks).
  - **Hard Quota Guardrails (严禁主模型干苦力杂活)**:
    - **NEVER** use the primary model as a grep/file-reader to serially scan large source files (e.g., 10,000+ line vendor scripts) or run 5+ consecutive `read_file` / `Select-String` calls in one turn. Each serial tool turn re-bills the entire context window + high-effort thinking tokens and will burn through weekly quota.
    - Keep pre-plan read-only checks to **at most 2–3 targeted tool calls**. If deeper multi-file code archaeology or log investigation is needed, formulate hypotheses first and dispatch an OpenCode Worker investigation/implementation task to do the heavy reading.
    - Consolidate post-worker verification into **1 combined command** whenever possible.

- **OpenCode Worker (Executor)**:
  - Performs full codebase search, file reads, multi-file edits, command execution, and test-driven bug fixing autonomously.

## 2. Helper Script Usage

The wrapper script is located at:
`~/.gemini/config/plugins/opencode-monitor/skills/opencode-worker/scripts/run_worker.py` (PowerShell: `"$HOME\.gemini\config\plugins\opencode-monitor\skills\opencode-worker\scripts\run_worker.py"`)

### Inspect Available Models & Variants
Run before choosing a model if the user updated OpenCode models or thinking variants:
```powershell
python "$HOME\.gemini\config\plugins\opencode-monitor\skills\opencode-worker\scripts\run_worker.py" --list-models
```

### Dispatch a New Task
Write the Task Spec to a UTF-8 markdown file first (to avoid PowerShell quoting/newline issues) and share clickable links to both the task file and `--progress-file` with the user before/when launching so they can inspect the prompt and live progress:
```powershell
python "$HOME\.gemini\config\plugins\opencode-monitor\skills\opencode-worker\scripts\run_worker.py" `
  --workdir "<project-root>" `
  --task-file "<path-to-task.md>" `
  --progress-file "<path-to-progress.log>"
```

Optional flags:
- `--model "<provider/model#variant>"`: Override model and thinking effort variant (e.g. `deepseek/deepseek-reasoner#high`). Omit to read `OPENCODE_WORKER_MODEL` (process env or Windows `HKCU\Environment`) or OpenCode's default model.
- `--title "<short-title>"`: Concise label for this worker (displayed in monitor tabs).
- `--batch-id "<batch-id>"`: Group ID shared by all concurrent workers launched in the same dispatch round (alphanumeric/`_`/`-`, e.g. `batch-20261119-133000`). Defaults to `run_id` for single tasks.
- `--batch-title "<group-title>"`: Human-readable title for the dispatch round shown in the `OpenCode Monitor` group dropdown.
- `--progress-file "<path>"`: Live timestamped human-readable progress file flushed line-by-line during execution.
- `--raw-log "<path>"`: Raw JSONL event log path.
- `--file "<path>"`: Attach key reference files (repeatable).
- `--idle-timeout <seconds>`: Abort only if no output arrives for N seconds (default `900`; `0` disables).
- `--timeout <seconds>`: Optional total runtime limit (default `0` = unlimited while active).
- `--server "<url>"`: Connect to a running `opencode serve` instance instead of `--standalone`.

### Parallel Multi-Worker Dispatch (多 Worker 并发派发)
When a user's request can be decomposed into independent sub-tasks (e.g., disjoint modules/files, independent test suites, or parallel read-only codebase/log investigations), proactively propose a parallel split during planning and dispatch **2–6 workers concurrently** (cap: 6 per round) to save wall-clock time:
1. **Strict Write Isolation**: Assign each worker non-overlapping files/directories and explicitly state **Owned Scope** and **Do Not Touch** in each `--task-file`. Never let two concurrent workers edit the same file or run conflicting installs/builds in the same directory (use `git worktree` or sequential execution if edits overlap).
2. **Group Under One Batch**: Pass the **same** `--batch-id` and `--batch-title` to every worker in the round, with a distinct `--title`, `--task-file`, and `--progress-file` per worker. `OpenCode Monitor` will group them into one round with horizontal worker tabs and one-click batch stop/cleanup.
3. **Launch Together**: Call multiple `run_command` invocations in parallel in the same turn with `WaitMsBeforeAsync: 500` (each `run_worker.py` automatically allocates its own private port and `opencode serve` process), then wait for completion notifications and perform a single consolidated verification.

### Follow-Up / Fix Issues in the Same Context
If review reveals small issues, reuse the `SessionID` printed in the worker summary so OpenCode continues without re-reading the codebase:
```powershell
python "$HOME\.gemini\config\plugins\opencode-monitor\skills\opencode-worker\scripts\run_worker.py" `
  --workdir "<project-root>" `
  --session "<SessionID>" `
  --task-file "<path-to-followup.md>"
```

## 3. Task Spec Template

Include these sections in `--task-file` so the worker succeeds in one pass:
1. **Goal**: Clear description of what to build/fix/refactor.
2. **Scope & Relevant Files**: Known entry points, target directories, or files not to touch.
3. **Requirements & Constraints**: Interfaces, edge cases, coding conventions, dependency limits.
4. **Verification**: Exact test/build/lint commands the worker must run and pass before finishing.
5. **Language & Final Report**: Remind the worker to perform its chain-of-thought reasoning and final summary in Simplified Chinese, listing changed files, commands run, and test results concisely.

## 4. Safety & Escalation

- When modifying an existing repo, prefer checking `git status` before delegation so pre-existing dirty files are noted in the spec.
- Always inspect `git diff --stat` (and critical diffs) after the worker exits; never trust summary text alone.
- If a worker fails to resolve an issue after 2 follow-up `--session` attempts, inspect the raw log path printed in the summary and take over directly.
