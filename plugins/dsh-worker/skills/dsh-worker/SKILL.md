---
name: dsh-worker
description: >-
  Delegate code implementation, refactoring, multi-file exploration, test writing,
  and command execution to DeepSeek's official CLI (`dsh headless --json`) as an
  external subagent worker. Activate when the user asks to delegate work to
  DeepSeek / DSH / DeepSeek CLI / DeepSeek Harness (saying "use DeepSeek" defaults
  to DSH unless OpenCode is explicitly requested), or wants Antigravity to plan
  and review while DeepSeek official CLI executes to save primary model quota.
---

# DeepSeek Harness External Worker (`dsh-worker`)

Use this skill to split work between **Antigravity (Architect & Reviewer)** and **DeepSeek Harness CLI (`dsh headless`) (Execution Worker)** so heavy multi-turn reading, editing, and test-fixing run on DeepSeek's official harness rather than Antigravity's primary model quota.

## 1. Division of Responsibilities

- **Antigravity (Planner & Reviewer)**:
  1. Focus strictly on **architectural thinking, root-cause reasoning, task planning, and final quality review**.
  2. Read only the minimal entry files / directory tree needed to understand the architecture.
  3. Write a self-contained **Task Spec** to a temporary UTF-8 markdown file (e.g., `<scratch>/dsh_task.md`).
  4. Dispatch the task via [run_dsh_worker.py](./scripts/run_dsh_worker.py).
  5. Verify results afterward using a single consolidated command (`git status` / `git diff --stat` / validation checks).
  - **Hard Quota Guardrails (严禁主模型干苦力杂活)**:
    - **NEVER** use the primary model to serially scan large source files or run 5+ consecutive `read_file` / `Select-String` calls in one turn.
    - Keep pre-plan read-only checks to **at most 2–3 targeted tool calls**. Dispatch a DSH Worker investigation task if deeper code archaeology is needed.
    - Consolidate post-worker verification into **1 combined command** whenever possible.

- **DSH Worker (Executor)**:
  - Runs `dsh headless --json --approval-policy never` in the target `--workdir` and performs full codebase search, file reads, multi-file edits, command execution, and test-driven bug fixing autonomously.

## 2. Helper Script Usage

The wrapper script is located at:
`~/.gemini/config/plugins/dsh-worker/skills/dsh-worker/scripts/run_dsh_worker.py` (PowerShell: `"$HOME\.gemini\config\plugins\dsh-worker\skills\dsh-worker\scripts\run_dsh_worker.py"`)

### Inspect Configured Models
Reads `~/.dsh/profiles/headless/cordis.patch.yml` to show the active default model, reasoning effort, and available provider models:
```powershell
python "$HOME\.gemini\config\plugins\dsh-worker\skills\dsh-worker\scripts\run_dsh_worker.py" --list-models
```

### Dispatch a New Task
Write the Task Spec to a UTF-8 markdown file first (passed via stdin `-` to `dsh headless`, avoiding PowerShell quoting/newline issues) and share clickable links to both the task file and `--progress-file` with the user:
```powershell
python "$HOME\.gemini\config\plugins\dsh-worker\skills\dsh-worker\scripts\run_dsh_worker.py" `
  --workdir "<project-root>" `
  --task-file "<path-to-task.md>" `
  --progress-file "<path-to-progress.log>"
```

Optional flags:
- `--mode "<native|ptc>"` / `--ptc`: Tool execution mode. Defaults to `native` (best for simple single-file tasks). For **complex multi-file edits, refactors, or multi-step codebase exploration**, pass `--mode ptc` (or `--ptc`), which injects `--patch "~/.dsh/patches/ptc.yml"` **before** `--profile headless` so the model composes TypeScript programmatic tool calls (~40% wall time / ~67% tokens on multi-file workloads).
- `--ptc-patch "<path>"`: Override the default `~/.dsh/patches/ptc.yml` path when `--mode ptc` is enabled.
- `--patch "<path>"`: Repeatable extra launcher `--patch` YAML file (always placed before `--profile headless`, composable with `--mode ptc` and `--model`).
- `--model "<provider/model#effort>"`: Override `agent-default-model` via a temporary `--patch` YAML (placed before `--profile headless`; e.g. `deepseek/deepseek-reasoner#high`). Omit to read `DSH_WORKER_MODEL` or the default configured in `~/.dsh/profiles/headless/cordis.patch.yml`.
- `--title "<short-title>"`: Concise label for this worker (displayed in `DSH Monitor` tabs).
- `--batch-id "<batch-id>"`: Group ID shared by all concurrent workers launched in the same dispatch round (alphanumeric/`_`/`-`, e.g. `batch-20261121-160000`). Defaults to `run_id` for single tasks.
- `--batch-title "<group-title>"`: Human-readable title for the dispatch round shown in the `DSH Monitor` group dropdown.
- `--progress-file "<path>"`: Live timestamped human-readable progress file flushed line-by-line during execution.
- `--raw-log "<path>"`: Raw NDJSON event log path.
- `--session "<SessionID>"`: Continue a persisted `dsh` session (`--session-id`).
- `--permission-mode "<preset>"`: Defaults to `danger-full-access` (matching OpenCode's direct shell execution and avoiding Windows Restricted-Token sandbox `0xC0000142` errors on `pwsh`); can be overridden to `workspace-write` or `read-only`.
- `--approval-policy "<never|ask>"`: Defaults to `never` so headless execution never hangs waiting for interactive approval.
- `--add-dir "<path>"`: Repeatable extra writable directory (`--add-dir`) when using `workspace-write`.
- `--idle-timeout <seconds>`: Abort only if no output arrives for N seconds (default `900`; `0` disables).
- `--timeout <seconds>`: Optional total runtime limit (default `0` = unlimited while active).

### Parallel Multi-Worker Dispatch (多 DSH 并发派发)
When a user's request can be decomposed into independent sub-tasks (e.g., disjoint modules/files, independent test suites, or parallel read-only codebase/log investigations), proactively propose a parallel split during planning and dispatch **2–6 workers concurrently** (cap: 6 per round) to save wall-clock time:
1. **Strict Write Isolation**: Assign each worker non-overlapping files/directories and explicitly state **Owned Scope** and **Do Not Touch** in each `--task-file`. Never let two concurrent workers edit the same file or run conflicting installs/builds in the same directory.
2. **Group Under One Batch**: Pass the **same** `--batch-id` and `--batch-title` to every worker in the round, with a distinct `--title`, `--task-file`, and `--progress-file` per worker. `DSH Monitor` will group them into one round with horizontal worker tabs and one-click batch stop/cleanup.
3. **Launch Together**: Call multiple `run_command` invocations in parallel in the same turn with `WaitMsBeforeAsync: 500`, then wait for completion notifications and perform a single consolidated verification.

### Follow-Up / Fix Issues in the Same Context
If review reveals small issues, reuse the `SessionID` printed in the worker summary so `dsh` continues in the same persisted session without re-reading the codebase:
```powershell
python "$HOME\.gemini\config\plugins\dsh-worker\skills\dsh-worker\scripts\run_dsh_worker.py" `
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
