/**
 * DeepSeek Harness (dsh) Worker Monitor sidecar backend.
 *
 * Scans %TEMP%/dsh-worker-logs/ for metadata (`worker-*.meta.json`) and live
 * progress streams written by `run_dsh_worker.py`, groups runs by dispatch
 * batch, and exposes stop & cleanup endpoints.
 */
import { execFileSync } from 'node:child_process';
import {
  appendFileSync,
  closeSync,
  existsSync,
  openSync,
  readdirSync,
  readFileSync,
  readSync,
  renameSync,
  statSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SidecarApp, Response } from 'sidecar_sdk';

const __dirname = dirname(fileURLToPath(import.meta.url));
const UI_PATH = join(__dirname, 'ui.html');
const LOG_DIR = join(tmpdir(), 'dsh-worker-logs');
const RESOLVED_LOG_DIR = resolve(LOG_DIR);
const MAX_PROGRESS_BYTES = 512 * 1024;
const MAX_RUNS_SCANNED = 80;
const MAX_GROUPS_LISTED = 25;
const META_RE = /^worker-.+\.meta\.json$/;
const SAFE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{2,100}$/;

function isWithinLogDir(targetPath) {
  if (!targetPath) return false;
  try {
    const base = process.platform === 'win32' ? RESOLVED_LOG_DIR.toLowerCase() : RESOLVED_LOG_DIR;
    const resolved = resolve(String(targetPath));
    const norm = process.platform === 'win32' ? resolved.toLowerCase() : resolved;
    return norm.startsWith(base + sep);
  } catch {
    return false;
  }
}

function safeUnlinkInLogDir(targetPath) {
  if (!isWithinLogDir(targetPath)) return false;
  try {
    if (existsSync(targetPath)) {
      unlinkSync(targetPath);
      return true;
    }
  } catch {
    // Ignore contention or already deleted files.
  }
  return false;
}

function killPidTree(pid) {
  const num = Number(pid);
  if (!Number.isInteger(num) || num <= 0) return false;
  try {
    if (process.platform === 'win32') {
      execFileSync('taskkill', ['/F', '/T', '/PID', String(num)], { stdio: 'ignore' });
    } else {
      process.kill(num, 'SIGTERM');
    }
    return true;
  } catch {
    return false;
  }
}

function formatHms(date = new Date()) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function stopWorkerRun(runId) {
  if (!runId || !SAFE_ID_RE.test(String(runId))) {
    return { ok: false, error: 'invalid runId' };
  }
  const metaPath = join(LOG_DIR, `worker-${runId}.meta.json`);
  const stopPath = join(LOG_DIR, `worker-${runId}.stop`);
  if (!existsSync(metaPath)) {
    return { ok: false, error: 'run not found' };
  }

  let meta;
  try {
    meta = JSON.parse(readFileSync(metaPath, 'utf-8'));
  } catch {
    return { ok: false, error: 'failed to read run metadata' };
  }

  try {
    writeFileSync(stopPath, String(Date.now()), 'utf-8');
  } catch {
    // Best-effort marker.
  }

  const pids = meta.pids || {};
  const killed = [];
  for (const key of ['cli', 'server', 'worker']) {
    if (killPidTree(pids[key])) {
      killed.push(`${key}:${pids[key]}`);
    }
  }

  if (meta.progress_log && existsSync(meta.progress_log)) {
    try {
      appendFileSync(meta.progress_log, `[${formatHms()}] [stopped] 用户已手动停止任务\n`, 'utf-8');
    } catch {
      // Ignore log append contention.
    }
  }

  meta.status = 'stopped';
  meta.ended_ms = Date.now();
  meta.updated_at = new Date().toISOString().slice(0, 19);
  meta.live_reasoning = '';
  meta.live_text = '';

  const tmpPath = `${metaPath}.stop.tmp`;
  try {
    writeFileSync(tmpPath, JSON.stringify(meta, null, 2), 'utf-8');
    renameSync(tmpPath, metaPath);
  } catch {
    // Ignore transient rename failure.
  }

  return { ok: true, runId, status: 'stopped', killed };
}

function deleteRunArtifacts(meta) {
  const runId = meta?.run_id;
  if (!runId || !SAFE_ID_RE.test(String(runId))) return false;
  const candidates = [
    join(LOG_DIR, `worker-${runId}.meta.json`),
    join(LOG_DIR, `worker-${runId}.stop`),
    join(LOG_DIR, `worker-${runId}.meta.tmp`),
    join(LOG_DIR, `worker-${runId}.meta.json.stop.tmp`),
    join(LOG_DIR, `worker-${runId}.jsonl`),
    join(LOG_DIR, `worker-${runId}.progress.log`),
  ];
  if (meta.raw_log) candidates.push(meta.raw_log);
  if (meta.progress_log) candidates.push(meta.progress_log);

  let removedAny = false;
  for (const p of new Set(candidates)) {
    if (safeUnlinkInLogDir(p)) removedAny = true;
  }
  return removedAny;
}

function safeStat(filePath) {
  try {
    return statSync(filePath);
  } catch {
    return null;
  }
}

function readTailText(filePath, maxBytes = MAX_PROGRESS_BYTES) {
  const st = safeStat(filePath);
  if (!st || !st.isFile() || st.size === 0) return '';
  const len = Math.min(st.size, maxBytes);
  const offset = st.size - len;
  const buf = Buffer.allocUnsafe(len);
  const fd = openSync(filePath, 'r');
  try {
    readSync(fd, buf, 0, len, offset);
  } finally {
    closeSync(fd);
  }
  let text = buf.toString('utf-8');
  if (offset > 0) {
    const firstNl = text.indexOf('\n');
    if (firstNl !== -1) text = text.slice(firstNl + 1);
  }
  return text;
}

function extractProgressLines(progressPath) {
  if (!progressPath || !existsSync(progressPath)) return [];
  const raw = readTailText(progressPath);
  if (!raw) return [];
  const lines = raw.split(/\r?\n/);
  const outputMarkerIdx = lines.indexOf('=== Worker Output ===');
  const sliced = outputMarkerIdx === -1 ? lines : lines.slice(0, outputMarkerIdx);
  return sliced.filter((line) => line.trim().length > 0);
}

function scanMetaEntries(limit = MAX_RUNS_SCANNED) {
  if (!existsSync(LOG_DIR)) return [];
  let names = [];
  try {
    names = readdirSync(LOG_DIR).filter((name) => META_RE.test(name));
  } catch {
    return [];
  }
  names.sort((a, b) => b.localeCompare(a));

  const entries = [];
  for (const name of names) {
    if (limit > 0 && entries.length >= limit) break;
    const fullPath = join(LOG_DIR, name);
    const st = safeStat(fullPath);
    if (!st || !st.isFile()) continue;
    try {
      const parsed = JSON.parse(readFileSync(fullPath, 'utf-8'));
      if (parsed && parsed.run_id) {
        entries.push({
          meta: parsed,
          sig: `${name}:${Math.round(st.mtimeMs)}:${st.size}`,
        });
      }
    } catch {
      // Skip briefly contended or malformed meta files.
    }
  }
  return entries;
}

function getBatchId(meta) {
  const bid = meta && typeof meta.batch_id === 'string' ? meta.batch_id.trim() : '';
  return bid || String(meta?.run_id || '');
}

function toRunSummary(meta) {
  return {
    run_id: meta.run_id,
    batch_id: getBatchId(meta),
    batch_title: meta.batch_title || meta.title || 'DSH Worker Run',
    title: meta.title || 'DSH Worker Run',
    status: meta.status || 'unknown',
    started_at: meta.started_at,
    started_ms: meta.started_ms || 0,
    ended_ms: meta.ended_ms || null,
    model: meta.model || '<dsh-default>',
    workdir: meta.workdir || '',
    session_id: meta.session_id || null,
    step_count: meta.step_count || 0,
    tools_count: meta.tools_count || 0,
    tokens: meta.tokens || {},
  };
}

function buildGroups(entries) {
  const byBatch = new Map();
  for (const entry of entries) {
    const bid = getBatchId(entry.meta);
    if (!byBatch.has(bid)) byBatch.set(bid, []);
    byBatch.get(bid).push(entry);
  }

  const groups = [];
  for (const [batchId, members] of byBatch.entries()) {
    // Keep natural dispatch order left-to-right inside each batch tab bar.
    members.sort((a, b) => {
      const diff = (a.meta.started_ms || 0) - (b.meta.started_ms || 0);
      return diff !== 0 ? diff : String(a.meta.run_id).localeCompare(String(b.meta.run_id));
    });

    const runSummaries = members.map(({ meta }) => toRunSummary(meta));
    let runningCount = 0;
    let okCount = 0;
    let stoppedCount = 0;
    let errorCount = 0;
    let maxEnded = 0;
    let firstTitle = '';

    for (const r of runSummaries) {
      if (!firstTitle && r.batch_title) firstTitle = r.batch_title;
      if (r.status === 'running') runningCount += 1;
      else if (r.status === 'ok') okCount += 1;
      else if (r.status === 'stopped') stoppedCount += 1;
      else if (r.status === 'error' || r.status === 'timeout') errorCount += 1;
      if (r.ended_ms && r.ended_ms > maxEnded) maxEnded = r.ended_ms;
    }

    let status = 'unknown';
    if (runningCount > 0) status = 'running';
    else if (errorCount > 0) status = 'error';
    else if (stoppedCount > 0) status = 'stopped';
    else if (okCount === runSummaries.length) status = 'ok';

    const earliest = runSummaries[0];
    groups.push({
      batch_id: batchId,
      batch_title: firstTitle || earliest.title || 'DSH 任务组',
      status,
      started_at: earliest.started_at,
      started_ms: earliest.started_ms || 0,
      ended_ms: runningCount > 0 ? null : (maxEnded || null),
      total_count: runSummaries.length,
      running_count: runningCount,
      ok_count: okCount,
      stopped_count: stoppedCount,
      error_count: errorCount,
      runs: runSummaries,
      _entries: members,
    });
  }

  groups.sort((a, b) => {
    const diff = (b.started_ms || 0) - (a.started_ms || 0);
    return diff !== 0 ? diff : String(b.batch_id).localeCompare(String(a.batch_id));
  });

  return groups.slice(0, MAX_GROUPS_LISTED);
}

function stopBatch(batchId) {
  if (!batchId || !SAFE_ID_RE.test(String(batchId))) {
    return { ok: false, error: 'invalid batchId' };
  }
  const entries = scanMetaEntries(0);
  const targets = entries.filter(
    (e) => getBatchId(e.meta) === batchId && e.meta.status === 'running',
  );
  const results = targets.map((e) => stopWorkerRun(e.meta.run_id));
  return {
    ok: true,
    batchId,
    stoppedCount: results.filter((r) => r.ok).length,
    results,
  };
}

function deleteBatch(batchId) {
  if (!batchId || !SAFE_ID_RE.test(String(batchId))) {
    return { ok: false, error: 'invalid batchId' };
  }
  const entries = scanMetaEntries(0);
  const members = entries.filter((e) => getBatchId(e.meta) === batchId);
  if (!members.length) {
    return { ok: false, error: 'batch not found' };
  }
  if (members.some((e) => e.meta.status === 'running')) {
    return { ok: false, error: '该任务组中仍有运行中的任务，请先停止后再清理' };
  }

  let deletedRuns = 0;
  for (const e of members) {
    if (deleteRunArtifacts(e.meta)) deletedRuns += 1;
  }
  return { ok: true, batchId, deletedRuns };
}

function clearHistory() {
  const entries = scanMetaEntries(0);
  let deletedRuns = 0;
  let keptRunning = 0;
  for (const e of entries) {
    if (e.meta.status === 'running') {
      keptRunning += 1;
      continue;
    }
    if (deleteRunArtifacts(e.meta)) deletedRuns += 1;
  }
  return { ok: true, deletedRuns, keptRunning };
}

function buildSnapshot(requestedBatchId, requestedRunId, clientEtag) {
  const entries = scanMetaEntries();
  if (!entries.length) {
    const etag = 'empty';
    if (clientEtag === etag) return { unchanged: true, etag };
    return {
      etag,
      logDir: LOG_DIR,
      groups: [],
      runs: [],
      selectedGroupId: '',
      selected: null,
      progressLines: [],
    };
  }

  const groupsWithEntries = buildGroups(entries);
  let selectedGroup =
    (requestedBatchId && groupsWithEntries.find((g) => g.batch_id === requestedBatchId)) ||
    (requestedRunId &&
      groupsWithEntries.find((g) => g.runs.some((r) => r.run_id === requestedRunId))) ||
    groupsWithEntries[0];

  const targetEntry =
    (requestedRunId &&
      selectedGroup._entries.find((e) => e.meta.run_id === requestedRunId)) ||
    selectedGroup._entries.find((e) => e.meta.status === 'running') ||
    selectedGroup._entries[0];

  const progressPath = targetEntry.meta.progress_log || '';
  const pStat = progressPath ? safeStat(progressPath) : null;
  const pSig = pStat ? `${Math.round(pStat.mtimeMs)}:${pStat.size}` : 'none';
  const etag = `${entries.map((e) => e.sig).join('|')}::${selectedGroup.batch_id}::${targetEntry.meta.run_id}::${pSig}`;

  if (clientEtag && clientEtag === etag) {
    return { unchanged: true, etag };
  }

  const groups = groupsWithEntries.map(({ _entries, ...rest }) => rest);
  const runs = entries.map(({ meta }) => toRunSummary(meta));
  const progressLines = extractProgressLines(progressPath);

  return {
    etag,
    logDir: LOG_DIR,
    groups,
    runs,
    selectedGroupId: selectedGroup.batch_id,
    selected: targetEntry.meta,
    progressLines,
  };
}

const app = new SidecarApp();

app.page('/', () => {
  const html = readFileSync(UI_PATH, 'utf-8');
  return new Response(html, {
    contentType: 'text/html; charset=utf-8',
    headers: {
      'Cache-Control': 'no-store, no-cache, must-revalidate',
      Pragma: 'no-cache',
      Expires: '0',
    },
  });
});

app.api(
  '/api/health',
  () => ({ ok: true, logDir: LOG_DIR, now: Date.now() }),
  'GET',
);

app.api(
  '/api/runs',
  (data) => buildSnapshot(data.batchId || '', data.runId || '', data.etag || ''),
  'GET',
);

app.api(
  '/api/stop',
  (data) => stopWorkerRun(data.runId || ''),
  'POST',
);

app.api(
  '/api/stop-batch',
  (data) => stopBatch(data.batchId || ''),
  'POST',
);

app.api(
  '/api/delete-batch',
  (data) => deleteBatch(data.batchId || ''),
  'POST',
);

app.api(
  '/api/clear-history',
  () => clearHistory(),
  'POST',
);

app.run();
