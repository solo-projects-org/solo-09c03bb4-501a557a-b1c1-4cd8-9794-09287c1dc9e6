"""FastAPI web service: trigger/inspect collections and read published days."""
from __future__ import annotations

import logging
import os
import socket
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from . import db, sync
from .collector import CollectionError, collect_day
from .receiver_client import ReceiverClient
from .station_client import StationClient, StationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("web")

DB_PATH = os.environ.get("DB_PATH", "/data/quality.db")
STATION_URL = os.environ.get("STATION_URL", "http://station:8000")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://receiver:8000")
SYNC_ENABLED = os.environ.get("SYNC_ENABLED", "1") not in ("0", "false", "False")
WORKER = os.environ.get("WORKER_NAME") or f"{socket.gethostname()}-{os.getpid()}"

# Background collection threads per process; the DB unique index is the real
# cross-process guard, this is just local bookkeeping.
_threads: dict[int, threading.Thread] = {}
_sender: sync.BackgroundSender | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    db.init_db(DB_PATH)
    recovered = db.recover_interrupted(DB_PATH, worker=WORKER)
    if recovered:
        log.warning("marked %s interrupted run(s) from a previous worker life", recovered)
    released = db.release_worker_claims(DB_PATH, WORKER)
    if released:
        log.warning("released %s delivery claim(s) from a previous life", released)
    global _sender
    if SYNC_ENABLED:
        _sender = sync.BackgroundSender(DB_PATH, RECEIVER_URL, WORKER)
        _sender.start()
    log.info(
        "worker %s ready, db=%s station=%s receiver=%s sync=%s",
        WORKER, DB_PATH, STATION_URL, RECEIVER_URL, SYNC_ENABLED,
    )
    yield
    if _sender is not None:
        _sender.stop()


app = FastAPI(title="Section water quality collector", version="1.0", lifespan=lifespan)


class CollectBody(BaseModel):
    section: str = Field(..., examples=["SECTION-A"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])


def _run_collection(run_id: int, section: str, date_str: str) -> None:
    client = StationClient(STATION_URL)
    try:
        result = collect_day(DB_PATH, run_id, section, date_str, client, WORKER)
    except CollectionError as exc:
        log.error("run %s failed: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, str(exc))
    except StationError as exc:
        log.error("run %s station error: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, f"station error: {exc}")
    except Exception as exc:  # defensive: never leave a run dangling
        log.exception("run %s unexpected error", run_id)
        db.fail_run(DB_PATH, run_id, f"unexpected error: {exc}")
    else:
        # The outbox was written in the publish transaction; just wake the
        # sender. If we crashed before this line the poller picks it up anyway.
        if _sender is not None:
            _sender.kick()
        log.info("run %s published: %s", run_id, result)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "worker": WORKER}


@app.get("/api/sections")
def api_sections() -> dict:
    try:
        return {"sections": StationClient(STATION_URL).list_sections()}
    except StationError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/collect")
def api_collect(body: CollectBody) -> JSONResponse:
    try:
        run_id = db.start_run(DB_PATH, body.section, body.date, WORKER)
    except db.ActiveRunError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    thread = threading.Thread(
        target=_run_collection,
        args=(run_id, body.section, body.date),
        name=f"collect-{run_id}",
        daemon=True,
    )
    _threads[run_id] = thread
    thread.start()
    return JSONResponse(
        status_code=202,
        content={
            "run_id": run_id,
            "section": body.section,
            "date": body.date,
            "worker": WORKER,
            "status": "running",
        },
    )


@app.get("/api/runs/{run_id}")
def api_run(run_id: int) -> dict:
    run = db.get_run(DB_PATH, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@app.get("/api/runs")
def api_runs(section: str, date: str) -> list[dict]:
    return db.list_runs(DB_PATH, section, date)


@app.get("/api/published")
def api_published(section: str, date: str) -> dict:
    published = db.get_published(DB_PATH, section, date)
    if published is None:
        raise HTTPException(
            status_code=404,
            detail="no complete published version yet for this section/day",
        )
    return published


@app.get("/api/sync")
def api_sync(section: str, date: str) -> dict:
    """Sync state of the *current* published revision to the local receiver."""
    return sync.sync_state(DB_PATH, section, date, RECEIVER_URL)


@app.get("/api/deliveries")
def api_deliveries(limit: int = 100) -> list[dict]:
    return db.list_outbox(DB_PATH, limit=min(limit, 500))


@app.post("/api/sync/retry")
def api_sync_retry(body: CollectBody) -> JSONResponse:
    """Retry sending the currently published revision of a day right now."""
    revision = db.get_published_revision(DB_PATH, body.section, body.date)
    if revision is None:
        raise HTTPException(
            status_code=404,
            detail="nothing published for this section/day; nothing to sync",
        )
    try:
        db.ensure_delivery(DB_PATH, body.section, body.date, revision)
    except Exception as exc:  # defensive: surface the DB problem, day is intact
        raise HTTPException(status_code=500, detail=f"cannot queue delivery: {exc}")
    try:
        db.retry_delivery(DB_PATH, body.section, body.date, revision)
    except db.DeliveryNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except db.StaleRevisionError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    client = ReceiverClient(RECEIVER_URL)
    try:
        result = sync.deliver_once(
            DB_PATH, body.section, body.date, revision, WORKER, client
        )
    except sync.DeliveryBusyError as exc:
        return JSONResponse(
            status_code=409,
            content={
                "status": "busy",
                "revision": revision,
                "detail": str(exc),
                "sync": sync.sync_state(DB_PATH, body.section, body.date, RECEIVER_URL),
            },
        )
    except sync.DeliveryError as exc:
        # The local published day is untouched; report the delivery failure.
        return JSONResponse(
            status_code=502,
            content={
                "status": "failed",
                "revision": revision,
                "detail": str(exc),
                "sync": sync.sync_state(DB_PATH, body.section, body.date, RECEIVER_URL),
            },
        )
    if _sender is not None:
        _sender.kick()
    return JSONResponse(
        status_code=200,
        content={
            "status": result["status"],
            "revision": revision,
            "acknowledgment": result.get("acknowledgment"),
            "sync": sync.sync_state(DB_PATH, body.section, body.date, RECEIVER_URL),
        },
    )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


PAGE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>断面水质采集台 · 氨氮逐时观测</title>
<style>
  body { font-family: system-ui, "Segoe UI", sans-serif; margin: 2rem; background: #f6f8fa; color: #1f2933; }
  h1 { font-size: 1.3rem; }
  .panel { background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 1rem 1.2rem; margin-bottom: 1.2rem; }
  label { margin-right: .4rem; font-weight: 600; }
  select, input { padding: .35rem .5rem; border: 1px solid #9fb3c8; border-radius: 6px; }
  button { padding: .4rem 1rem; border: 0; border-radius: 6px; background: #0b6e4f; color: #fff; cursor: pointer; }
  button:disabled { background: #94a3b8; cursor: default; }
  table { border-collapse: collapse; width: 100%; font-size: .9rem; }
  th, td { border: 1px solid #d9e2ec; padding: .35rem .6rem; text-align: left; }
  th { background: #eef2f6; }
  .muted { color: #627d98; font-size: .85rem; }
  .tag { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .8rem; }
  .running { background: #fff3cd; } .success { background: #d1f0dd; }
  .failed { background: #f8d4d4; } .interrupted { background: #e1d5f7; }
  .pending { background: #fff3cd; } .sending { background: #dbeafe; }
  .sent { background: #d1f0dd; } .superseded { background: #e5e7eb; }
  .rejected { background: #f8d4d4; }
  #msg { white-space: pre-wrap; font-family: ui-monospace, monospace; font-size: .85rem; }
</style>
</head>
<body>
<h1>断面水质采集台 — 逐时氨氮（NH₃-N）观测</h1>

<div class="panel">
  <label>断面
    <select id="section"></select>
  </label>
  <label>UTC 日期
    <input id="date" type="date" required>
  </label>
  <button id="collectBtn">开始采集并整日发布</button>
  <button id="viewBtn">查看已发布版本</button>
  <p class="muted">采集为后台任务：逐页核对修订号，收齐总行数并确认源端修订未变后才整日发布；修订变化或读取失败时保留旧版。</p>
</div>

<div class="panel">
  <h3>采集任务</h3>
  <div id="runs"></div>
</div>

<div class="panel">
  <h3>已发布观测（时刻 / 数值 / 来源页 / 修订号）</h3>
  <div id="published"></div>
</div>

<div class="panel">
  <h3>同步到分析组本地接收端</h3>
  <div id="syncState"></div>
  <button id="retryBtn">重试同步当前修订</button>
  <p class="muted">整日观测发布时会在同一事务里登记待送修订。两个 Web 实例均可发送/重试；网络中断或进程重启后会自动补送。发送失败不影响本地已发布日档。</p>
  <div id="deliveries"></div>
</div>

<div class="panel">
  <h3>消息</h3>
  <div id="msg" class="muted"></div>
</div>

<script>
const $ = id => document.getElementById(id);
const msg = t => { $('msg').textContent = t; };
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

async function loadSections() {
  const r = await fetch('/api/sections');
  if (!r.ok) { msg('无法获取断面列表：' + r.status); return; }
  const data = await r.json();
  $('section').innerHTML = data.sections.map(s => `<option>${esc(s)}</option>`).join('');
}

function params() {
  return new URLSearchParams({ section: $('section').value, date: $('date').value });
}

async function startCollect() {
  const date = $('date').value;
  if (!date) { msg('请选择 UTC 日期'); return; }
  $('collectBtn').disabled = true;
  try {
    const r = await fetch('/api/collect', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ section: $('section').value, date })
    });
    const data = await r.json();
    if (!r.ok) { msg('启动失败 (' + r.status + '): ' + JSON.stringify(data.detail)); return; }
    msg('任务已启动 run_id=' + data.run_id + ' worker=' + data.worker);
    pollRun(data.run_id);
  } finally {
    $('collectBtn').disabled = false;
  }
}

async function pollRun(runId) {
  const timer = setInterval(async () => {
    const r = await fetch('/api/runs/' + runId);
    const run = await r.json();
    await renderRuns();
    await renderSync();
    if (run.status !== 'running') {
      clearInterval(timer);
      msg('任务结束：' + run.status + (run.error ? ' — ' + run.error : ''));
      await renderPublished();
      await renderSync();
    }
  }, 700);
}

async function renderRuns() {
  const r = await fetch('/api/runs?' + params());
  const runs = await r.json();
  if (!runs.length) { $('runs').innerHTML = '<p class="muted">暂无任务</p>'; return; }
  $('runs').innerHTML = `<table><tr><th>run</th><th>worker</th><th>状态</th><th>修订号</th>
    <th>行数</th><th>页尝试</th><th>开始(UTC)</th><th>结束(UTC)</th><th>错误</th></tr>` +
    runs.map(r => `<tr><td>${r.id}</td><td>${esc(r.worker)}</td>
      <td><span class="tag ${r.status}">${r.status}</span></td>
      <td>${r.revision ?? '—'}</td><td>${r.rows_collected}</td><td>${r.page_attempts}</td>
      <td>${esc(r.started_at)}</td><td>${esc(r.finished_at ?? '—')}</td>
      <td>${esc(r.error ?? '')}</td></tr>`).join('') + '</table>';
}

async function renderPublished() {
  const r = await fetch('/api/published?' + params());
  if (r.status === 404) { $('published').innerHTML = '<p class="muted">尚无完整发布版本（不完整采集不会公布）。</p>'; return; }
  const d = await r.json();
  $('published').innerHTML =
    `<p>修订号 <b>${d.revision}</b> · 发布于 ${esc(d.published_at)} · 发布者 ${esc(d.published_by)} · 共 ${d.rows.length} 行</p>` +
    '<table><tr><th>时刻(UTC)</th><th>NH₃-N (mg/L)</th><th>来源页</th><th>修订号</th></tr>' +
    d.rows.map(x => `<tr><td>${esc(x.time)}</td><td>${x.nh3n}</td><td>${x.source_page}</td><td>${x.revision}</td></tr>`).join('') +
    '</table>';
}

const SYNC_LABELS = {
  synced: '已同步', pending: '待发送', failed: '发送失败',
  rejected: '接收端拒绝', not_queued: '未登记待送',
  nothing_published: '尚未发布'
};

async function renderSync() {
  const date = $('date').value;
  if (!date || !$('section').value) { $('syncState').innerHTML = ''; return; }
  let r;
  try { r = await fetch('/api/sync?' + params()); } catch (e) { return; }
  if (!r.ok) { $('syncState').innerHTML = '<p class="muted">无法读取同步状态 (' + r.status + ')</p>'; return; }
  const s = await r.json();
  const state = s.sync_state;
  const cur = s.current_delivery;
  const receiver = !s.receiver_reachable
    ? `<span class="muted">接收端不可达：${esc(s.receiver_error || '')}</span>`
    : (s.receiver_revision == null
        ? '接收端：无该日档'
        : `接收端：修订号 <b>${s.receiver_revision}</b>` +
          (s.receiver_matches ? '（与本地一致）' : '（与本地不一致）'));

  let html = `<p>本地已发布修订号：<b>${s.published_revision ?? '—'}</b>` +
    (s.published_rows != null ? `（${s.published_rows} 行）` : '') +
    ` · 同步状态：<span class="tag ${state === 'synced' ? 'sent' : state}">${SYNC_LABELS[state] || state}</span>` +
    ` · ${receiver}</p>`;
  if (cur && cur.last_error) {
    html += `<p class="muted">最近错误：${esc(cur.last_error)}</p>`;
  }
  if (s.deliveries.length) {
    html += '<table><tr><th>修订号</th><th>投递状态</th><th>尝试</th><th>最后尝试(UTC)</th>' +
      '<th>成功时间(UTC)</th><th>发送者</th><th>错误</th></tr>' +
      s.deliveries.map(d => `<tr><td>${d.revision}</td>` +
        `<td><span class="tag ${d.status}">${d.status}</span></td>` +
        `<td>${d.attempts}</td><td>${esc(d.last_attempt_at ?? '—')}</td>` +
        `<td>${esc(d.sent_at ?? '—')}</td><td>${esc(d.sent_by ?? '—')}</td>` +
        `<td>${esc(d.last_error ?? '')}</td></tr>`).join('') + '</table>';
  } else {
    html += '<p class="muted">暂无待送/已送修订。</p>';
  }
  $('syncState').innerHTML = html;
  $('retryBtn').disabled = !(s.published_revision != null && state !== 'synced');
}

async function retrySync() {
  const date = $('date').value;
  if (!date) { msg('请选择 UTC 日期'); return; }
  $('retryBtn').disabled = true;
  try {
    const r = await fetch('/api/sync/retry', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ section: $('section').value, date })
    });
    const data = await r.json();
    if (!r.ok) {
      msg('同步失败 (' + r.status + ')：' + (data.detail || JSON.stringify(data)));
    } else {
      msg('同步结果：' + data.status + '，修订号 ' + data.revision);
    }
    await renderSync();
    await renderDeliveries();
  } finally {
    $('retryBtn').disabled = false;
  }
}

async function renderDeliveries() {
  const r = await fetch('/api/deliveries?limit=20');
  if (!r.ok) { $('deliveries').innerHTML = ''; return; }
  const rows = await r.json();
  if (!rows.length) { $('deliveries').innerHTML = '<p class="muted">尚无任何待送修订。</p>'; return; }
  $('deliveries').innerHTML = '<h4>全部投递队列（最近 20 条）</h4>' +
    '<table><tr><th>断面</th><th>日期</th><th>修订号</th><th>状态</th><th>尝试</th><th>错误</th></tr>' +
    rows.map(d => `<tr><td>${esc(d.section)}</td><td>${esc(d.date)}</td><td>${d.revision}</td>` +
      `<td><span class="tag ${d.status}">${d.status}</span></td><td>${d.attempts}</td>` +
      `<td>${esc(d.last_error ?? '')}</td></tr>`).join('') + '</table>';
}

$('collectBtn').onclick = startCollect;
$('retryBtn').onclick = retrySync;
$('viewBtn').onclick = async () => { await renderRuns(); await renderPublished(); await renderSync(); await renderDeliveries(); };
$('section').onchange = async () => { await renderRuns(); await renderPublished(); await renderSync(); };
$('date').onchange = async () => { await renderRuns(); await renderPublished(); await renderSync(); };
$('date').value = new Date().toISOString().slice(0, 10);
let liveTimer = setInterval(async () => {
  if ($('date').value && $('section').value) { await renderSync(); await renderDeliveries(); }
}, 3000);
loadSections().then(renderRuns).then(renderPublished).then(renderSync).then(renderDeliveries);
</script>
</body>
</html>
"""
