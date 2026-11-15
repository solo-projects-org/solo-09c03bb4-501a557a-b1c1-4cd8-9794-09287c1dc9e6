"""Local receiving end for complete daily NH3-N observation files.

Exposes only complete daily files (all 24 hourly UTC slots). Repeated delivery
of the same revision is idempotent; a stale revision can never overwrite a
newer stored one. Used by the analysis group's local archive.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional

from . import store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("receiver")

DB_PATH = os.environ.get("RECEIVER_DB_PATH", "/data/receiver.db")


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    store.init_db(DB_PATH)
    log.info("receiver ready, db=%s", DB_PATH)
    yield


app = FastAPI(title="Local receiving end (analysis archive)", version="1.0",
              lifespan=lifespan)


class DeliveryBody(BaseModel):
    section: str
    date: str
    revision: int
    complete: bool = False
    row_count: Optional[int] = None
    digest: Optional[str] = None
    published_at: Optional[str] = None
    published_by: Optional[str] = None
    delivered_by: Optional[str] = None
    rows: list[dict]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "service": "receiver"}


@app.post("/api/deliveries")
def receive_delivery(body: DeliveryBody, request: Request) -> JSONResponse:
    payload = body.model_dump()
    remote = request.client.host if request.client else None
    try:
        result = store.receive_day(DB_PATH, payload, remote_addr=remote)
    except store.DeliveryRejected as exc:
        log.warning("delivery rejected: %s %s", exc.error, exc.message)
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": {"error": exc.error, "message": exc.message}},
        )
    log.info("delivery %s: %s", result["status"], result)
    return JSONResponse(status_code=200, content=result)


@app.get("/api/days/{section}/{date}")
def get_day(section: str, date: str) -> dict:
    day = store.get_day(DB_PATH, section, date)
    if day is None:
        raise HTTPException(
            status_code=404,
            detail="no complete daily file for this section/date",
        )
    return day


@app.get("/api/days")
def list_days(limit: int = 200) -> list[dict]:
    return store.list_days(DB_PATH, limit=min(limit, 1000))


@app.get("/api/deliveries")
def list_deliveries(limit: int = 100) -> list[dict]:
    return store.list_delivery_audit(DB_PATH, limit=min(limit, 1000))


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


PAGE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>分析组本地接收端 · 完整日档</title>
<style>
  body { font-family: system-ui, "Segoe UI", sans-serif; margin: 2rem; background: #f6f8fa; color: #1f2933; }
  h1 { font-size: 1.2rem; }
  .panel { background:#fff; border:1px solid #d9e2ec; border-radius:8px; padding:1rem 1.2rem; margin-bottom:1.2rem; }
  table { border-collapse: collapse; width:100%; font-size:.88rem; }
  th, td { border:1px solid #d9e2ec; padding:.3rem .55rem; text-align:left; }
  th { background:#eef2f6; }
  .muted { color:#627d98; font-size:.85rem; }
  .ok { background:#d1f0dd; } .bad { background:#f8d4d4; }
</style>
</head>
<body>
<h1>分析组本地接收端 — 只接收并公布完整日档（24 个整点）</h1>
<p class="muted">同版重复投递幂等；旧修订不会覆盖新修订；同日新修订整日原子替换。</p>
<div class="panel"><h3>已归档日档</h3><div id="days"></div></div>
<div class="panel"><h3>最近投递审计</h3><div id="audit"></div></div>
<script>
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function load() {
  const days = await (await fetch('/api/days')).json();
  document.getElementById('days').innerHTML = days.length
    ? '<table><tr><th>断面</th><th>日期</th><th>修订号</th><th>行数</th><th>接收时间(UTC)</th></tr>' +
      days.map(d => `<tr><td>${esc(d.section)}</td><td>${esc(d.date)}</td><td>${d.revision}</td>` +
        `<td>${d.row_count}</td><td>${esc(d.received_at)}</td></tr>`).join('') + '</table>'
    : '<p class="muted">暂无日档</p>';
  const audit = await (await fetch('/api/deliveries?limit=30')).json();
  document.getElementById('audit').innerHTML = audit.length
    ? '<table><tr><th>时间(UTC)</th><th>断面</th><th>日期</th><th>修订号</th><th>结果</th><th>说明</th></tr>' +
      audit.map(a => `<tr class="${a.outcome.startsWith('rejected') ? 'bad' : 'ok'}">` +
        `<td>${esc(a.received_at)}</td><td>${esc(a.section)}</td><td>${esc(a.date)}</td>` +
        `<td>${a.revision}</td><td>${esc(a.outcome)}</td><td>${esc(a.detail ?? '')}</td></tr>`).join('') + '</table>'
    : '<p class="muted">暂无投递</p>';
}
load();
</script>
</body>
</html>
"""
