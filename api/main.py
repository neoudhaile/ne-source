import sys
import os
import asyncio
import re

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pipeline.db import get_connection, get_lead
from api.db_queries import get_runs, get_stats
from api.models import (
    RunRecord, StatsResponse, StatusResponse,
    ConfigPayload, TriggerResponse
)
import api.pipeline_runner as runner

app = FastAPI(title="Ne'Source API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:5173'],
    allow_methods=['*'],
    allow_headers=['*'],
)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pipeline', 'config.py')


def _read_config_values() -> dict:
    import pipeline.config as cfg
    import importlib
    importlib.reload(cfg)
    return {
        'industries': list(cfg.INDUSTRIES),
        'cities': list(cfg.CITIES),
        'min_reviews': cfg.MIN_REVIEWS,
        'min_rating': cfg.MIN_RATING,
        'geo_radius_miles': cfg.GEO_RADIUS_MILES,
        'max_leads_per_run': cfg.MAX_LEADS_PER_RUN,
    }


def _write_config_list(varname: str, items: list[str]):
    with open(CONFIG_PATH, 'r') as f:
        content = f.read()
    formatted = '[\n' + ''.join(f"    '{item}',\n" for item in items) + ']'
    pattern = rf'{varname}\s*=\s*\[.*?\]'
    new_content = re.sub(pattern, f'{varname} = {formatted}', content, flags=re.DOTALL)
    with open(CONFIG_PATH, 'w') as f:
        f.write(new_content)


def _write_config_scalar(varname: str, value):
    with open(CONFIG_PATH, 'r') as f:
        content = f.read()
    pattern = rf'^({varname}\s*=\s*).*$'
    new_content = re.sub(pattern, rf'\g<1>{value}', content, flags=re.MULTILINE)
    with open(CONFIG_PATH, 'w') as f:
        f.write(new_content)


def _get_next_run_at() -> str | None:
    try:
        import subprocess
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        for line in result.stdout.splitlines():
            if 'pipeline.py' in line and not line.startswith('#'):
                return line.strip()
    except Exception:
        pass
    return None


# ── Routes ──────────────────────────────────────────────────────────────────

@app.post('/api/runs', response_model=TriggerResponse)
async def trigger_run():
    if runner.is_running:
        raise HTTPException(status_code=409, detail='A run is already in progress')
    run_id = await runner.start_run()
    return TriggerResponse(run_id=run_id)


@app.get('/api/runs', response_model=list[RunRecord])
def list_runs(limit: int = 20):
    conn = get_connection()
    try:
        return get_runs(conn, limit)
    finally:
        conn.close()


@app.get('/api/runs/{run_id}', response_model=RunRecord)
def get_run(run_id: int):
    conn = get_connection()
    try:
        rows = get_runs(conn, limit=100)
        for row in rows:
            if row['id'] == run_id:
                return row
        raise HTTPException(status_code=404, detail='Run not found')
    finally:
        conn.close()


@app.get('/api/stats', response_model=StatsResponse)
def stats():
    conn = get_connection()
    try:
        return get_stats(conn)
    finally:
        conn.close()


@app.get('/api/status', response_model=StatusResponse)
def status():
    return StatusResponse(
        is_running=runner.is_running,
        active_run_id=runner.active_run_id,
        next_run_at=_get_next_run_at(),
    )


@app.get('/api/leads')
def list_leads(limit: int = 50, offset: int = 0):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            'SELECT * FROM smb_leads ORDER BY created_at DESC LIMIT %s OFFSET %s',
            (limit, offset)
        )
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return [_serialize_lead(dict(zip(cols, row))) for row in rows]
    finally:
        cur.close()
        conn.close()


@app.get('/api/leads/{lead_id}')
def get_lead_detail(lead_id: int):
    lead = get_lead(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail='Lead not found')
    return _serialize_lead(lead)


def _serialize_lead(lead: dict) -> dict:
    """Convert non-JSON-serializable types (datetime, Decimal) to strings."""
    import datetime
    from decimal import Decimal
    out = {}
    for k, v in lead.items():
        if isinstance(v, datetime.datetime):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


@app.get('/api/config')
def get_config():
    return _read_config_values()


@app.put('/api/config')
def update_config(payload: ConfigPayload):
    if payload.industries is not None:
        _write_config_list('INDUSTRIES', payload.industries)
    if payload.cities is not None:
        _write_config_list('CITIES', payload.cities)
    if payload.min_reviews is not None:
        _write_config_scalar('MIN_REVIEWS', payload.min_reviews)
    if payload.min_rating is not None:
        _write_config_scalar('MIN_RATING', payload.min_rating)
    if payload.geo_radius_miles is not None:
        _write_config_scalar('GEO_RADIUS_MILES', payload.geo_radius_miles)
    if payload.max_leads_per_run is not None:
        _write_config_scalar('MAX_LEADS_PER_RUN', payload.max_leads_per_run)
    return _read_config_values()


# ── WebSocket ────────────────────────────────────────────────────────────────

@app.websocket('/ws/runs/{run_id}')
async def ws_run(websocket: WebSocket, run_id: int):
    await websocket.accept()

    queue = runner.active_queues.get(run_id)
    if queue is None:
        await websocket.close(code=1008)
        return

    async def keepalive():
        while True:
            await asyncio.sleep(15)
            try:
                await websocket.send_json({'type': 'ping'})
            except Exception:
                break

    keepalive_task = asyncio.create_task(keepalive())

    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
            if event.get('type') in ('done', 'error'):
                break

    except WebSocketDisconnect:
        pass
    finally:
        keepalive_task.cancel()
