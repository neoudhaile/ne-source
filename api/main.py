import sys
import os
import asyncio
import re

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pipeline.db import get_connection, get_lead, get_tier1_leads_for_run
from pipeline.csv_import import import_csv
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


@app.get('/api/runs/{run_id}/tier1-export')
def export_tier1_leads(run_id: int):
    leads = get_tier1_leads_for_run(run_id)
    if not leads:
        raise HTTPException(status_code=404, detail='No Tier 1 leads found for this run')

    lines = [
        f'# Tier 1 Outreach Brief — Run #{run_id}',
        '',
    ]
    for lead in leads:
        phone = lead.get('best_phone') or 'No phone found'
        services = lead.get('services_offered') or []
        certifications = lead.get('certifications') or []
        description = lead.get('company_description') or f'{lead.get("industry") or "Business"} in {lead.get("city") or "unknown location"}.'

        if isinstance(services, list):
            services_text = ', '.join(str(s) for s in services[:6]) if services else '—'
        else:
            services_text = str(services) if services else '—'

        if isinstance(certifications, list):
            certs_text = ', '.join(str(s) for s in certifications[:6]) if certifications else '—'
        else:
            certs_text = str(certifications) if certifications else '—'

        lines.extend([
            f'## {lead.get("company") or "Unknown company"}',
            f'Tier: Tier 1',
            f'Tier Reason: {lead.get("tier_reason") or "—"}',
            f'Phone: {phone}',
            f'Email: {lead.get("best_email") or "—"}',
            f'Website: {lead.get("website") or "—"}',
            f'LinkedIn: {lead.get("owner_linkedin") or "—"}',
            f'Location: {", ".join(part for part in [lead.get("city"), lead.get("state")] if part) or "—"}',
            f'Industry: {lead.get("industry") or "—"}',
            f'Employees: {lead.get("employee_count") or "—"}',
            f'Year Established: {lead.get("year_established") or "—"}',
            f'Revenue: {lead.get("revenue_estimate") or "—"}',
            '',
            '### Company Summary',
            description,
            '',
            '### Services',
            services_text,
            '',
            '### Certifications',
            certs_text,
            '',
            '### Reviews',
            lead.get('review_summary') or '—',
            '',
            '### Links',
            f'- Google Maps: {lead.get("google_maps_url") or "—"}',
            f'- Facebook: {lead.get("facebook_url") or "—"}',
            f'- Yelp: {lead.get("yelp_url") or "—"}',
            '',
        ])

    filename = f'tier1-run-{run_id}.md'
    return PlainTextResponse(
        '\n'.join(lines),
        media_type='text/markdown',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


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
        is_paused=runner.is_paused,
        active_run_id=runner.active_run_id,
        next_run_at=_get_next_run_at(),
    )


@app.post('/api/runs/pause', response_model=StatusResponse)
def pause_run():
    if not runner.is_running:
        raise HTTPException(status_code=409, detail='No run is in progress')
    if not runner.pause_run():
        raise HTTPException(status_code=409, detail='Run is already paused')
    return status()


@app.post('/api/runs/resume', response_model=StatusResponse)
def resume_run():
    if not runner.is_running:
        raise HTTPException(status_code=409, detail='No run is in progress')
    if not runner.resume_run():
        raise HTTPException(status_code=409, detail='Run is not paused')
    return status()


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


# ── CSV Upload ───────────────────────────────────────────────────────────────

@app.post('/api/upload-csv')
async def upload_csv(file: UploadFile = File(...)):
    """
    Upload a CSV of leads. Claude maps columns to the DB schema automatically.
    Inserts rows synchronously, then kicks off enrichment + email gen via
    WebSocket (same real-time feed as normal pipeline runs).
    Returns {run_id, inserted, skipped, total, mapping} immediately.
    """
    import logging
    logger = logging.getLogger('uvicorn.error')

    logger.info(f'CSV upload started: {file.filename}')

    if not file.filename or not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail='File must be a .csv')

    content = await file.read()
    logger.info(f'CSV read: {len(content)} bytes')

    if len(content) > 10 * 1024 * 1024:  # 10MB limit
        raise HTTPException(status_code=400, detail='File too large (max 10MB)')

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, import_csv, content)
        logger.info(f'CSV import done: inserted={result["inserted"]}, skipped={result["skipped"]}')
    except ValueError as e:
        logger.error(f'CSV import ValueError: {e}')
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f'CSV import error: {e}')
        raise HTTPException(status_code=500, detail=f'Import failed: {e}')

    run_id = None
    if result['lead_ids']:
        # Kick off enrichment + email gen in background via WebSocket
        run_id = await runner.start_csv_run(result['lead_ids'])
        logger.info(f'CSV enrichment kicked off: run_id={run_id}')
    else:
        logger.info('CSV import produced no new leads; skipping enrichment run')

    return {
        'run_id': run_id,
        'inserted': result['inserted'],
        'skipped': result['skipped'],
        'total': result['total'],
        'mapping': result['mapping'],
        'message': 'No new leads were inserted from this CSV.' if not result['lead_ids'] else None,
    }


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
            try:
                await websocket.send_json(event)
            except RuntimeError:
                # Client disconnected — stop sending but don't crash
                break
            if event.get('type') in ('done', 'error'):
                break

    except WebSocketDisconnect:
        pass
    finally:
        keepalive_task.cancel()
