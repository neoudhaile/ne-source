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
from api.db_queries import create_run, update_run, get_runs, get_stats, get_leads_by_run_id
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


@app.get('/api/runs/{run_id}/logs')
def download_run_logs(run_id: int):
    logs = runner.get_run_logs(run_id)
    if logs is None:
        raise HTTPException(status_code=404, detail='No logs found for this run (logs are kept in memory for the last 10 runs)')

    lines: list[str] = []
    for ev in logs:
        lines.append(_format_log_event(ev))

    filename = f'run-{run_id}.log'
    return PlainTextResponse(
        '\n'.join(lines),
        media_type='text/plain',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


def _format_log_event(ev: dict) -> str:
    t = ev.get('type', '?')
    company = ev.get('company', '')
    step = ev.get('step', '')

    if t == 'done':
        return f"Run complete — Inserted: {ev.get('inserted', 0)} | Geo: {ev.get('skipped_geo', 0)} | Dupes: {ev.get('skipped_dupe', 0)}"
    if t == 'error':
        return f"ERROR: {ev.get('message', ev.get('error', '?'))}"
    if t == 'tier_result':
        return f"{company} → {ev.get('tier', '?')} · {ev.get('tier_reason', '')}"
    if t == 'tier_done':
        return f"Tiering complete — {ev.get('kept', 0)} kept, {ev.get('removed', 0)} removed"
    if t == 'tier_start':
        return f"Tiering {ev.get('count', '?')} leads..."
    if t == 'enrich_start':
        return f"Enriching {ev.get('count', '?')} leads..."
    if t == 'enrich_done':
        return f"Enrichment complete — {ev.get('count', ev.get('total', '?'))} leads enriched"
    if t == 'enrich_lead_done':
        sources = ', '.join(ev.get('sources', [])) or 'no sources'
        return f"Enriched {company} ({ev.get('index', '?')}/{ev.get('total', '?')}) — {sources}"
    if t == 'enrich_step_start':
        return f"⟳ {company} → {step}"
    if t == 'enrich_step_done':
        fields = ', '.join(ev.get('fields_filled', []))
        elapsed = ev.get('elapsed', 0)
        cost = ev.get('cost', 0)
        if fields:
            return f"✓ {company} → {step} ({elapsed}s, ${cost:.3f}) — {fields}"
        return f"✓ {company} → {step} ({elapsed}s, ${cost:.3f})"
    if t == 'enrich_step_skip':
        return f"· {company} → {step} — {ev.get('reason', ev.get('message', 'skipped'))}"
    if t == 'generate_done':
        return f"Generated email for {company}"
    if t == 'generate_start':
        return f"Generating emails for {ev.get('count', '?')} leads..."
    if t == 'search_start':
        return f"Searching: {ev.get('query', '?')} in {ev.get('city', '?')}"
    if t == 'search_done':
        return f"Search complete — {ev.get('count', '?')} results"
    if t == 'search_batch':
        return f"Batch: +{ev.get('batch_inserted', 0)} inserted, {ev.get('batch_rejected', 0)} rejected"
    if t == 'csv_start':
        return f"CSV import pipeline started"
    if t == 'csv_lead':
        return f"+ {company} — {ev.get('tier', '?')} · {ev.get('industry', 'Unknown')}"
    if t == 'insufficient_funds':
        return f"Insufficient funds — balance: ${ev.get('balance', 0):.2f}, estimated: ${ev.get('estimated_cost', 0):.2f}"
    if t == 'paused':
        return "Run paused"
    if t == 'resumed':
        return "Run resumed"
    if t == 'export_start':
        return f"Notion export starting — {ev.get('count', '?')} leads"
    if t == 'export_lead':
        return f"Exported {company} ({ev.get('index', '?')}/{ev.get('total', '?')})"
    if t == 'export_skip':
        return f"Skipped lead · {ev.get('reason', ev.get('message', ''))}"
    if t == 'export_done':
        return f"Notion export complete — {ev.get('exported', 0)} exported, {ev.get('skipped', 0)} skipped, {ev.get('errors', 0)} errors"
    if t == 'export_error':
        return f"Notion export error: {ev.get('error', ev.get('message', '?'))}"

    # Fallback: dump the event type and any message/detail
    msg = ev.get('message', ev.get('detail', ''))
    return f"[{t}] {company} {msg}".strip()


@app.get('/api/runs/{run_id}/tier1-export')
def export_tier1_leads(run_id: int):
    leads = get_tier1_leads_for_run(run_id)
    if not leads:
        raise HTTPException(status_code=404, detail='No tiered leads found for this run')

    lines = [
        f'# Outreach Brief — Run #{run_id}',
        '',
    ]
    def _fmt_list(val):
        if isinstance(val, list):
            return ', '.join(str(s) for s in val) if val else '—'
        return str(val) if val else '—'

    def _fmt(val):
        if val is None or val == '':
            return '—'
        return str(val)

    for lead in leads:
        location_parts = [lead.get('address'), lead.get('city'), lead.get('state'), lead.get('zipcode')]
        location = ', '.join(p for p in location_parts if p) or '—'

        lines.extend([
            f'## {lead.get("company") or "Unknown company"}',
            '',
            '### Classification',
            f'- **Tier:** {(lead.get("tier") or "unknown").replace("_", " ").title()}',
            f'- **Tier Reason:** {_fmt(lead.get("tier_reason"))}',
            f'- **Industry:** {_fmt(lead.get("industry"))}',
            f'- **Ownership Type:** {_fmt(lead.get("ownership_type"))}',
            '',
            '### Contact — Owner',
            f'- **Owner Name:** {_fmt(lead.get("owner_name"))}',
            f'- **Owner Email:** {_fmt(lead.get("owner_email"))}',
            f'- **Owner Phone:** {_fmt(lead.get("owner_phone"))}',
            f'- **Owner LinkedIn:** {_fmt(lead.get("owner_linkedin"))}',
            '',
            '### Contact — Company',
            f'- **Email:** {_fmt(lead.get("email"))}',
            f'- **Phone:** {_fmt(lead.get("phone"))}',
            f'- **Website:** {_fmt(lead.get("website"))}',
            '',
            '### Location',
            f'- **Address:** {location}',
            f'- **Latitude:** {_fmt(lead.get("latitude"))}',
            f'- **Longitude:** {_fmt(lead.get("longitude"))}',
            f'- **Distance (mi):** {_fmt(lead.get("distance_miles"))}',
            '',
            '### Company Details',
            f'- **Employee Count:** {_fmt(lead.get("employee_count"))}',
            f'- **Year Established:** {_fmt(lead.get("year_established"))}',
            f'- **Revenue Estimate:** {_fmt(lead.get("revenue_estimate"))}',
            f'- **Rating:** {_fmt(lead.get("rating"))}',
            f'- **Review Count:** {_fmt(lead.get("review_count"))}',
            f'- **Key Staff:** {_fmt_list(lead.get("key_staff"))}',
            '',
            '### Company Summary',
            lead.get('company_description') or f'{lead.get("industry") or "Business"} in {lead.get("city") or "unknown location"}.',
            '',
            '### Services',
            _fmt_list(lead.get('services_offered')),
            '',
            '### Certifications',
            _fmt_list(lead.get('certifications')),
            '',
            '### Reviews',
            _fmt(lead.get('review_summary')),
            '',
            '### Links',
            f'- **Google Maps:** {_fmt(lead.get("google_maps_url"))}',
            f'- **Facebook:** {_fmt(lead.get("facebook_url"))}',
            f'- **Yelp:** {_fmt(lead.get("yelp_url"))}',
            '',
            '### Generated Outreach',
            f'- **Subject:** {_fmt(lead.get("generated_subject"))}',
            '',
            lead.get('generated_email') or '—',
            '',
        ])

    filename = f'leads-run-{run_id}.md'
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


@app.get('/api/runs/{run_id}/leads')
def get_run_leads(run_id: int):
    conn = get_connection()
    try:
        leads = get_leads_by_run_id(conn, run_id)
        return [_serialize_lead(lead) for lead in leads]
    finally:
        conn.close()


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
