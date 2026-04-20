import asyncio
import sys
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pipeline.run import run_pipeline, run_csv_pipeline
from pipeline.db import get_connection, set_leads_run_id
from api.db_queries import create_run, update_run

executor = ThreadPoolExecutor(max_workers=1)

is_running = False
is_paused = False
active_run_id: int | None = None
active_queues: dict[int, asyncio.Queue] = {}
pause_event = threading.Event()
pause_event.set()
active_emit = None

# ---------- per-run log storage (in-memory, last N runs) ----------
MAX_STORED_RUNS = 10
run_logs: dict[int, list[dict]] = {}
_run_log_order: list[int] = []


def _store_event(run_id: int, event: dict):
    if run_id not in run_logs:
        run_logs[run_id] = []
        _run_log_order.append(run_id)
        while len(_run_log_order) > MAX_STORED_RUNS:
            old = _run_log_order.pop(0)
            run_logs.pop(old, None)
    run_logs[run_id].append(event)


def get_run_logs(run_id: int) -> list[dict] | None:
    return run_logs.get(run_id)


def _wait_if_paused():
    while not pause_event.is_set():
        time.sleep(0.2)


def pause_run() -> bool:
    global is_paused
    if not is_running or is_paused:
        return False
    is_paused = True
    pause_event.clear()
    if active_emit is not None:
        active_emit({'type': 'paused'})
    return True


def resume_run() -> bool:
    global is_paused
    if not is_running or not is_paused:
        return False
    is_paused = False
    pause_event.set()
    if active_emit is not None:
        active_emit({'type': 'resumed'})
    return True


async def start_run() -> int:
    global is_running, is_paused, active_run_id, active_emit

    conn = get_connection()
    run_id = create_run(conn)
    conn.close()

    queue: asyncio.Queue = asyncio.Queue()
    active_queues[run_id] = queue
    active_run_id = run_id
    is_running = True
    is_paused = False
    pause_event.set()

    loop = asyncio.get_running_loop()

    # Track final event data so DB update happens in runner, not WebSocket handler
    run_result: dict = {}

    def emit(event):
        event['run_id'] = run_id
        _store_event(run_id, event)
        if event.get('type') == 'done':
            run_result.update(event)
        loop.call_soon_threadsafe(queue.put_nowait, event)

    active_emit = emit

    async def _run():
        global is_running, is_paused, active_run_id, active_emit
        try:
            await loop.run_in_executor(
                executor,
                lambda: run_pipeline(emit=emit, run_id=run_id, wait_if_paused=_wait_if_paused),
            )
            # Update DB from runner — independent of WebSocket connection
            conn2 = get_connection()
            update_run(conn2, run_id,
                       status='completed',
                       finished_at=datetime.now(timezone.utc),
                       inserted=run_result.get('inserted', 0),
                       skipped_geo=run_result.get('skipped_geo', 0),
                       skipped_dupe=run_result.get('skipped_dupe', 0),
                       total_leads=run_result.get('total_leads'))
            conn2.close()
        except Exception as e:
            conn2 = get_connection()
            update_run(conn2, run_id,
                       status='failed',
                       finished_at=datetime.now(timezone.utc),
                       error_message=str(e))
            conn2.close()
            # Push error event to queue so WebSocket sees it
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {'type': 'error', 'run_id': run_id, 'message': str(e)}
            )
        finally:
            is_running = False
            is_paused = False
            active_run_id = None
            active_emit = None
            pause_event.set()
            active_queues.pop(run_id, None)

    asyncio.create_task(_run())
    return run_id


async def start_csv_run(lead_ids: list[int]) -> int:
    global is_running, is_paused, active_run_id, active_emit

    conn = get_connection()
    run_id = create_run(conn)
    set_leads_run_id(conn, lead_ids, run_id)
    conn.close()

    queue: asyncio.Queue = asyncio.Queue()
    active_queues[run_id] = queue
    active_run_id = run_id
    is_running = True
    is_paused = False
    pause_event.set()

    loop = asyncio.get_running_loop()
    run_result: dict = {}

    def emit(event):
        event['run_id'] = run_id
        _store_event(run_id, event)
        if event.get('type') == 'done':
            run_result.update(event)
        loop.call_soon_threadsafe(queue.put_nowait, event)

    active_emit = emit

    async def _run():
        global is_running, is_paused, active_run_id, active_emit
        try:
            await loop.run_in_executor(
                executor,
                lambda: run_csv_pipeline(
                    lead_ids,
                    emit=emit,
                    run_id=run_id,
                    wait_if_paused=_wait_if_paused,
                ),
            )
            conn2 = get_connection()
            update_run(conn2, run_id,
                       status='completed',
                       finished_at=datetime.now(timezone.utc),
                       inserted=run_result.get('inserted', 0),
                       skipped_geo=0,
                       skipped_dupe=0,
                       total_leads=run_result.get('total_leads'))
            conn2.close()
        except Exception as e:
            conn2 = get_connection()
            update_run(conn2, run_id,
                       status='failed',
                       finished_at=datetime.now(timezone.utc),
                       error_message=str(e))
            conn2.close()
            loop.call_soon_threadsafe(
                queue.put_nowait,
                {'type': 'error', 'run_id': run_id, 'message': str(e)}
            )
        finally:
            is_running = False
            is_paused = False
            active_run_id = None
            active_emit = None
            pause_event.set()
            active_queues.pop(run_id, None)

    asyncio.create_task(_run())
    return run_id
