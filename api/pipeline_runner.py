import asyncio
import sys
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from pipeline.run import run_pipeline
from pipeline.db import get_connection
from api.db_queries import create_run, update_run

executor = ThreadPoolExecutor(max_workers=1)

is_running = False
active_run_id: int | None = None
active_queues: dict[int, asyncio.Queue] = {}


async def start_run() -> int:
    global is_running, active_run_id

    conn = get_connection()
    run_id = create_run(conn)
    conn.close()

    queue: asyncio.Queue = asyncio.Queue()
    active_queues[run_id] = queue
    active_run_id = run_id
    is_running = True

    loop = asyncio.get_running_loop()

    # Track final event data so DB update happens in runner, not WebSocket handler
    run_result: dict = {}

    def emit(event):
        event['run_id'] = run_id
        if event.get('type') == 'done':
            run_result.update(event)
        loop.call_soon_threadsafe(queue.put_nowait, event)

    async def _run():
        global is_running, active_run_id
        try:
            await loop.run_in_executor(executor, lambda: run_pipeline(emit=emit, run_id=run_id))
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
            active_run_id = None
            active_queues.pop(run_id, None)

    asyncio.create_task(_run())
    return run_id
