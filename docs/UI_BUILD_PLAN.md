# Ne'Source UI — Detailed Build Plan

## Overview
A local web UI that visualizes the BH acquisition pipeline, streams live run
progress, tracks run history, and displays lead stats. The backend reads/writes
from the VPS Postgres. No auth. No cloud hosting.

---

## 1. Database Changes

### New table: pipeline_runs
Run once on VPS Postgres.

```sql
CREATE TABLE pipeline_runs (
    id              SERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed')),
    inserted        INTEGER NOT NULL DEFAULT 0,
    skipped_geo     INTEGER NOT NULL DEFAULT 0,
    skipped_dupe    INTEGER NOT NULL DEFAULT 0,
    total_leads     INTEGER,        -- snapshot of COUNT(*) from smb_leads at run end
    error_message   TEXT,           -- only populated if status = 'failed'
    triggered_by    TEXT DEFAULT 'ui'
);

CREATE INDEX idx_pipeline_runs_started_at ON pipeline_runs (started_at DESC);
```

No changes to smb_leads. This table is append-only — one row per pipeline run.

---

## 2. Backend Changes

### 2a. Refactor pipeline.py

**Current behavior:** `main()` function, runs synchronously, prints to stdout.

**New behavior:** rename `main()` to `run_pipeline(emit=None)`. The `emit`
parameter is an optional callable that receives a dict event at each meaningful
pipeline step. If None, defaults to a no-op so CLI usage is unchanged.

```
if __name__ == '__main__':
    run_pipeline(emit=lambda e: print(e))
```

#### Event shapes emitted during a run

| Event type  | When fired                        | Payload fields                                           |
|-------------|-----------------------------------|----------------------------------------------------------|
| `start`     | Before any queries                | industries[], cities[], total_queries (int)              |
| `search`    | Before each API call              | query, city, index (1-based), total                      |
| `results`   | After each API call returns       | query, city, count                                       |
| `geo`       | While geo filtering a batch       | query, city, passed, rejected                            |
| `normalizing` | While normalizing a batch       | query, city                                              |
| `inserting` | While inserting a batch           | query, city                                              |
| `insert`    | Each time a lead is written to DB | company, industry, city, distance_miles, ownership_type  |
| `skip_dupe` | Each time ON CONFLICT fires       | company                                                  |
| `progress`  | After each query batch completes  | inserted_so_far, skipped_geo_so_far, skipped_dupe_so_far |
| `done`      | After all queries complete        | inserted, skipped_geo, skipped_dupe, total_leads         |
| `error`     | On any unhandled exception        | message                                                  |

Note: individual `skip_geo` events are NOT emitted per record. Running totals
are sent in `progress` after each batch.

### 2b. New file structure under api/

```
bh-pipeline/
├── api/
│   ├── __init__.py
│   ├── main.py            -- FastAPI app, CORS, all routes, WebSocket handler
│   ├── pipeline_runner.py -- thread executor + asyncio bridge
│   ├── db_queries.py      -- pipeline_runs CRUD + stats queries
│   └── models.py          -- Pydantic response models
```

### 2c. api/db_queries.py

Four functions, all using psycopg2 (same pattern as db.py):

```python
def create_run(conn) -> int
    # INSERT INTO pipeline_runs DEFAULT VALUES, return id

def update_run(conn, run_id, **fields)
    # UPDATE pipeline_runs SET ... WHERE id = run_id
    # Fields: status, finished_at, inserted, skipped_geo, skipped_dupe,
    #         total_leads, error_message

def get_runs(conn, limit=20) -> list[dict]
    # SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT limit

def get_stats(conn) -> dict
    # Returns:
    # {
    #   "total_leads": 40,
    #   "by_industry": [{"industry": "HVAC repair", "count": 8}, ...],
    #   "by_ownership_type": [{"ownership_type": "FAMILY", "count": 12}, ...]
    # }
```

### 2d. api/pipeline_runner.py

Bridges synchronous pipeline.py with FastAPI's async event loop.

Key design decisions:
- One `ThreadPoolExecutor(max_workers=1)` — enforces one run at a time
- `is_running` flag — API returns 409 if a run is already active
- `emit` uses `loop.call_soon_threadsafe(queue.put_nowait, event)` to safely
  push events from worker thread into asyncio Queue

```python
is_running = False

async def start_run(run_id: int, queue: asyncio.Queue):
    global is_running
    loop = asyncio.get_event_loop()

    def emit(event):
        event["run_id"] = run_id
        loop.call_soon_threadsafe(queue.put_nowait, event)

    is_running = True
    try:
        await loop.run_in_executor(executor, run_pipeline, emit)
    finally:
        is_running = False
```

### 2e. api/main.py — REST endpoints

| Method    | Path               | Description                                              |
|-----------|--------------------|----------------------------------------------------------|
| POST      | /api/runs          | Trigger a new run. Returns {run_id}. 409 if running.    |
| GET       | /api/runs          | List last 20 runs. Query param: ?limit=N.               |
| GET       | /api/runs/{id}     | Single run record.                                       |
| GET       | /api/stats         | Total leads + breakdowns.                                |
| GET       | /api/status        | {"is_running": bool, "active_run_id": int or null}      |
| GET       | /api/config        | Returns current industries[], cities[] from config.py   |
| PUT       | /api/config        | Updates industries[] and/or cities[] in config.py       |
| WebSocket | /ws/runs/{run_id}  | Streams JSON events for a run.                           |

### 2f. WebSocket handler (keepalive + drain)

Pipeline takes 60–120s. Keepalive ping every 15s prevents browser from
dropping idle connections.

```python
active_queues: dict[int, asyncio.Queue] = {}

@app.websocket("/ws/runs/{run_id}")
async def ws_run(websocket: WebSocket, run_id: int):
    await websocket.accept()
    queue = active_queues.get(run_id)
    if queue is None:
        await websocket.close(1008)
        return

    async def keepalive():
        while True:
            await asyncio.sleep(15)
            await websocket.send_json({"type": "ping"})

    keepalive_task = asyncio.create_task(keepalive())
    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
            if event.get("type") in ("done", "error"):
                break
    except WebSocketDisconnect:
        pass
    finally:
        keepalive_task.cancel()
```

### 2g. Python packages to add

```
fastapi
uvicorn[standard]
```

websockets, pydantic v2, and psycopg2-binary are already installed.

---

## 3. Frontend

### 3a. Tech stack

| Tool          | Purpose                                      |
|---------------|----------------------------------------------|
| Vite + React  | Build tooling + UI framework                 |
| TypeScript    | Type safety                                  |
| @xyflow/react | Pipeline node graph (ReactFlow v12)          |
| Tailwind CSS  | Layout and styling                           |
| lucide-react  | Icons                                        |

No charting library — bars are CSS flex divs.
No state management library — useState + useContext is sufficient.

### 3b. File structure

```
ui/
├── index.html
├── package.json
├── vite.config.ts              -- proxies /api and /ws to localhost:8000
├── tailwind.config.ts
├── postcss.config.js
└── src/
    ├── main.tsx                -- mounts App, imports @xyflow/react/dist/style.css
    ├── App.tsx                 -- layout shell, global state
    ├── types.ts                -- shared TypeScript interfaces
    ├── api.ts                  -- typed fetch wrappers + openRunSocket()
    ├── hooks/
    │   └── usePipelineSocket.ts
    └── components/
        ├── PipelineGraph.tsx   -- 3-node ReactFlow graph
        ├── ConfigPanel.tsx     -- slide-in panel from Config node click
        ├── TriggerButton.tsx   -- run button + cadence indicator
        ├── LiveFeed.tsx        -- scrolling event log inside Search node
        ├── RunHistoryDrawer.tsx -- left-side slide-out overlay
        └── LeadStats.tsx       -- totals + breakdowns
```

### 3c. vite.config.ts proxy

```ts
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': 'http://localhost:8000',
      '/ws': { target: 'ws://localhost:8000', ws: true },
    }
  }
})
```

---

## 4. Frontend Layout

```
┌─────────────────────────────────────────────────────────────────┐
│ ≡  BH Pipeline          ● Running — next run Mon Mar 23 6:00am  │
│                                          [Run Pipeline ▶]       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│                                                                  │
│     [    Config    ] ──────► [      Search      ] ──► [ Done ]  │
│     Industries: 8            Searching (12/64)                   │
│     Cities: 8                Geo filtering...                    │
│     Click to edit            Normalizing...                      │
│                              Inserting to database...            │
│                                                                  │
│                                                                  │
│  Progress: ████████████░░░░░░░░  12 / 64 queries                 │
├─────────────────────────────────────────────────────────────────┤
│  LIVE FEED                      │  LEAD STATS                   │
│  ───────────────────────────    │  ─────────────────────────    │
│  Searching: HVAC repair         │  Total Leads: 40              │
│    in Los Angeles (12/64)       │                               │
│  Found 50 results               │  By Industry                  │
│  Geo filtering...               │  HVAC repair   ████████  8    │
│  + Eubanks Air, Inc.            │  plumbing      ██████    6    │
│    21.8 mi · INDEPENDENT        │  pool service  █████     5    │
│  Searching: HVAC repair         │  pest control  ████      4    │
│    in Long Beach (13/64)        │  landscaping   ███       3    │
│                                 │  roofing       ███       3    │
│                                 │  electrical    ███       3    │
│                                 │  auto repair   ███       3    │
│                                 │                               │
│                                 │  By Ownership                 │
│                                 │  FAMILY       ████████  12   │
│                                 │  INDEPENDENT  ██████    9    │
│                                 │  Unknown      ████████  19   │
└─────────────────────────────────────────────────────────────────┘

LEFT SIDEBAR (slides in from ≡ hamburger):
┌──────────────────────────┐
│  Run History         ✕   │
│  ──────────────────────  │
│  #2  Mar 16 03:46        │
│  completed               │
│  In: 39  Geo: 3091       │
│  Dupe: 70  Total: 40     │
│                          │
│  #1  Mar 16 03:45        │
│  completed               │
│  In: 1   Geo: 49         │
│  Dupe: 0   Total: 1      │
└──────────────────────────┘
```

---

## 5. PipelineGraph.tsx — 3 Nodes

Three nodes only. The Search node contains the live status text inside it.

```
Node ID   Label    x     y    Description
──────────────────────────────────────────────────────
config    Config   0     0    Clickable — opens ConfigPanel
search    Search   280   0    Shows live sub-status text inside the node
done      Done     560   0    Lights up when run completes
```

### Node states

| State    | Visual                                  | Triggered by            |
|----------|-----------------------------------------|-------------------------|
| idle     | Gray border, gray label                 | Default / no run active |
| active   | Blue border, blue label, pulsing ring   | Currently processing    |
| complete | Green border, green label, no icon      | Next node becomes active or run done |
| error    | Red border, red label, X icon           | `error` event           |

No checkmark icon on complete state — just color change to green.

### Search node — live sub-status text

While the Search node is active, it shows a secondary line of text inside the
node that updates with each event:

| Event type    | Sub-status text shown inside Search node     |
|---------------|----------------------------------------------|
| `search`      | "Searching: HVAC repair in Los Angeles..."   |
| `geo`         | "Geo filtering..."                           |
| `normalizing` | "Normalizing..."                             |
| `inserting`   | "Inserting to database..."                   |
| `progress`    | "Batch complete (12/64)"                     |

This text resets to blank when the run completes or the node goes idle.

### Config node — click behavior

Clicking the Config node opens a slide-in ConfigPanel from the right side.
The panel shows the current industries and cities, editable via the API.

---

## 6. ConfigPanel.tsx — Editable Config

Slides in from the right when Config node is clicked. Closes on X or
clicking outside.

### What is shown and editable

**Industries** (editable list)
- Displays current list from GET /api/config
- Add new: text input + Add button
- Remove: X button next to each item
- On save: PUT /api/config with updated industries[]
- Changes take effect on next run

**Cities** (editable list)
- Same pattern as industries
- Format must be "City, CA" — validated before save

**Read-only display** (not editable in UI):
- PAGE_SIZE: 50
- MIN_REVIEWS: 5
- MIN_RATING: 3.5
- GEO_RADIUS_MILES: 40 miles

Note: read-only fields are visible so you know exactly what filters are
applied during a run. They are edited in config.py directly for now.

---

## 7. TriggerButton.tsx + Cadence Indicator

### Run Pipeline button
- Blue button, top-right of header
- Disabled + shows spinner while a run is active
- On click: POST /api/runs → receive run_id → open WebSocket

### Cadence indicator (header, center)
Shows the pipeline run schedule and current state:

| State          | Display                                      |
|----------------|----------------------------------------------|
| Idle, cron set | "Next run: Mon Mar 23, 6:00am"               |
| Running        | "● Running" (blue pulsing dot)               |
| No cron set    | "No schedule set"                            |
| Just completed | "Last run: Mar 16, 3:46am — 39 leads added"  |

The next run time is read from GET /api/status which includes
`next_run_at` (ISO timestamp from cron schedule, null if not set).

---

## 8. RunHistoryDrawer.tsx — Left Sidebar Overlay

### How it opens
Hamburger icon (≡) in top-left of header. Click → drawer slides in from left.
Overlay darkens the main content. Click outside or X to close.

### Drawer content

Header: "Run History" + X close button

Each run card shows:
```
Run #2                    Mar 16, 2026 03:46am
● completed               Duration: 1m 52s
────────────────────────────────────────────
Inserted:    39
Geo skipped: 3,091
Dupes:       70
Total leads: 40
```

Status badge colors:
- `running`   → blue pulsing dot
- `completed` → green dot (no icon, just color)
- `failed`    → red dot

Most recent run always at top. Shows last 20 runs.
Refreshes automatically after each `done` event.

---

## 9. LiveFeed.tsx — Event Log Detail

Auto-scrolling list, pinned to bottom. Rendered in the bottom-left panel.

| Event type    | Display text                                           | Color  |
|---------------|--------------------------------------------------------|--------|
| `start`       | "Pipeline started — 8 industries × 8 cities"          | Blue   |
| `search`      | "Searching: HVAC repair in Los Angeles, CA (12/64)"    | Gray   |
| `results`     | "  Found 50 results"                                   | Gray   |
| `geo`         | "  Geo filtering... (X passed, Y rejected)"            | Gray   |
| `normalizing` | "  Normalizing..."                                     | Gray   |
| `inserting`   | "  Inserting to database..."                           | Gray   |
| `insert`      | "+ Eubanks Air, Inc. — 21.8 mi · INDEPENDENT"          | Green  |
| `skip_dupe`   | "~ Eubanks Air, Inc. — duplicate"                      | Yellow |
| `progress`    | "  Batch done: 39 inserted, 3,091 filtered so far"     | Gray   |
| `done`        | "Run complete — Inserted: 39 | Geo: 3,091 | Dupes: 70" | Green  |
| `error`       | "Error: <message>"                                     | Red    |
| `ping`        | (silently ignored — not displayed)                     | —      |

Max 500 events kept in memory.

---

## 10. LeadStats.tsx — Stats Detail

Fetched from GET /api/stats on mount and after each `done` event.

### Total leads
Large number. Shows delta from last run: "+39 this run" in smaller text below.

### By industry (CSS flex bars)
Row format: [label]  [████████░░░░]  [count]
Bar width = (count / max_count) * 100%
Sorted descending by count.

### By ownership type
Same bar format. Three rows: FAMILY, INDEPENDENT, Unknown.
FAMILY row uses a distinct highlight color — highest priority acquisition target.

---

## 11. Build Sequence

### Step 1 — Postgres
- Add pipeline_runs table to VPS Postgres

### Step 2 — Refactor pipeline.py
- Rename main() to run_pipeline(emit=None)
- Add emit() calls at every stage: start, search, results, geo, normalizing,
  inserting, insert, skip_dupe, progress, done, error
- Keep `if __name__ == '__main__'` working for CLI use
- Test: python pipeline.py still works correctly

### Step 3 — Backend (api/)
- Create api/__init__.py
- Create api/db_queries.py
- Create api/models.py
- Create api/pipeline_runner.py
- Create api/main.py
- Add GET /api/config and PUT /api/config endpoints
- Add next_run_at to GET /api/status (reads cron schedule)
- pip install fastapi "uvicorn[standard]"
- Test all endpoints with curl before touching React

### Step 4 — Frontend (ui/)
- npm create vite@latest ui -- --template react-ts
- npm install @xyflow/react tailwindcss lucide-react postcss autoprefixer
- Configure Tailwind + vite proxy
- Build order:
  1. types.ts
  2. api.ts
  3. usePipelineSocket.ts
  4. LiveFeed.tsx
  5. LeadStats.tsx
  6. RunHistoryDrawer.tsx
  7. ConfigPanel.tsx
  8. TriggerButton.tsx
  9. PipelineGraph.tsx (3 nodes: Config, Search, Done)
  10. App.tsx

### Step 5 — Run both
- Terminal 1: uvicorn api.main:app --reload --port 8000
- Terminal 2: cd ui && npm run dev
- Open: http://localhost:5173

---

## 12. What Does NOT Change

- scraper.py — unchanged
- normalize.py — unchanged
- db.py — unchanged
- .env — unchanged
- smb_leads table — unchanged
- VPS setup — unchanged
- Cron job (when deployed) — calls run_pipeline() directly, bypasses API
