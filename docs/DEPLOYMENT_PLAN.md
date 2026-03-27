check# Ne'Source — Deployment Plan

## Goal
Public-facing web app where authorized users can log in, configure and run
the pipeline, manage Instantly campaigns, and watch live status. Backend
stays on the DigitalOcean VPS. Frontend served via Vercel.

---

## Architecture

```
Browser → Vercel (React frontend)
              ↓ HTTPS REST + WSS
         DigitalOcean VPS (137.184.11.14)
              ├── nginx  (SSL termination, reverse proxy)
              ├── FastAPI (port 8000, uvicorn)
              ├── PostgreSQL in Docker (port 5432)
              └── cron  (scheduled pipeline runs)
```

WebSocket connections go directly from the browser to the VPS via WSS.
Vercel only serves static assets — no proxying needed.

---

## Phase 1 — Domain + HTTPS on VPS

**Goal:** VPS API accessible at `https://api.yourdomain.com`

Steps:
1. Buy a domain (Namecheap, Cloudflare, etc.)
2. Add an A record: `api.yourdomain.com` → `137.184.11.14`
3. SSH into VPS and install nginx:
   ```bash
   apt install nginx certbot python3-certbot-nginx -y
   ```
4. Write nginx config for `api.yourdomain.com`:
   - Proxy `/api/*` → `http://localhost:8000`
   - Proxy `/ws/*` → `ws://localhost:8000` (with upgrade headers for WebSocket)
5. Run certbot to get a free SSL cert:
   ```bash
   certbot --nginx -d api.yourdomain.com
   ```
6. Update FastAPI CORS to allow `https://yourapp.vercel.app` and your
   custom domain once known.
7. Update frontend `api.ts` to use env var `VITE_API_URL` instead of
   hardcoded `127.0.0.1:8000`.

**Deliverable:** `https://api.yourdomain.com/api/status` returns JSON.

---

## Phase 2 — Deploy Frontend to Vercel

**Goal:** React app live at `https://yourapp.vercel.app` (or custom domain)

Steps:
1. Push `ui/` to a GitHub repo (or the whole project with `ui/` as root).
2. Connect repo to Vercel. Set:
   - Framework: Vite
   - Root directory: `ui`
   - Build command: `npm run build`
   - Output directory: `dist`
3. Add environment variable in Vercel dashboard:
   ```
   VITE_API_URL=https://api.yourdomain.com
   ```
4. In `ui/src/api.ts` and `hooks/usePipelineSocket.ts`, replace hardcoded
   `127.0.0.1:8000` with `import.meta.env.VITE_API_URL`.
5. Deploy. Every push to `main` auto-deploys.

**Deliverable:** App loads at Vercel URL, talks to VPS API.

---

## Phase 3 — Authentication

**Goal:** Login wall — only users with a username + password can access anything.

### Backend

1. Add `users` table to Postgres:
   ```sql
   CREATE TABLE users (
       id         SERIAL PRIMARY KEY,
       username   TEXT UNIQUE NOT NULL,
       password   TEXT NOT NULL,  -- bcrypt hash
       created_at TIMESTAMPTZ DEFAULT now()
   );
   ```
2. Install deps on VPS:
   ```bash
   pip install python-jose[cryptography] passlib[bcrypt]
   ```
3. Add `api/auth.py`:
   - `POST /auth/login` — verify username + bcrypt hash, return signed JWT
   - JWT contains `user_id`, expires in 24h
   - `GET /auth/me` — validate token, return user info
4. Add `get_current_user` dependency to all existing routes (runs, stats,
   config, websocket). Routes return 401 if token missing or invalid.
5. Add a seed script to create the first admin user:
   ```bash
   python create_user.py --username admin --password <password>
   ```

### Frontend

1. Add `src/pages/LoginPage.tsx` — username + password form, calls
   `POST /auth/login`, stores JWT in `localStorage`.
2. Add `src/hooks/useAuth.ts` — reads token, exposes `user` and `logout`.
3. Wrap `App.tsx` in an `<AuthGate>` component — if no valid token,
   render `<LoginPage>` instead.
4. Attach `Authorization: Bearer <token>` header to all `fetch` calls
   in `api.ts`.
5. Pass token in WebSocket URL as query param:
   `wss://api.yourdomain.com/ws/runs/{id}?token=<jwt>`

**Deliverable:** Unauthenticated users see login screen. Token persists
across sessions. Logout clears token and redirects to login.

---

## Phase 4 — Campaign Management

**Goal:** Create and manage Instantly campaigns from the UI, select which
campaign a run pushes leads into.

### Backend

1. Add `campaigns` table:
   ```sql
   CREATE TABLE campaigns (
       id                 SERIAL PRIMARY KEY,
       name               TEXT NOT NULL,
       instantly_id       TEXT,  -- Instantly campaign_id once created
       status             TEXT DEFAULT 'draft',
       created_at         TIMESTAMPTZ DEFAULT now()
   );
   ```
2. Add endpoints:
   - `GET  /api/campaigns` — list all campaigns
   - `POST /api/campaigns` — create a campaign in Instantly + save record
   - `GET  /api/campaigns/{id}` — get single campaign with lead count
3. Add `POST /api/runs` body param: `campaign_id` — pipeline runner uses
   this campaign instead of the env var default.

### Frontend

1. Add `CampaignPanel.tsx` — list campaigns, create new, select active.
2. Add campaign selector to the Run button flow — clicking Run opens a
   "Select campaign" modal if none is active.
3. Show campaign name in run history drawer next to each run.

**Deliverable:** Users can create campaigns and route pipeline runs into
specific Instantly campaigns.

---

## Phase 5 — Recurring Pipeline (APScheduler)

**Goal:** Set a recurring schedule from the UI without SSH. Pipeline runs
automatically on the configured interval. All logged-in users see the live
run in the node graph as it happens.

**Decision: APScheduler only — no cron.** The schedule must be fully
configurable from the UI. Crontab requires SSH to change.

### Backend

1. Install APScheduler on VPS:
   ```bash
   pip install apscheduler
   ```

2. Add `schedule` table to Postgres to persist the schedule across restarts:
   ```sql
   CREATE TABLE schedule (
       id          SERIAL PRIMARY KEY,
       cron_expr   TEXT NOT NULL,        -- e.g. "0 6 * * *" (6am daily)
       timezone    TEXT NOT NULL DEFAULT 'America/Los_Angeles',
       enabled     BOOLEAN DEFAULT true,
       created_at  TIMESTAMPTZ DEFAULT now(),
       updated_at  TIMESTAMPTZ DEFAULT now()
   );
   ```

3. Add `api/scheduler.py`:
   - Initializes `BackgroundScheduler` with `APScheduler`
   - `start(run_fn)` — loads saved schedule from DB, registers job
   - `set_schedule(cron_expr, timezone)` — saves to DB, replaces job
   - `clear_schedule()` — removes job, marks disabled in DB
   - `get_next_run()` — returns ISO timestamp of next scheduled run
   - On FastAPI startup: call `scheduler.start(runner.start_run)`
   - On FastAPI shutdown: call `scheduler.shutdown()`

4. Add endpoints in `api/main.py`:
   ```
   GET    /api/schedule   — return { cron_expr, timezone, enabled, next_run_at }
   PUT    /api/schedule   — body: { cron_expr, timezone } — set or update schedule
   DELETE /api/schedule   — disable schedule (keeps DB row, sets enabled=false)
   ```

5. The scheduled job calls `runner.start_run()` exactly like a manual run.
   All WebSocket clients see the live events. Run is recorded in the
   `pipeline_runs` table with `triggered_by = 'scheduler'`.

6. Replace `_get_next_run_at()` in `main.py` (which reads crontab) with
   `scheduler.get_next_run()`.

### Frontend

1. Replace the static cadence label in the header with a clickable
   `ScheduleButton` that opens `SchedulePanel.tsx`.

2. `SchedulePanel.tsx` contains:
   - **Frequency picker**: dropdown — Hourly / Daily / Weekly / Custom
   - **Time picker**: hour + minute (for daily/weekly)
   - **Day picker**: day of week (for weekly)
   - **Timezone**: dropdown defaulting to America/Los_Angeles
   - **Custom cron**: text input shown when "Custom" is selected, with
     a live plain-English preview (e.g. "Every day at 6:00 AM PT")
   - **Enable/Disable toggle**: turns schedule on/off without deleting it
   - **Save** button — calls `PUT /api/schedule`
   - **Clear** button — calls `DELETE /api/schedule`
   - Shows "Next run: Monday Mar 23 at 6:00 AM" when active

3. Header cadence label updates to show live countdown when schedule is set:
   "Next run in 4h 23m" — polls `GET /api/schedule` every 60s.

4. When a scheduled run starts, all connected clients receive the WebSocket
   events automatically — no page refresh needed.

### Cron expression presets (UI → cron mapping)
| UI Option       | Cron expression  |
|-----------------|------------------|
| Every hour      | `0 * * * *`      |
| Every 6 hours   | `0 */6 * * *`    |
| Daily at 6am    | `0 6 * * *`      |
| Daily at 9am    | `0 9 * * *`      |
| Weekdays at 8am | `0 8 * * 1-5`    |
| Weekly Monday   | `0 8 * * 1`      |
| Custom          | user-entered     |

**Deliverable:** Admin opens the schedule panel, picks "Daily at 6am PT",
clicks Save. Pipeline runs every morning without SSH. Any user logged in
at run time sees it live. Run history shows `triggered_by: scheduler`.

---

## Execution Order

| Phase | Effort | Blocks |
|-------|--------|--------|
| 1 — Domain + HTTPS | ~1 hr | Everything else |
| 2 — Vercel deploy | ~30 min | Auth (needs public URL for CORS) |
| 3 — Auth | ~3 hrs | Campaign mgmt, scheduler |
| 4 — Campaign mgmt | ~2 hrs | Nothing |
| 5 — Scheduler | ~2 hrs | Nothing |

Start with Phase 1 + 2 to get a live URL, then Phase 3 before sharing
with anyone.

---

## Open Questions

- Domain: do you have one, or do we need to buy one?
- Auth: single shared login, or individual accounts per user?
- Scheduler: Option A (cron, simpler) or Option B (APScheduler, no SSH needed to change schedule)?
- Vercel: is the repo already on GitHub, or does it need to be pushed?
