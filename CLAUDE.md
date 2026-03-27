# Ne'Source — Broeren Haile Holdings

## What this project is
Ne'Source — automated SMB lead sourcing pipeline for acquisition targeting.
Searches Google Maps for niche service businesses in Southern California
via the Openmart API (through Orthogonal/x402), geo-filters to the LA
metro, normalizes records, and inserts them into a Postgres database.

## MVP scope — build only this
- One data source: Openmart /api/v1/search
- One table: smb_leads
- One geo target: 40-mile radius from LA center (34.0522, -118.2437)
- No outreach, no enrichment, no email sending in MVP

## Current build status (as of 2026-03-16)
MVP pipeline complete and running. Now building toward public deployment.
- Pipeline + FastAPI backend: complete
- React UI (local): complete — graph, live feed, run history, config panel
- Instantly outreach integration: infrastructure built, needs campaign ID
- Deployment (Vercel + VPS): planned, not yet built
- Auth: planned, not yet built
- APScheduler: planned, not yet built

## File structure
config.py           -- search targets, geo constants, city fallback list
db.py               -- Postgres connection, insert_lead(), count_leads()
scraper.py          -- Openmart API calls via x402
normalize.py        -- geo filter (3-layer), field mapping, validation
pipeline.py         -- main entrypoint, orchestrates search + outreach
instantly.py        -- Instantly API client, push_leads() batch function
DEPLOYMENT_PLAN.md  -- full plan for Vercel + VPS + auth + scheduler
MULTI_API_PLAN.md   -- deferred plan for multi-source API orchestration

api/main.py         -- FastAPI app, all HTTP + WebSocket routes
api/models.py       -- Pydantic request/response models
api/pipeline_runner.py -- asyncio bridge, runs pipeline in thread executor
api/db_queries.py   -- create_run(), update_run(), get_runs(), get_stats()

ui/src/App.tsx                        -- root component, layout, state
ui/src/api.ts                         -- all fetch/WebSocket calls
ui/src/types.ts                       -- shared TypeScript interfaces
ui/src/components/PipelineGraph.tsx   -- ReactFlow 4-node pipeline viz
ui/src/components/LiveFeed.tsx        -- real-time event log
ui/src/components/LeadStats.tsx       -- stats panel
ui/src/components/ConfigPanel.tsx     -- slide-in config editor
ui/src/components/RunHistoryDrawer.tsx -- left drawer, past runs
ui/src/hooks/usePipelineSocket.ts     -- WebSocket hook
ui/src/hooks/useDragResize.ts         -- bottom panel drag-to-resize

test_db.py          -- tests DB connection
test_scraper.py     -- tests a single Openmart call
test_normalize.py   -- tests geo filter with real results

## Infrastructure
- VPS: DigitalOcean droplet at 137.184.11.14 (Ubuntu 24.04, 1GB, SFO3)
- SSH: ssh root@137.184.11.14
- Postgres: Docker container named bh-postgres on the VPS
- DB name: broeren_haile | user: bh_admin
- Docker commands:
    docker ps                          -- check containers running
    docker exec -it bh-postgres psql -U bh_admin -d broeren_haile

## Credentials (all from .env via python-dotenv)
PRIVATE_KEY   -- Orthogonal/x402 wallet private key (64-char hex, not the orth_live_ API key)
DB_HOST       -- 137.184.11.14 (VPS IP)
DB_PORT       -- 5432
DB_NAME       -- broeren_haile
DB_USER       -- bh_admin
DB_PASSWORD   -- stored in .env

## Local environment
- Python: /opt/homebrew/bin/python3.11 (system python3 is 3.9 -- too old for x402)
- Venv: ./venv (created with python3.11)
- Activate: source venv/bin/activate
- Run scripts: venv/bin/python <script>.py

## x402 payment pattern — use this exactly, do not deviate
The x402 package API changed in v2.x. The correct pattern for the installed
version (2.3.0) is:

    import os, requests
    from eth_account import Account
    from x402 import x402ClientSync
    from x402.mechanisms.evm.exact.v1.client import ExactEvmSchemeV1
    from x402.http.clients.requests import x402_http_adapter
    from dotenv import load_dotenv
    load_dotenv()

    account = Account.from_key(os.getenv('PRIVATE_KEY'))
    client = x402ClientSync()
    client.register_v1('base', ExactEvmSchemeV1(signer=account))
    session = requests.Session()
    session.mount('https://', x402_http_adapter(client))

Why this pattern:
- Orthogonal uses x402Version 1 with network="base" (not eip155:8453)
- Must use register_v1() not register()
- Must use ExactEvmSchemeV1 not ExactEvmScheme
- Import is x402.http.clients.requests (not x402.clients.requests -- that path does not exist)
- PRIVATE_KEY must be a 64-char hex Ethereum private key, NOT the orth_live_... API key

## Python packages installed
x402[evm], eth-account, requests, psycopg2-binary, python-dotenv,
fastapi, uvicorn, apscheduler (to be installed for scheduler phase)
Note: must install x402[evm] extra -- plain x402 is missing web3/EVM signer deps

## Scheduling — APScheduler (NOT cron)
Decision: use APScheduler running inside the FastAPI process, not crontab.
Reason: schedule must be configurable from the UI without SSH access.
- Library: apscheduler (BackgroundScheduler)
- Schedule stored in the DB (schedule table), not on the filesystem
- On API startup: load saved schedule from DB and re-register the job
- Endpoints: GET/PUT/DELETE /api/schedule
- UI: schedule panel in header — set frequency, time, timezone
- _get_next_run_at() in main.py already reads crontab — replace with
  APScheduler .get_jobs() query once scheduler is built
- Do NOT add crontab entries for this project going forward

## Instantly integration
- instantly.py: push_leads(leads) sends batch to Instantly campaign
- INSTANTLY_API_KEY: in .env (already set)
- INSTANTLY_CAMPAIGN_ID: in .env (empty — fill in when account is ready)
- Leads without email are skipped (Instantly requires email field)
- Outreach runs automatically at end of every pipeline run
- Pipeline events: outreach_start → outreach_done / outreach_error
- Frontend node: Config → Search → Outreach → Done

## Authentication (planned — not yet built)
- JWT-based, signed with a secret key stored in .env as JWT_SECRET
- users table in Postgres: id, username, password (bcrypt), created_at
- All API routes protected by get_current_user FastAPI dependency
- Token passed as Authorization: Bearer header on HTTP requests
- Token passed as ?token= query param on WebSocket connections
- Frontend: LoginPage.tsx, useAuth.ts hook, AuthGate wrapper in App.tsx
- Seed first admin: python create_user.py --username admin --password <pw>

## Deployment architecture (planned — not yet built)
- Frontend: Vercel (static build from ui/, auto-deploy on git push)
- Backend: DigitalOcean VPS, nginx reverse proxy with SSL (Let's Encrypt)
- nginx routes: /api/* and /ws/* → localhost:8000 (FastAPI)
- Frontend talks to backend via VITE_API_URL env var (not hardcoded IP)
- CORS in main.py must be updated to allow the Vercel domain

## Openmart endpoint
POST https://x402.orth.sh/openmart/api/v1/search
Body: { query, page_size, min_rating, min_reviews }
DO NOT pass a geo parameter -- live testing confirmed it has no effect.
Geo filtering is done in normalize.py using haversine distance.

## Openmart response structure — IMPORTANT
The API returns a list directly (not wrapped in {"data": [...]}).
Each item in the list has this shape:
    {
      "id": "...",
      "content": { <-- all business fields are nested here },
      "match_score": ...,
      "cursor": [...]
    }

normalize.py unwraps content with: raw = record.get('content') or record
All field mapping (business_name, latitude, staffs[], etc.) reads from raw, not record.

## smb_leads table schema

### Existing columns (from Openmart search)
id, company, owner_name, email, phone, address, city, state, zipcode,
website, industry, google_place_id (UNIQUE), rating, review_count,
ownership_type, distance_miles, latitude, longitude, openmart_id,
status (default 'new'), source (default 'openmart'), raw_data (JSONB),
created_at

### Enrichment columns (to be added)
-- People & Contact:
owner_email, owner_phone, owner_linkedin, owner_title,
employee_count (int), key_staff (text[])
-- Company Intel:
year_established (int), services_offered (text[]),
company_description, revenue_estimate, certifications (text[])
-- Online Presence:
review_summary, facebook_url, yelp_url, google_maps_url
-- Metadata:
enrichment_meta (JSONB — per-field source attribution)
-- AI Email:
generated_subject, generated_email

### pipeline_runs addition
cost (numeric(8,4)) — total enrichment + generation cost per run

## Dedup key
google_place_id extracted from Openmart's source_id field.
source_id format: "GOOGLE_MAP@ChIJ..." -- strip the "GOOGLE_MAP@" prefix.
Insert with: ON CONFLICT (google_place_id) DO NOTHING

## Geo filter in normalize.py -- 3 layers in this order
1. State check: reject if state not in ('California', 'CA')
2. Haversine distance: reject if > 40 miles from (34.0522, -118.2437)
3. City name fallback: check against TARGET_CITIES set in config.py
Return None from normalize_lead() if any layer rejects the record.
Confirmed geo rejection rate: ~98% of Openmart results are out-of-area.

## Email validation
Filter out filler domains: godaddy.com, wix.com, squarespace.com,
wordpress.com, example.com -- these appear in Openmart data but are
not real business emails.

## Owner extraction
Check staffs[] array in Openmart response (inside content{}) for role containing:
owner, founder, president, ceo -- return that person's name.

## Key decisions already made
- Do NOT use n8n or Clay -- Python scripts replace both
- Openmart geo param does not work as a hard filter (tested live)
- ownership_type field stored as-is: FAMILY, INDEPENDENT, or None
- distance_miles stored for every inserted record
- raw_data JSONB stores the full Openmart response (the full record including content{})
- psycopg2 parameterized queries only -- never f-string SQL
- Employee count / revenue enrichment deferred to Phase 1 (post-MVP)
  -- Openmart lookup_people and ScrapeGraphAI smartscraper are candidates
  -- Decision: run MVP for 2-3 weeks first, validate data quality, then enrich
- Scheduler: APScheduler (not cron) so schedule is UI-configurable without SSH
- Multi-source API orchestration (Orthogonal waterfall) deferred to post-deployment
  -- See MULTI_API_PLAN.md for full design
- Campaign management (create Instantly campaigns from UI) planned for Phase 4
  -- See DEPLOYMENT_PLAN.md

## Pipeline performance (first full run, 2026-03-16)
- 8 industries x 8 cities = 64 API calls
- 3,200 raw records returned (50 per call)
- 39 new leads inserted
- 3,091 skipped (geo filter)
- 70 skipped (duplicates across queries)
- ~98% geo rejection rate confirmed
