# Ne'Source — Broeren Haile Holdings

## What this project is
Ne'Source — automated SMB lead sourcing pipeline for acquisition targeting.
Searches for niche service businesses in Southern California, tiers them
for acquisition fit, enriches each lead with contact and company data from
multiple APIs, generates personalized outreach emails with AI, and pushes
them to Instantly for cold email delivery.

## Current build status
- Pipeline + FastAPI backend: complete
- React UI (local): complete — graph, live feed, leads table, run history, config panel
- CSV import: complete — upload CSV, Claude maps columns, batch insert, enrichment via WebSocket
- Tiering: complete — Claude-based tier classification, hard removes before enrichment
- Enrichment waterfall: 9 steps (Claude discovery → Google Places → Maps URL → Hunter → Apollo → Firecrawl website → Firecrawl reviews → Company fallback → Claude failsafe)
- Email generation: complete — fixed template + Claude observation per lead
- Instantly outreach integration: infrastructure built, needs campaign ID
- Deployment (Vercel + VPS): planned, not yet built
- Auth: planned, not yet built
- Scheduler: planned, not yet built

## File structure

```
pipeline/
  config.py             — search targets, geo constants, timeouts, concurrency settings
  scraper.py            — Openmart API calls via x402, cursor pagination
  normalize.py          — 3-layer geo filter, field mapping, dedup key extraction
  db.py                 — Postgres: insert_lead, get_lead, update_lead, batch inserts,
                          tiering helpers, CSV helpers, run cost tracking
  enrichment.py         — 9-step waterfall enrichment with x402 balance tracking,
                          Claude retry with retry-after, error logging to logs/
  email_generator.py    — fixed template + Claude-generated specific observation
  instantly.py          — Instantly API client, per-lead custom variables
  run.py                — main orchestrator: Search → Tier → Enrich → Generate → Outreach
                          also: run_csv_pipeline() for CSV-uploaded leads
                          pre-flight balance check, auto-pause on insufficient funds
  csv_import.py         — CSV parsing, Claude column mapping, batched DB insert
  tiering.py            — Claude-based tier classification (tier_1/2/3 + hard_remove)
  vertical_theses.py    — industry-specific thesis statements for email generation
  firecrawl_client.py   — Firecrawl API client for website/page scraping
  google_places.py      — Google Maps Platform API for place matching + details

api/
  main.py               — FastAPI app, all HTTP + WebSocket routes
  models.py             — Pydantic request/response models
  pipeline_runner.py    — asyncio bridge, runs pipeline in thread executor, pause/resume
  db_queries.py         — create_run(), update_run(), get_runs(), get_leads_by_run_id()

ui/src/
  App.tsx               — root component, layout, state, CSV upload, insufficient funds modal
  api.ts                — all fetch/WebSocket calls, getRunLeads()
  types.ts              — shared TypeScript interfaces, DBLead, ENRICHABLE_FIELDS
  components/
    PipelineGraph.tsx   — ReactFlow 6-node pipeline viz (Config→Search→Enrich→Generate→Outreach→Done)
    LiveFeed.tsx        — real-time event log with CSV/tiering/enrichment events
    LeadsTable.tsx      — event-driven collapsible lead cards with field-level enrichment tracking,
                          source badges, incremental event processing via _seq counter
    LeadStats.tsx       — stats panel
    ConfigPanel.tsx     — slide-in config editor
    RunHistoryDrawer.tsx — left drawer, past runs with cost
    LeadViewer.tsx      — full lead detail viewer with source badges
  hooks/
    usePipelineSocket.ts — WebSocket hook with monotonic _seq counter, 2000 event cap
    useDragResize.ts    — bottom panel drag-to-resize

logs/
  enrichment_errors.log — auto-generated, full stack traces for all enrichment step errors

tests/
  test_db.py            — tests DB connection
  test_scraper.py       — tests a single Openmart call
  test_normalize.py     — tests geo filter with real results
  test_enrichment_balance.py — x402 balance check, 402 flagging, retry logic
  test_leads_table.py   — LeadsTable event processing
```

## Infrastructure
- VPS: DigitalOcean droplet at 137.184.11.14 (Ubuntu 24.04, 1GB, SFO3)
- SSH: ssh root@137.184.11.14
- Postgres: Docker container named bh-postgres on the VPS
- DB name: broeren_haile | user: bh_admin | PostgreSQL 15.17
- Docker commands:
    docker ps
    docker exec -it bh-postgres psql -U bh_admin -d broeren_haile

## Credentials (all from .env via python-dotenv)
PRIVATE_KEY        — Orthogonal/x402 wallet private key (64-char hex)
ANTHROPIC_API_KEY  — sk-ant-... for Claude
GOOGLE_MAPS_API_KEY — Google Maps Platform API key
FIRECRAWL_API_KEY  — Firecrawl API key
INSTANTLY_API_KEY  — Instantly Bearer token
INSTANTLY_CAMPAIGN_ID — Campaign to push leads to (empty = skip outreach)
DB_HOST            — 137.184.11.14
DB_PORT            — 5432
DB_NAME            — broeren_haile
DB_USER            — bh_admin
DB_PASSWORD        — stored in .env

## x402 wallet
Address: 0x254f9eeba6EC17B26Ded62E44D81BD9F160eFBC1
Network: Base (USDC)
Funds all Orthogonal-proxied APIs (Openmart, Hunter, Apollo)

## Local environment
- Python: /opt/homebrew/bin/python3.11
- Venv: ./venv (created with python3.11)
- Run scripts: venv/bin/python <script>.py

## x402 payment pattern — use this exactly
```python
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
```

## Enrichment waterfall (9 steps, 3 phases)
For each lead, these run in phases. Each step only fills empty fields.

Phase 1 (sequential):
1. Claude discovery — infers website/owner_name from company + location
2. Google Places — bootstraps website, phone, rating, review_count, google_maps_url
3. Google Maps URL — constructs URL from place_id (free)

Phase 2 (parallel, ENRICH_PHASE2_CONCURRENCY=3):
4. Hunter.io — email lookup by domain ($0.01 via x402)
5. Apollo — people match for owner_name, contact details, employee_count ($0.01 via x402)
6. Firecrawl website — scrapes company website for services, description, etc.

Phase 3 (sequential):
7. Firecrawl reviews — scrapes review pages for sentiment summary
8. Company fallback — copies company contact into owner fields if owner-level data missing (free)
9. Claude failsafe — infers remaining empty fields from all known data

Source attribution stored in enrichment_meta JSONB per field.

## x402 balance & 402 handling
- Pre-flight balance check before enrichment: if balance < estimated cost, auto-pause run
- UI shows "Continue Anyway" modal (App.tsx fundsAlert state)
- Mid-run 402s: consecutive counter per step (Hunter/Apollo), flags as insufficient after 3 consecutive 402s
- Flag resets at start of each run via reset_x402_flag()
- When flagged, Hunter/Apollo silently skip (return 0.0) instead of throwing errors

## Claude rate limits
- Anthropic org limit: 50 req/min on claude-haiku-4-5-20251001
- _claude_call_with_retry: 3 retries, respects retry-after header, exponential backoff (5s, 10s, 20s)
- ENRICH_CONCURRENCY=3 to stay under limit (was 10, caused heavy 429s)

## Tiering
Before enrichment, leads are classified by Claude into:
- tier_1: strong acquisition fit
- tier_2: moderate fit
- tier_3: weak fit
- hard_remove: clearly not a match (deleted before enrichment)

Tiering uses vertical_theses.py for industry-specific criteria.

## Email generation
Fixed template written by Rigel + Claude-generated specific_observation.
Subject: "{company} — Potential Partnership Conversation"
Vertical thesis comes from vertical_theses.py (no AI needed when match exists).
Claude only writes one sentence about what stood out about the company.

## CSV upload flow
1. User uploads .csv via UI → POST /api/upload-csv
2. Headers mapped via deterministic aliases + Claude fallback for unknowns
3. Rows inserted in batches (execute_values) with synthetic dedup keys
4. Returns run_id immediately → UI connects via WebSocket
5. Background: tier → enrich → generate → outreach (same as search pipeline)

## Database schema

### smb_leads (41 columns)
id, company, owner_name, email, phone, address, city, state, zipcode,
website, industry, google_place_id (UNIQUE), rating, review_count,
ownership_type, distance_miles, latitude, longitude, openmart_id,
run_id, status, source, raw_data (JSONB), created_at,
owner_email, owner_phone, owner_linkedin,
employee_count (int), key_staff (text[]), year_established (int),
services_offered (text[]), company_description, revenue_estimate,
certifications (text[]), review_summary, facebook_url, yelp_url,
google_maps_url, enrichment_meta (JSONB),
generated_subject, generated_email,
tier, tier_reason

### pipeline_runs
id, started_at, finished_at, status, inserted, skipped_geo, skipped_dupe,
total_leads, error_message, triggered_by, cost (NUMERIC 8,4)

## Backend API endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | /api/runs | Trigger a new pipeline run |
| GET | /api/runs | List past runs (with cost) |
| GET | /api/runs/{id} | Get one run |
| GET | /api/stats | Lead counts by industry/ownership |
| GET | /api/status | Running state + active run ID |
| GET | /api/config | Read pipeline config values |
| PUT | /api/config | Update config (writes to config.py) |
| GET | /api/leads | List leads with pagination |
| GET | /api/leads/{id} | Full lead detail with enrichment |
| GET | /api/runs/{id}/leads | All leads for a specific run |
| POST | /api/upload-csv | Upload CSV for import + enrichment |
| POST | /api/pause | Pause active run |
| POST | /api/resume | Resume paused run |
| WS | /ws/runs/{id} | Live event stream for a run |

## Running locally

Backend: `venv/bin/uvicorn api.main:app --port 8000`
Frontend: `cd ui && npm run dev` → http://localhost:5173
Vite proxies /api/* and /ws/* to http://127.0.0.1:8000

Kill ports: `lsof -ti:8000 | xargs kill -9; lsof -ti:5173 | xargs kill -9`

## Key decisions
- Do NOT use n8n or Clay — Python scripts replace both
- Openmart geo param does not work as a hard filter (tested live)
- owner_title removed from schema (was never reliably populated)
- Sixtyfour disabled (too slow ~130s/call, returns empty, $0.40/lead)
- ScrapeGraph replaced by Firecrawl + Claude extraction
- Tier before enrichment — hard removes deleted before paid API calls
- Concurrent enrichment: ENRICH_CONCURRENCY=3, GENERATE_CONCURRENCY=4, ENRICH_PHASE2_CONCURRENCY=3
- psycopg2 parameterized queries only — never f-string SQL

## Planned (not yet built)
- Authentication: JWT-based login
- Deployment: Vercel (frontend) + VPS nginx reverse proxy (backend)
- Scheduler: UI-configurable recurring runs
- Campaign management: Create/manage Instantly campaigns from UI
