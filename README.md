# Ne'Source

Automated SMB lead-sourcing pipeline for acquisition targeting. Built for **Broeren Haile Holdings**.

Searches for niche service businesses in Southern California, classifies them by acquisition fit, enriches each lead with multi-source contact and company data, and exports the results to Notion for review and outreach.

---

## What it does

1. **Search** — query Openmart for SMBs across configured industries and cities.
2. **Filter** — 3-layer geo filter (state → city → distance) drops leads outside Southern California.
3. **Tier** — Claude classifies each lead as `tier_1`, `tier_2`, `tier_3`, or `hard_remove`. Hard-removes are deleted before any paid API calls.
4. **Enrich** — 9-step waterfall fills owner identity, contact info, and company evidence from 6+ providers.
5. **Re-tier** — Claude re-classifies each lead with the enriched data.
6. **Export** — push the final lead set to a Notion database for the operator to review.

CSV uploads follow the same path from step 3 onward.

---

## High-level architecture

```
┌──────────────┐
│   React UI   │  Vite dev server, ReactFlow graph, live feed,
│   (ui/)      │  leads table, run history, CSV upload
└──────┬───────┘
       │ HTTP + WebSocket (proxied via Vite)
┌──────┴───────────────────────────────────────────────────────┐
│  FastAPI backend (api/)                                       │
│  - /api/runs, /api/leads, /api/upload-csv, /api/config, ...   │
│  - /ws/runs/{id} live event stream                            │
└──────┬───────────────────────────────────────────────────────┘
       │ asyncio thread executor
┌──────┴───────────────────────────────────────────────────────┐
│  Pipeline (pipeline/)                                         │
│                                                               │
│   Search ──► Geo Filter ──► Tier ──► Enrich (9 steps)         │
│                                          │                    │
│                                          ▼                    │
│                                       Re-tier ──► Notion      │
│                                                               │
│  Concurrency: ENRICH_CONCURRENCY=3, ENRICH_PHASE2_CONCURRENCY=3│
│  Payment:     x402 (USDC on Base) for Hunter / Apollo / etc.  │
└──────┬───────────────────────────────────────────────────────┘
       │ psycopg2
┌──────┴───────────────────────────────────────────────────────┐
│  Postgres on DigitalOcean droplet (Docker: bh-postgres)       │
│  - smb_leads (41 cols, JSONB enrichment_meta + raw_data)      │
│  - pipeline_runs (status, cost, counts)                       │
└───────────────────────────────────────────────────────────────┘
```

---

## Tech stack

| Layer       | Tools                                                      |
|-------------|------------------------------------------------------------|
| Backend     | Python 3.11, FastAPI, psycopg2, asyncio                    |
| Frontend    | React 19, TypeScript, Vite, ReactFlow                      |
| Database    | PostgreSQL 15 (Dockerized on a DigitalOcean droplet)       |
| AI          | Anthropic Claude (Haiku 4.5)                               |
| Payment     | x402 protocol (USDC on Base) via Orthogonal proxy          |
| Hosting     | VPS for backend + DB; Vercel planned for frontend          |

---

## API integrations

Every external service is invoked from `pipeline/`. APIs marked **x402** are paid per call in USDC via the Orthogonal proxy (`https://x402.orth.sh`) — the wallet at `0x254f9eeba6EC17B26Ded62E44D81BD9F160eFBC1` funds them.

### 1. Openmart — Lead search (`pipeline/scraper.py`)
- **What it does:** returns SMBs matching an industry + city query.
- **How it works:** cursor-paginated REST API calls via x402. Each call is paid per result page. Geo-filtering is done locally afterward (Openmart's geo param is unreliable).
- **When it runs:** Stage 1 of `run_pipeline()` — once per (industry × city) combination.

### 2. Anthropic Claude — Reasoning core (`pipeline/tiering.py`, `csv_import.py`, `enrichment.py`)
- **What it does:** column mapping for arbitrary CSV uploads, tier classification, enrichment discovery + failsafe inference, and per-lead observation strings.
- **How it works:** uses `claude-haiku-4-5-20251001` directly via the Anthropic API. All calls go through `_claude_call_with_retry` with 3 retries, retry-after handling, and 5/10/20s exponential backoff. Concurrency capped at 3 to stay under the org-wide 50 req/min limit.
- **When it runs:**
  - **Tier**: before enrichment (drops `hard_remove`s) and again after.
  - **Discovery (step 1 of waterfall)**: infers missing `website` / `owner_name` from `company` + location.
  - **Failsafe (step 9)**: fills any remaining empty fields from all known evidence.
  - **CSV import**: maps unknown column headers to `smb_leads` columns.

### 3. Google Maps Platform — Place data (`pipeline/google_places.py`)
- **What it does:** match a company name + address to a Google Place; pull `website`, `phone`, `rating`, `review_count`, `place_id`.
- **How it works:** direct REST calls to Google Places API v1 with `GOOGLE_MAPS_API_KEY`. Place ID is then used to construct a free `google_maps_url`.
- **When it runs:** step 2 of the enrichment waterfall.

### 4. Hunter.io — Email lookup *(x402)*
- **What it does:** find professional emails by domain.
- **How it works:** routed through Orthogonal x402 proxy, $0.01 per call. Requires a verified `website` to be useful — short-circuits when `website` is empty.
- **When it runs:** step 4 of the waterfall (Phase 2, parallel with Apollo + Firecrawl).

### 5. Apollo — People + company match *(x402)*
- **What it does:** match an `owner_name` (or LinkedIn URL) to contact info, phone, employee count, and key staff.
- **How it works:** routed through Orthogonal x402 proxy, $0.01 per call. Sends `name`, `domain`, `email`, plus `organization_linkedin_url` when available.
- **When it runs:** step 5 of the waterfall.

### 6. Firecrawl — Website + reviews scrape (`pipeline/firecrawl_client.py`)
- **What it does:** scrapes the company's homepage and review pages for services, description, certifications, and review sentiment.
- **How it works:** direct API with `FIRECRAWL_API_KEY`. Falls back to a direct HTTP fetch + Zyte proxy when configured. Markdown output is then parsed with Claude for structured extraction.
- **When it runs:** step 6 (website) and step 7 (reviews).

### 7. FullEnrich — Final paid fallback (`pipeline/fullenrich.py`)
- **What it does:** identity + contact enrichment as a last-resort paid provider.
- **How it works:** direct REST API with `FULLENRICH_API_KEY`. Gated — runs only when an owner field is still empty after all earlier steps.
- **When it runs:** late in the waterfall (currently part of Phase 2; there is a planned restructure to make it Stage 6 / final-fallback only).

### 8. Notion — Export sink (`pipeline/notion.py`)
- **What it does:** push the final enriched lead set into a Notion database for the operator.
- **How it works:** direct Notion API with a workspace token. Each lead becomes a row with the full enrichment payload.
- **When it runs:** terminal step of `run_pipeline` and `run_csv_pipeline`.

### 9. x402 / Orthogonal — Payment fabric
- **What it does:** funds all paid SMB-data APIs (Openmart, Hunter, Apollo) with per-call USDC micropayments on Base.
- **How it works:** every paid request hits `https://x402.orth.sh/<skill>`. The first response is HTTP 402 with payment terms; the client signs a payment intent with the wallet's private key and retries. The Python `x402` SDK's `x402_http_adapter` is mounted on a `requests.Session` to make this transparent.
- **Safety nets:**
  - **Pre-flight balance check** before enrichment; auto-pauses the run if the wallet can't cover the estimated cost.
  - **Mid-run 402 handling** — after 3 consecutive 402s on a step, that step silently skips the rest of the run instead of throwing.
  - **UI modal** ("Continue Anyway") surfaces insufficient-funds events to the operator.

---

## Enrichment waterfall (9 steps, 3 phases)

Each step only fills empty fields. Source attribution is recorded per-field in `enrichment_meta` (JSONB).

**Phase 1 — sequential (cheap context):**
1. **Claude discovery** — infer `website` / `owner_name` from name + location.
2. **Google Places** — bootstrap `website`, `phone`, `rating`, `review_count`, `google_maps_url`.
3. **Google Maps URL** — construct from `place_id` (free).

**Phase 2 — parallel (paid lookups, `ENRICH_PHASE2_CONCURRENCY=3`):**
4. **Hunter** — domain → email.
5. **Apollo** — name + domain → contact + employee count.
6. **Firecrawl website** — scrape homepage for services / description.

**Phase 3 — sequential (cleanup):**
7. **Firecrawl reviews** — scrape review pages for sentiment summary.
8. **Company fallback** — copy company contact into owner fields where appropriate (currently `owner_email` only — `owner_phone` is **not** filled here, to avoid silently promoting business phones to owner phones).
9. **Claude failsafe** — infer remaining empty fields from all known evidence.

---

## Database

**`smb_leads`** (41 columns) — full lead record. Notable JSONB columns:
- `enrichment_meta` — per-field source attribution and confidence.
- `raw_data` — original Openmart / CSV payload, preserved for audit.

**`pipeline_runs`** — one row per run with status, lead counts, error message, and aggregate `cost` (NUMERIC).

Schema lives on a Dockerized Postgres 15 instance on the VPS. See `migrations/` for changes.

---

## Local development

```bash
# Backend
venv/bin/uvicorn api.main:app --port 8000

# Frontend
cd ui && npm run dev    # http://localhost:5173

# Kill stuck ports
lsof -ti:8000 | xargs kill -9; lsof -ti:5173 | xargs kill -9
```

Vite proxies `/api/*` and `/ws/*` to `http://127.0.0.1:8000`.

Tests:
```bash
venv/bin/python -m pytest tests/ -v
```

---

## Repo layout

```
pipeline/    # Python: scraper, normalize, enrichment, tiering, providers, runners
api/         # FastAPI app: routes, models, async bridge to pipeline
ui/          # React + Vite frontend
tests/       # pytest suites for enrichment, csv import, normalization, etc.
migrations/  # SQL schema changes
docs/        # architecture notes, run logs, investigations
logs/        # auto-generated enrichment error logs (gitignored)
```

For deeper internals, infrastructure details, and the full file-by-file map, see [`CLAUDE.md`](./CLAUDE.md).

---

## Status

| Component                  | Status                                  |
|----------------------------|-----------------------------------------|
| Pipeline + FastAPI backend | ✅ Complete                              |
| React UI (local)           | ✅ Complete                              |
| CSV import                 | ✅ Complete                              |
| Tiering (pre + post)       | ✅ Complete                              |
| 9-step enrichment          | ✅ Complete                              |
| Notion export              | ✅ Complete                              |
| Owner-contact maximization | 🚧 In progress (`docs/superpowers/`)    |
| Auth                       | 📋 Planned                               |
| Vercel + nginx deploy      | 📋 Planned                               |
| Scheduler                  | 📋 Planned                               |
