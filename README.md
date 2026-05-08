# Ne'Source

Automated SMB lead-sourcing pipeline for acquisition targeting. Built for **Broeren Haile Holdings**.

Searches for niche service businesses in Southern California, classifies them by acquisition fit, enriches each lead with multi-source contact and company data, and exports the results to Notion for review and outreach.

---

## What it does

1. **Search** — query Openmart for SMBs across configured industries and cities.
2. **Filter** — 3-layer geo filter (state → city → distance) drops leads outside Southern California.
3. **Tier** — Claude classifies each lead as `tier_1`, `tier_2`, `tier_3`, or `hard_remove`. Hard-removes are deleted before any paid API calls.
4. **Enrich** — multi-source waterfall fills owner/senior decision-maker identity, owner contact info, and company evidence.
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
│   Search ──► Geo Filter ──► Tier ──► Enrich (multi-source)    │
│                                          │                    │
│                                          ▼                    │
│                                       Re-tier ──► Notion      │
│                                                               │
│  Concurrency: ENRICH_CONCURRENCY=3, ENRICH_PHASE2_CONCURRENCY=3│
│  Payment:     Orthogonal credits first, x402 fallback          │
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
| Payment     | Orthogonal Run API first, x402 protocol fallback              |
| Hosting     | VPS for backend + DB; Vercel planned for frontend          |

---

## API integrations

Every external service is invoked from `pipeline/`. Paid data providers now route through the Orthogonal Run API first, with x402 retained as fallback during the migration.

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

### 4. Hunter.io — Email lookup
- **What it does:** find professional emails and owner/senior contacts by domain.
- **How it works:** routed through Orthogonal Run API first, x402 fallback second. Requires a verified `website`/domain to be useful.
- **When it runs:** owner-contact stage, parallel with Apollo and Openmart company enrichment.

### 5. Apollo — People + company match
- **What it does:** match a person/company to contact info and organization fields; can also search for senior decision makers.
- **How it works:** routed through Orthogonal Run API first, x402 fallback second. Sends name/domain/email/company LinkedIn when available.
- **When it runs:** owner-contact stage, parallel with Hunter and Openmart company enrichment.

### 6. Website scrape (`pipeline/firecrawl_client.py`)
- **What it does:** scrapes homepage/contact/about/team/leadership pages for owner names, company email/phone, services, description, certifications, and other company evidence.
- **How it works:** direct HTTP fetch with Zyte fallback when configured. Page text is parsed with Claude for structured extraction.
- **When it runs:** after Google Places/domain recovery, before paid owner-contact providers.

### 7. FullEnrich — Final paid fallback (`pipeline/fullenrich.py`)
- **What it does:** identity + contact enrichment as a last-resort paid provider.
- **How it works:** direct REST API with `FULLENRICH_API_KEY`. Gated — runs only when an owner field is still empty after all earlier steps.
- **When it runs:** late in the waterfall (currently part of Phase 2; there is a planned restructure to make it Stage 6 / final-fallback only).

### 8. Notion — Export sink (`pipeline/notion.py`)
- **What it does:** push the final enriched lead set into a Notion database for the operator.
- **How it works:** direct Notion API with a workspace token. Each lead becomes a row with the full enrichment payload.
- **When it runs:** terminal step of `run_pipeline` and `run_csv_pipeline`.

### 9. Orthogonal / x402 — Payment fabric
- **What it does:** funds paid SMB-data APIs such as Openmart, Hunter, Apollo, and Sixtyfour.
- **How it works:** `pipeline/orthogonal.py` calls `https://api.orthogonal.com/v1/run` when `ORTHOGONAL_API_KEY` is configured. x402 routes remain as fallback for compatible providers.
- **Safety nets:**
  - **Pre-flight balance check** before enrichment; auto-pauses the run if the wallet can't cover the estimated cost.
  - **Mid-run 402 handling** — after 3 consecutive 402s on a step, that step silently skips the rest of the run instead of throwing.
  - **UI modal** ("Continue Anyway") surfaces insufficient-funds events to the operator.

---

## Enrichment waterfall

Each step only fills empty fields. Source attribution is recorded per-field in `enrichment_meta` (JSONB).

Current shape:

1. **Google Places + Maps URL** — bootstrap website, company phone, rating, review count, and maps URL.
2. **Domain recovery** — recover verified websites when source data lacks a domain.
3. **Website scrape** — extract company evidence and explicit owner/senior decision-maker names from linked pages.
4. **Openmart / Apollo / Hunter** — parallel owner/contact and company enrichment.
5. **Sixtyfour** — owner phone and senior decision-maker fallback.
6. **FullEnrich** — final paid fallback when configured.
7. **Company fallback** — copies company email to `owner_email` only when no owner email exists; `owner_phone` is never filled from company phone.
8. **Claude failsafe** — infer remaining non-contact fields only.

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
| Multi-source enrichment    | ✅ Complete, owner-contact tuning active |
| Notion export              | ✅ Complete                              |
| Owner-contact maximization | 🚧 In progress (`docs/superpowers/`)    |
| Auth                       | 📋 Planned                               |
| Vercel + nginx deploy      | 📋 Planned                               |
| Scheduler                  | 📋 Planned                               |
