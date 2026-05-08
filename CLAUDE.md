# Ne'Source — Agent Notes

Last updated: 2026-05-08T13:57:56Z

## Project Goal

Ne'Source is an SMB sourcing pipeline for acquisition targeting. The prototype goal is:

1. Search for companies by industry/region/business criteria, or upload a CSV.
2. Tier the companies for fit.
3. Enrich each company with a real owner or senior decision-maker name, email, and phone when available.
4. Preserve source attribution and company-vs-owner contact distinction.
5. Export call-ready records to Notion.

The current active work is owner-contact coverage and sourcing usability. Start with:

- `docs/ACTIVE.md`
- `docs/superpowers/plans/2026-04-26-owner-contact-maximization.md`
- `docs/superpowers/plans/2026-04-26-owner-contact-maximization.md.tasks.json`
- `docs/superpowers/plans/benchmarks/owner-contact-benchmark-summary.md`

Archived/superseded docs live under `docs/archive/2026-05-08-doc-cleanup/`.

## Current Build Status

- FastAPI backend and React local UI are built.
- CSV upload works: deterministic/Claude-assisted column mapping, batch insert, WebSocket run.
- Search flow works through provider abstraction in `pipeline/scraper.py`.
- CSV and search runs both route through `enrich_lead()`.
- Tiering runs before enrichment; hard removes are deleted before paid enrichment.
- Enrichment uses Orthogonal Run API first, x402 fallback second.
- Notion export is the terminal workflow; legacy generated email / Instantly docs are not current.

## Current Enrichment Waterfall

```text
source
  -> normalize + insert
  -> tier
  -> google_places + google_maps
  -> domain_recovery
  -> website scrape with linked leadership-page discovery
  -> openmart_company | apollo | hunter
  -> sixtyfour phone / senior decision-maker fallback
  -> fullenrich if configured
  -> company fallback for owner_email only
  -> claude failsafe for non-contact fields only
  -> re-tier
  -> Notion export
```

Guardrails:

- Do not let Claude invent owner contact data.
- Do not promote `company_phone` into `owner_phone`.
- Treat `company_fallback` owner_email as non-truthful for owner-contact benchmarks.
- Do not replace a website-grounded owner/senior-executive name with an unrelated provider directory match.
- Use `scripts/owner_contact_benchmark.py --mode memory` for repeated benchmark runs unless DB behavior is explicitly under test.

## Important Files

```text
pipeline/
  run.py                main orchestrator for search and CSV runs
  enrichment.py         owner/company enrichment waterfall
  orthogonal.py         shared Orthogonal Run API client
  google_search.py      verified domain recovery
  firecrawl_client.py   direct/Zyte website scrape helpers
  csv_import.py         CSV mapping/parsing/batch insert
  notion.py             Notion export
  db.py                 Postgres helpers
  scraper.py            search provider abstraction
  tiering.py            tier classification

api/
  main.py               FastAPI routes and WebSocket endpoints
  pipeline_runner.py    async bridge, pause/resume
  db_queries.py         run/lead queries

ui/src/
  App.tsx
  api.ts
  components/LeadsTable.tsx
  components/LiveFeed.tsx
  components/PipelineGraph.tsx

scripts/
  owner_contact_benchmark.py
```

## Latest Owner-Contact Benchmarks

Original short 3-row:

- owner_name: 3/3 truthful
- owner_email: 3/3 truthful
- owner_phone: 2/3 truthful
- miss: KWI owner phone

Wholesale short 5-row:

- owner_name: 4/5 truthful
- owner_email: 3/5 truthful
- owner_phone: 4/5 truthful
- misses: Raquel Confections domain recovery; Coast to Coast owner email

Benchmark details are in:

`docs/superpowers/plans/benchmarks/owner-contact-benchmark-summary.md`

## Local Commands

Backend:

```bash
venv/bin/uvicorn api.main:app --port 8000
```

Frontend:

```bash
cd ui && npm run dev
```

Focused tests:

```bash
venv/bin/python -m pytest tests/test_csv_import_guards.py tests/test_enrichment_stages.py tests/test_hunter_extraction.py tests/test_apollo_extraction.py tests/test_orthogonal.py -q
```

No-DB benchmark:

```bash
set -a; source .env; set +a
venv/bin/python scripts/owner_contact_benchmark.py \
  --csv "/Users/neoudhaile/Desktop/simple tier/Wholesale_Distribution_short.csv" \
  --limit 5 \
  --label owner-contact-wholesale-short-5-baseline-memory \
  --mode memory
```

Full `pytest` may need DB/network access; sandboxed runs often fail on remote Postgres tests.
