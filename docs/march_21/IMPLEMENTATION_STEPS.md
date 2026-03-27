# Ne'Source — Schema Update Implementation Steps

Reference: `docs/march_21/REQUIREMENTS_SCHEMA_UPDATE.md`

---

## Step 1: Database Migration

**What:** Add 17 new columns to `smb_leads` and 1 new column to `pipeline_runs`.

**Why:** The enrichment waterfall and AI email generation need somewhere to store
their output. These columns must exist before any enrichment code runs.

**Code changes:**

Run this SQL on the VPS Postgres (`docker exec -it bh-postgres psql -U bh_admin -d broeren_haile`):

```sql
-- People & Contact
ALTER TABLE smb_leads ADD COLUMN owner_email       TEXT;
ALTER TABLE smb_leads ADD COLUMN owner_phone       TEXT;
ALTER TABLE smb_leads ADD COLUMN owner_linkedin    TEXT;
ALTER TABLE smb_leads ADD COLUMN owner_title       TEXT;
ALTER TABLE smb_leads ADD COLUMN employee_count    INT;
ALTER TABLE smb_leads ADD COLUMN key_staff         TEXT[];

-- Company Intelligence
ALTER TABLE smb_leads ADD COLUMN year_established     INT;
ALTER TABLE smb_leads ADD COLUMN services_offered     TEXT[];
ALTER TABLE smb_leads ADD COLUMN company_description  TEXT;
ALTER TABLE smb_leads ADD COLUMN revenue_estimate     TEXT;
ALTER TABLE smb_leads ADD COLUMN certifications       TEXT[];

-- Online Presence & Reputation
ALTER TABLE smb_leads ADD COLUMN review_summary    TEXT;
ALTER TABLE smb_leads ADD COLUMN facebook_url      TEXT;
ALTER TABLE smb_leads ADD COLUMN yelp_url          TEXT;
ALTER TABLE smb_leads ADD COLUMN google_maps_url   TEXT;

-- Enrichment metadata
ALTER TABLE smb_leads ADD COLUMN enrichment_meta   JSONB;

-- AI-generated email
ALTER TABLE smb_leads ADD COLUMN generated_subject TEXT;
ALTER TABLE smb_leads ADD COLUMN generated_email   TEXT;

-- Run cost tracking
ALTER TABLE pipeline_runs ADD COLUMN cost NUMERIC(8,4) DEFAULT 0;
```

**Order:** Must be done FIRST — all subsequent steps depend on these columns existing.

---

## Step 2: Update `pipeline/db.py` — Return Insert IDs + Add `update_lead()`

**What:**
1. Modify `insert_lead()` to return the inserted row's `id` (integer) instead of `True`/`False`. Use `INSERT ... RETURNING id` and return `None` on conflict (duplicate).
2. Add `update_lead(lead_id, fields_dict)` that takes a lead ID and a dict of column→value pairs and runs a parameterized `UPDATE smb_leads SET ... WHERE id = %s`.
3. Add `get_lead(lead_id)` that returns a full lead row as a dict (needed for email generation prompt).
4. Add `update_run_cost(run_id, cost)` that sets `pipeline_runs.cost = cost`.

**Why:**
- `insert_lead()` currently returns a boolean. The pipeline needs the actual row ID to track which leads to enrich, generate emails for, and push to Instantly.
- `update_lead()` is needed because enrichment and email generation write data back to existing rows (not new inserts).
- `get_lead()` is needed to re-read the fully enriched lead before generating the email prompt.
- `update_run_cost()` stores the total API cost for the run.

**Code changes in `pipeline/db.py`:**

```python
# Change insert_lead return:
#   Old: cur.execute("INSERT ... ON CONFLICT DO NOTHING"); return cur.rowcount > 0
#   New: cur.execute("INSERT ... ON CONFLICT DO NOTHING RETURNING id"); row = cur.fetchone(); return row[0] if row else None

def update_lead(lead_id: int, fields: dict):
    """Update specific columns on an existing lead."""
    if not fields:
        return
    conn = get_connection()
    cur = conn.cursor()
    set_clause = ', '.join(f'{col} = %s' for col in fields.keys())
    values = list(fields.values())
    values.append(lead_id)
    cur.execute(f'UPDATE smb_leads SET {set_clause} WHERE id = %s', values)
    conn.commit()
    cur.close()

def get_lead(lead_id: int) -> dict | None:
    """Fetch a single lead by ID, return as dict."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM smb_leads WHERE id = %s', (lead_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return None
    cols = [desc[0] for desc in cur.description]
    cur.close()
    return dict(zip(cols, row))

def update_run_cost(run_id: int, cost: float):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('UPDATE pipeline_runs SET cost = %s WHERE id = %s', (cost, run_id))
    conn.commit()
    cur.close()
```

**Note:** The `update_lead()` function needs to handle `text[]` columns (key_staff, services_offered, certifications) — psycopg2 automatically converts Python lists to Postgres arrays, so no special handling needed.

**Order:** Must be done before Steps 3-5 since they all call these functions.

---

## Step 3: Create `pipeline/enrichment.py` — Waterfall Enrichment

**What:** New file implementing the 7-step enrichment waterfall.

**Why:** This is the core new functionality — takes a raw lead and enriches it with
contact info, company intel, and reputation data from Orthogonal APIs, with Claude
as failsafe.

**Code structure:**

```python
# pipeline/enrichment.py

import os
from pipeline.db import get_lead, update_lead
# x402 session setup (same pattern as scraper.py)

ENRICHABLE_FIELDS = [
    'owner_email', 'owner_phone', 'owner_linkedin', 'owner_title',
    'employee_count', 'key_staff', 'year_established', 'services_offered',
    'company_description', 'revenue_estimate', 'certifications',
    'review_summary', 'facebook_url', 'yelp_url', 'google_maps_url',
]

def enrich_lead(lead_id: int, emit=None) -> dict:
    """Run full waterfall on one lead. Returns {'cost': float, 'sources': dict}."""
    lead = get_lead(lead_id)
    enriched = {}
    meta = {}
    total_cost = 0.0

    # Step 1: google_maps_url (free, no API call)
    if lead.get('google_place_id'):
        enriched['google_maps_url'] = f"https://www.google.com/maps/place/?q=place_id:{lead['google_place_id']}"
        meta['google_maps_url'] = {'source': 'constructed'}

    # Step 2: Hunter.io
    if not enriched.get('owner_email') and not lead.get('owner_email'):
        result, cost = _hunter_lookup(lead)
        total_cost += cost
        # merge result into enriched, update meta

    # Step 3: Apollo
    missing = _get_missing(lead, enriched)
    if any(f in missing for f in ['owner_email','owner_phone','owner_linkedin','owner_title','employee_count','key_staff']):
        result, cost = _apollo_lookup(lead)
        total_cost += cost
        # merge, update meta

    # Step 4: Sixtyfour
    missing = _get_missing(lead, enriched)
    if any(f in missing for f in ['owner_email','owner_phone','owner_title','employee_count','revenue_estimate']):
        result, cost = _sixtyfour_lookup(lead)
        total_cost += cost
        # merge, update meta

    # Step 5: ScrapeGraphAI — website
    missing = _get_missing(lead, enriched)
    if any(f in missing for f in ['services_offered','year_established','company_description','certifications','facebook_url','yelp_url','employee_count']):
        result, cost = _scrape_website(lead)
        total_cost += cost
        # merge, update meta

    # Step 6: ScrapeGraphAI — reviews
    if not enriched.get('review_summary') and not lead.get('review_summary'):
        result, cost = _scrape_reviews(lead, enriched)
        total_cost += cost
        # merge, update meta

    # Step 7: Claude failsafe
    missing = _get_missing(lead, enriched)
    if missing:
        result, cost = _claude_failsafe(lead, enriched, missing)
        total_cost += cost
        # merge, tag all as 'claude_inferred' in meta

    # Write to DB
    enriched['enrichment_meta'] = json.dumps(meta)
    update_lead(lead_id, enriched)

    return {'cost': total_cost, 'sources': meta}

def _get_missing(lead, enriched) -> list[str]:
    """Return field names that are still None/empty in both lead and enriched."""
    missing = []
    for f in ENRICHABLE_FIELDS:
        if f == 'google_maps_url':
            continue
        val = enriched.get(f) or lead.get(f)
        if val is None or val == '' or val == []:
            missing.append(f)
    return missing
```

**Each API helper function (`_hunter_lookup`, `_apollo_lookup`, etc.):**
- Uses the x402 session (same pattern as `scraper.py`) for Orthogonal APIs
- Makes the API call, parses the response, returns `(fields_dict, cost_float)`
- Handles errors gracefully — if an API fails, return empty dict and log the error
- Does NOT write to DB — just returns what it found

**Claude failsafe (`_claude_failsafe`):**
- Uses Anthropic SDK directly (not x402)
- Builds a prompt with all known data about the lead + list of missing fields
- Asks Claude to infer reasonable values
- Parses structured response into field dict
- All fields tagged as `claude_inferred` in meta

**Order:** After Step 2 (needs `get_lead`, `update_lead`). Before Step 5 (run.py calls this).

---

## Step 4: Create `pipeline/email_generator.py` — Claude Email Generation

**What:** New file that generates a personalized email for each lead using Claude API.

**Why:** Each lead needs a unique subject + body that references real details about
that specific business. This is what makes the emails non-generic.

**Code structure:**

```python
# pipeline/email_generator.py

import os
import anthropic
from pipeline.db import get_lead, update_lead

client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))

def generate_email(lead_id: int) -> dict:
    """Generate personalized email for one lead. Returns {'cost': float}."""
    lead = get_lead(lead_id)
    if not lead:
        return {'cost': 0}

    prompt = _build_prompt(lead)

    response = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=500,
        messages=[{'role': 'user', 'content': prompt}],
    )

    text = response.content[0].text
    subject, body = _parse_response(text)

    update_lead(lead_id, {
        'generated_subject': subject,
        'generated_email': body,
    })

    # Calculate cost from response.usage
    cost = (response.usage.input_tokens * 0.003 + response.usage.output_tokens * 0.015) / 1000
    return {'cost': cost}

def _build_prompt(lead: dict) -> str:
    """Build the full Claude prompt with all lead data."""
    # Use the exact prompt template from REQUIREMENTS_SCHEMA_UPDATE.md
    # Populate every field from the lead dict
    # Fields that are None/empty get "Not available" placeholder
    ...

def _parse_response(text: str) -> tuple[str, str]:
    """Extract SUBJECT: and BODY: from Claude's response."""
    # Split on SUBJECT: and BODY: markers
    # Return (subject_string, body_string)
    ...
```

**Key details:**
- Uses `anthropic` SDK directly — NOT through x402/Orthogonal
- Model: `claude-sonnet-4-6` (fast, cheap, good for short emails)
- `_build_prompt()` includes ALL 29 data fields from the requirements doc
- `_parse_response()` must handle edge cases (missing markers, extra whitespace)
- If generation fails for a lead, catch the exception and skip (don't crash pipeline)
- Returns cost so run.py can track total

**Dependencies:** `pip install anthropic` in the venv. Add `ANTHROPIC_API_KEY` to `.env`.

**Order:** After Step 2 (needs `get_lead`, `update_lead`). Before Step 5.

---

## Step 5: Update `pipeline/run.py` — Add Enrich + Generate Stages

**What:** Insert two new stages between Search and Outreach in the pipeline flow.

**Why:** The pipeline currently goes Search → Outreach. It now needs to go
Search → Enrich → Generate → Outreach, with proper event emission at each stage.

**Code changes:**

1. **Import new modules:**
   ```python
   from pipeline.enrichment import enrich_lead
   from pipeline.email_generator import generate_email
   from pipeline.db import update_run_cost
   ```

2. **Modify search loop — enforce 50-lead cap:**
   ```python
   # After insert_lead() call:
   lead_id = insert_lead(conn, normalized)  # Now returns int or None
   if lead_id is not None:
       inserted_ids.append(lead_id)
       inserted_leads.append(normalized)  # Keep for backward compat
       if len(inserted_ids) >= MAX_LEADS_PER_RUN:
           emit({'type': 'search_capped', 'count': MAX_LEADS_PER_RUN})
           break  # Exit both city and industry loops
   ```
   Note: need to break out of nested loops (industry × city). Use a flag variable
   or refactor the loop structure.

3. **Add Enrich stage (after search loop, before outreach):**
   ```python
   total_cost = 0.0

   emit({'type': 'enrich_start', 'count': len(inserted_ids)})
   for i, lead_id in enumerate(inserted_ids):
       try:
           result = enrich_lead(lead_id, emit=emit)
           total_cost += result['cost']
           emit({
               'type': 'enrich_lead',
               'index': i + 1,
               'total': len(inserted_ids),
               'company': inserted_leads[i].get('company', ''),
               'sources': list(result['sources'].keys()),
           })
       except Exception as e:
           emit({'type': 'enrich_error', 'message': str(e), 'lead_id': lead_id})
   emit({'type': 'enrich_done', 'count': len(inserted_ids)})
   ```

4. **Add Generate stage (after enrich, before outreach):**
   ```python
   emit({'type': 'generate_start', 'count': len(inserted_ids)})
   for i, lead_id in enumerate(inserted_ids):
       try:
           result = generate_email(lead_id)
           total_cost += result['cost']
           emit({
               'type': 'generate_lead',
               'index': i + 1,
               'total': len(inserted_ids),
               'company': inserted_leads[i].get('company', ''),
           })
       except Exception as e:
           emit({'type': 'generate_error', 'message': str(e), 'lead_id': lead_id})
   emit({'type': 'generate_done', 'count': len(inserted_ids)})

   # Store total cost on the run
   update_run_cost(run_id, total_cost)
   ```

5. **Pass `run_id` into `run_pipeline()`:**
   Currently `run_pipeline(emit)` only takes an emit callback. It needs the run_id
   to call `update_run_cost()`. Either pass it as a parameter or have the caller
   (pipeline_runner.py) handle cost tracking.

**Order:** After Steps 3 and 4 (imports enrichment.py and email_generator.py).

---

## Step 6: Update `pipeline/instantly.py` — Per-Lead Custom Variables

**What:** Modify `_lead_to_instantly()` to include `generated_subject` and
`generated_email` as custom variables in the Instantly lead payload.

**Why:** Instantly needs each lead to carry its own unique email content. The
Instantly campaign template will reference `{{subject}}` and `{{email_body}}`
custom variables, so every email sent is unique.

**Code changes in `pipeline/instantly.py`:**

```python
def _lead_to_instantly(lead: dict) -> dict:
    # ... existing name/email mapping ...
    payload = {
        'email': lead.get('email') or lead.get('owner_email'),
        'first_name': first,
        'last_name': last,
        'company_name': lead.get('company', ''),
        'custom_variables': {
            'phone': lead.get('phone', ''),
            'city': lead.get('city', ''),
            'industry': lead.get('industry', ''),
            # NEW: AI-generated email content
            'subject': lead.get('generated_subject', ''),
            'email_body': lead.get('generated_email', ''),
        },
    }
    return payload
```

**Also update `push_leads()`:**
- Currently receives a list of lead dicts from normalize output
- Now needs to receive full DB rows (with generated_email etc.)
- Modify to accept lead dicts that include the enrichment + email fields
- Or: have run.py re-read leads from DB before pushing to Instantly:
  ```python
  from pipeline.db import get_lead
  leads_to_push = [get_lead(lid) for lid in inserted_ids]
  leads_to_push = [l for l in leads_to_push if l and l.get('generated_email')]
  ```

**Prefer email priority:** Use `owner_email` over generic `email` when available:
```python
'email': lead.get('owner_email') or lead.get('email'),
```

**Order:** After Step 5 (run.py changes determine how leads are passed).

---

## Step 7: Add `ANTHROPIC_API_KEY` to `.env`

**What:** Add the Anthropic API key to the `.env` file.

**Why:** Both `pipeline/email_generator.py` (Step 4) and the Claude failsafe in
`pipeline/enrichment.py` (Step 3) need this key to call the Claude API directly.

**Change:**
```
ANTHROPIC_API_KEY=sk-ant-...
```

**Order:** Before testing Steps 3-4. User needs to provide their actual key.

---

## Step 8: Update `pipeline/config.py` — Add `MAX_LEADS_PER_RUN`

**What:** `MAX_LEADS_PER_RUN = 50` is already defined (added earlier). Verify it's
present and used correctly in run.py.

**Why:** The search phase stops inserting once this cap is reached. Already added
in a prior session — just confirm it's wired into the search loop in Step 5.

**Order:** Verify during Step 5.

---

## Step 9: Update Frontend — Pipeline Graph (6 Nodes)

**What:** Add Enrich and Generate nodes to `PipelineGraph.tsx`.

**Why:** The graph currently shows 4 nodes: Config → Search → Outreach → Done.
It needs 6: Config → Search → Enrich → Generate → Outreach → Done.

**Code changes in `ui/src/components/PipelineGraph.tsx`:**

1. **Add 2 new node definitions in the nodes array:**
   ```tsx
   { id: 'enrich', position: { x: 350, y: 100 }, ... }
   { id: 'generate', position: { x: 525, y: 100 }, ... }
   ```
   Shift Outreach and Done nodes right to make room.

2. **Add 2 new edges:**
   ```tsx
   { id: 'e-search-enrich', source: 'search', target: 'enrich', ... }
   { id: 'e-enrich-generate', source: 'enrich', target: 'generate', ... }
   ```
   Update existing edge from search→outreach to generate→outreach.

3. **Add node state logic in the parent component:**
   Map new event types to node states:
   - `enrich_start` → enrich node `active`
   - `enrich_done` → enrich node `complete`
   - `generate_start` → generate node `active`
   - `generate_done` → generate node `complete`
   - `enrich_error` / `generate_error` → respective node `error`

4. **Add subText for new nodes:**
   - Enrich: show `"3/50 leads..."` during enrichment
   - Generate: show `"3/50 emails..."` during generation

**Order:** After Step 5 (events must exist before frontend can react to them).

---

## Step 10: Update Frontend — LiveFeed Event Rendering

**What:** Add rendering for new pipeline event types in `LiveFeed.tsx`.

**Why:** The live feed needs to show enrichment and generation progress as it
happens in real time.

**Code changes in `ui/src/components/LiveFeed.tsx`:**

Add cases for these event types:
- `enrich_start` → "Starting enrichment for {count} leads..."
- `enrich_lead` → "Enriched {company} ({index}/{total}) — sources: {sources}"
- `enrich_done` → "Enrichment complete — {count} leads enriched"
- `enrich_error` → "Enrichment error: {message}"
- `generate_start` → "Generating emails for {count} leads..."
- `generate_lead` → "Email generated for {company} ({index}/{total})"
- `generate_done` → "Email generation complete — {count} emails"
- `generate_error` → "Email generation error: {message}"
- `search_capped` → "Search cap reached — {count} leads inserted"

**Order:** After Step 5 (events must exist).

---

## Step 11: Update Frontend — Types

**What:** Add new event fields to `PipelineEvent` in `types.ts`.

**Why:** TypeScript needs to know about the new event fields.

**Code changes in `ui/src/types.ts`:**

```typescript
export interface PipelineEvent {
  // ... existing fields ...
  // enrichment events
  sources?: string[]
  lead_id?: number
  // cost
  cost?: number
}
```

Also update `RunRecord` to include `cost`:
```typescript
export interface RunRecord {
  // ... existing fields ...
  cost: number | null
}
```

**Order:** Before Steps 9-10 (frontend components import these types).

---

## Step 12: Update Backend — API Endpoints

**What:** Add new endpoints and update existing ones.

**Why:** The frontend needs to read lead details (for lead viewer) and the run
cost data.

**Code changes in `api/main.py`:**

1. **GET `/api/leads`** — list leads with pagination
   ```python
   @app.get('/api/leads')
   async def list_leads(limit: int = 50, offset: int = 0):
       # Query smb_leads ORDER BY created_at DESC LIMIT/OFFSET
       # Return list of lead dicts
   ```

2. **GET `/api/leads/{lead_id}`** — single lead with all fields
   ```python
   @app.get('/api/leads/{lead_id}')
   async def get_lead_detail(lead_id: int):
       from pipeline.db import get_lead
       lead = get_lead(lead_id)
       if not lead:
           raise HTTPException(404)
       return lead
   ```

3. **Update `get_runs()`** — include cost field in run records
   The `api/db_queries.py` SELECT query for runs needs to include the new `cost`
   column.

**Order:** After Steps 1-2 (needs DB columns and functions).

---

## Step 13: Update `api/db_queries.py` — Include Cost in Run Queries

**What:** Add `cost` to the SELECT in `get_runs()` and return it in run records.

**Why:** The run history drawer should show how much each run cost.

**Code changes:**
- Add `cost` to the SELECT columns in `get_runs()`
- Add `cost` to the dict construction for each run record

**Order:** After Step 1 (column must exist).

---

## Step 14: Frontend — Lead Viewer Component

**What:** New component to browse and inspect individual leads with all their data.

**Why:** User wants "a way to view all the leads that were generated and the data
for every lead" (from requirements discussion).

**New file: `ui/src/components/LeadViewer.tsx`**

- Table/list view of all leads with columns: company, industry, city, status, created_at
- Click a lead to expand/open detail view showing all fields
- Detail view shows: basic info, enrichment data, generated email preview
- Fields sourced from Claude failsafe could be visually distinguished
- Pagination (50 per page)
- Calls GET `/api/leads` and GET `/api/leads/{id}`

**Also update `ui/src/App.tsx`:**
- Add a way to navigate to the lead viewer (tab, button, or drawer)
- Import and render the LeadViewer component

**Also update `ui/src/api.ts`:**
- Add `getLeads(limit, offset)` and `getLead(id)` fetch functions

**Order:** After Step 12 (needs API endpoints).

---

## Step 15: Update Run History — Show Cost

**What:** Display cost per run in `RunHistoryDrawer.tsx`.

**Why:** User wants to track how much each run costs.

**Code changes in `ui/src/components/RunHistoryDrawer.tsx`:**
- Show `$X.XX` next to each run entry
- Read from `run.cost` field (now included in RunRecord type)

**Order:** After Steps 11 and 13.

---

## Execution Order Summary

```
Step 1:  DB migration (SQL)                    — no code deps
Step 2:  pipeline/db.py updates                — depends on Step 1
Step 7:  .env ANTHROPIC_API_KEY                — no code deps
Step 8:  pipeline/config.py verify             — no code deps
Step 3:  pipeline/enrichment.py (NEW)          — depends on Steps 2, 7
Step 4:  pipeline/email_generator.py (NEW)     — depends on Steps 2, 7
Step 5:  pipeline/run.py updates               — depends on Steps 3, 4
Step 6:  pipeline/instantly.py updates         — depends on Step 5
Step 11: ui/src/types.ts updates               — no code deps
Step 13: api/db_queries.py updates             — depends on Step 1
Step 12: api/main.py new endpoints             — depends on Steps 2, 13
Step 9:  PipelineGraph.tsx (6 nodes)           — depends on Steps 5, 11
Step 10: LiveFeed.tsx new events               — depends on Steps 5, 11
Step 14: LeadViewer.tsx (NEW)                  — depends on Steps 11, 12
Step 15: RunHistoryDrawer.tsx cost display     — depends on Steps 11, 13
```

### Parallelizable groups:
- **Group A (no deps):** Steps 1, 7, 8, 11
- **Group B (after DB):** Steps 2, 13
- **Group C (after db.py):** Steps 3, 4, 12
- **Group D (after enrichment):** Steps 5, 6
- **Group E (after events + types):** Steps 9, 10, 14, 15

---

## Testing Strategy

After each step, verify:

1. **After Step 1:** `\d smb_leads` shows all 17 new columns. `\d pipeline_runs` shows `cost`.
2. **After Step 2:** Run `insert_lead()` manually — confirm it returns an int. Run `update_lead()` — confirm it writes. Run `get_lead()` — confirm it reads all columns.
3. **After Step 3:** Test `enrich_lead()` on one existing lead ID. Check DB for populated fields and enrichment_meta.
4. **After Step 4:** Test `generate_email()` on one enriched lead. Check DB for generated_subject and generated_email.
5. **After Step 5:** Run full pipeline. Confirm events fire in order: search → enrich → generate → outreach. Check that cost is written to pipeline_runs.
6. **After Step 6:** Confirm Instantly payload includes custom variables with generated email content.
7. **After Steps 9-15:** Start UI, trigger a run, confirm 6-node graph animates correctly, live feed shows all event types, run history shows cost, lead viewer displays enriched data.
