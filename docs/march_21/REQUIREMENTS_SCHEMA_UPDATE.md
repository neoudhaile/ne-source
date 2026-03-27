# Ne'Source — Enrichment + AI Email Generation Requirements

## The Problem

Right now the pipeline finds leads (company name, address, rating, owner
name if available) and pushes them straight to Instantly. Every lead gets
the same generic template. Generic cold emails get ignored — response
rates sit around 1-3%.

## The Goal

Every outbound email is unique and specific to the recipient company.
The email references real details about that business (services, reviews,
years in operation, owner name, employee count, online presence) so it
reads like a human researched them. Target: 10-15% response rate.

## How It Works — New Pipeline Flow

```
Current:
  Config → Search → Outreach (push to Instantly) → Done

New:
  Config → Search → Enrich → Generate → Outreach → Done
                      │          │
                  Waterfall   Claude API
                  (Orth APIs)  (per-lead email)
```

### Step-by-step:

1. **Search** — Openmart finds businesses, geo-filter, dedup, insert
   into `smb_leads`. **Hard cap: 50 leads per run.** Once 50 new leads
   have been inserted, the search loop stops — remaining queries are
   skipped. Leads beyond 50 are NOT inserted into the DB because the
   next run may have different config (industries, cities, filters),
   so stale unprocessed leads from a prior config shouldn't sit in the
   table. `insert_lead()` now returns the row `id` (not True/False).
   All inserted IDs are collected into `inserted_ids[]`.

2. **Enrich** (NEW) — for each ID in `inserted_ids`, run the enrichment
   waterfall: Orthogonal APIs first (lookup_people → ScrapeGraphAI),
   then Claude as failsafe for any fields still empty. Each lead gets
   an UPDATE with the enriched data. Claude-inferred fields are tagged
   in `enrichment_sources` so we can distinguish API-sourced vs
   AI-inferred data.

3. **Generate** (NEW) — for each ID in `inserted_ids`, re-read the
   enriched lead from DB, build a Claude prompt with all available
   data, generate a unique subject + email body. UPDATE the row with
   `generated_subject`, `generated_email`, `email_generated_at`.

4. **Outreach** — load all leads WHERE `id IN inserted_ids AND
   generated_email IS NOT NULL`. Push to Instantly with the generated
   subject and body as per-lead custom variables. Only leads from THIS
   run are pushed — never stale leads from prior runs.

---

## Enrichment — Waterfall Detail

### Fields to enrich (final)

| Field | Type | Why it matters for emails |
|-------|------|--------------------------|
| `owner_email` | text | Direct owner inbox instead of info@ generic |
| `owner_phone` | text | "I can give you a quick call at [direct]" |
| `owner_linkedin` | text | Reference their profile — "saw on LinkedIn you..." |
| `owner_title` | text | Correct title — "as the Founder..." vs "as the GM..." |
| `employee_count` | int | Size framing — "a team of 12" vs "your growing crew" |
| `key_staff` | text[] | Depth — shows we know who works there |
| `year_established` | int | Longevity — "after 15 years building [Company]..." |
| `services_offered` | text[] | Specificity — "your residential HVAC and ductwork services" |
| `company_description` | text | General context for Claude email prompt |
| `revenue_estimate` | text | Size-appropriate tone — small shop vs larger operation |
| `certifications` | text[] | Compliment — "your EPA certification shows..." |
| `review_summary` | text | Specific praise — "customers love your same-day service" |
| `facebook_url` | text | Alt channel reference |
| `yelp_url` | text | Link to review data |
| `google_maps_url` | text | Direct link to their Google profile |
| `enrichment_meta` | jsonb | Per-field source attribution (see schema section) |

### Waterfall logic

For each lead ID in `inserted_ids`, run these steps in order.
Each step only populates fields that are still empty after the
previous step. The waterfall is hardcoded — not configurable in the UI.

```
Step 1: Construct google_maps_url
  - Input: google_place_id (already on the lead)
  - Output: google_maps_url
  - Cost: free (string construction, no API call)

Step 2: Hunter.io — email lookup
  - Endpoint: /domain-search
  - Input: company website domain
  - Fills: owner_email
  - Cost: ~$0.01

Step 3: Apollo — people match
  - Endpoint: /v1/people/match
  - Input: owner_name + company name + city
  - Fills: owner_email, owner_phone, owner_linkedin, owner_title,
           employee_count, key_staff
  - Cost: ~$0.01

Step 4: Sixtyfour — lead enrichment
  - Endpoint: /enrich-lead, /find-email, /find-phone
  - Input: company name + owner_name + city
  - Fills: owner_email, owner_phone, owner_title, employee_count,
           revenue_estimate
  - Cost: ~$0.10

Step 5: ScrapeGraphAI — company website scrape
  - Endpoint: /v1/smartscraper
  - Input: company website URL
  - Prompt: extract services, year established, description,
            certifications, social links (facebook, yelp)
  - Fills: services_offered, year_established, company_description,
           certifications, facebook_url, yelp_url, employee_count
  - Cost: ~$0.04

Step 6: ScrapeGraphAI — review scrape
  - Endpoint: /v1/smartscraper
  - Input: google_maps_url or yelp_url
  - Prompt: extract top review themes and customer sentiment
  - Fills: review_summary
  - Cost: ~$0.04

Step 7 (FAILSAFE): Claude API
  - Runs ONLY for fields still empty after all Orthogonal APIs
  - Input: company name, website, address, city, industry + all
    data already collected from steps 1-6
  - Claude infers missing fields from public knowledge, industry
    norms, and any partial data available
  - Fields filled by Claude are tagged as 'claude_inferred' in
    enrichment_meta so we know what's API-sourced vs AI-inferred
  - Cost: ~$0.005 per lead (small prompt, Sonnet)
```

After all 7 steps, UPDATE the lead row with all enriched fields
and the enrichment_meta JSONB. Each field in enrichment_meta records
which step sourced it (e.g. `{"owner_email": {"source": "hunter"}}`).

### Schema migration (FINAL)

```sql
-- People & Contact Enrichment
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

-- Enrichment metadata (per-field source attribution)
ALTER TABLE smb_leads ADD COLUMN enrichment_meta   JSONB;

-- AI-generated email
ALTER TABLE smb_leads ADD COLUMN generated_subject TEXT;
ALTER TABLE smb_leads ADD COLUMN generated_email   TEXT;

-- Run cost tracking
ALTER TABLE pipeline_runs ADD COLUMN cost NUMERIC(8,4) DEFAULT 0;
```

### enrichment_meta JSONB structure

Stores which API sourced each enriched field. Example:
```json
{
  "owner_email":    {"source": "hunter", "raw_response_key": "emails[0].value"},
  "employee_count": {"source": "sixtyfour"},
  "services_offered": {"source": "scrapegraphai"},
  "review_summary": {"source": "claude_inferred"},
  "year_established": {"source": "scrapegraphai"}
}
```
Fields sourced from the original Openmart search are NOT included here —
only fields populated during the enrichment phase. `claude_inferred`
means Claude filled the gap as the failsafe.

### Key design decisions

- **Enrichment is fixed, not configurable.** Waterfall order and APIs
  are hardcoded. No UI toggles.
- **Flat columns + one metadata JSONB.** Data columns are queryable
  directly. `enrichment_meta` stores provenance per field.
- **Cost on runs, not leads.** `pipeline_runs.cost` tracks total
  enrichment + generation cost per run.
- **Timestamps on runs, not leads.** No `enriched_at` or
  `email_generated_at` columns. The run's timestamps cover this.
- **`google_maps_url` is constructed.** Built from `google_place_id`:
  `https://www.google.com/maps/place/?q=place_id:{google_place_id}`

---

## AI Email Generation — Detail

### Input to Claude

For each lead, construct a prompt with:

```
COMPANY DATA:
- Company name: {company}
- Industry: {industry}
- Address: {address}
- City: {city}, {state} {zipcode}
- Website: {website}
- Google Maps: {google_maps_url}
- Google rating: {rating} ({review_count} reviews)
- Ownership type: {ownership_type}
- Distance from LA: {distance_miles} miles

OWNER / CONTACT:
- Owner name: {owner_name}
- Owner title: {owner_title}
- Owner email: {owner_email}
- Owner phone: {owner_phone}
- Owner LinkedIn: {owner_linkedin}

TEAM:
- Employee count: {employee_count}
- Key staff: {key_staff}

BUSINESS DETAIL:
- Year established: {year_established}
- Services offered: {services_offered}
- Company description: {company_description}
- Revenue estimate: {revenue_estimate}
- Certifications: {certifications}

REPUTATION:
- Review highlights: {review_summary}
- Facebook: {facebook_url}
- Yelp: {yelp_url}

CONTEXT:
Broeren Haile Holdings is an acquisition firm looking to acquire
established service businesses in the LA metro area. We want to
reach out to {owner_name} to explore whether they'd be open to
a conversation about a potential acquisition or partnership.

INSTRUCTIONS:
Write a short, warm, personalized email (3-5 sentences max) from
our team to {owner_name}. Reference specific details about their
business that show we've done our research. Keep the tone
conversational — not salesy. End with a soft ask for a brief call.
Do not use generic filler. Every sentence should be specific to
this company. Use only data provided above — do not invent facts.

Output format:
SUBJECT: <subject line>
BODY: <email body>
```

### Implementation

- New file: `pipeline/email_generator.py`
- Uses Claude API (Anthropic SDK) directly — NOT through Orthogonal/x402
- Model: claude-sonnet-4-6 (fast, cheap, good enough for short emails)
- Batch processing: generate all emails sequentially with a small delay
  to respect rate limits
- Store `generated_subject` and `generated_email` on the lead record
- If generation fails for a lead, skip it (don't block the pipeline)

### Cost estimate

- ~40 leads per run
- ~500 tokens per email generation (input + output)
- Sonnet pricing: ~$0.003 per 1K input tokens, ~$0.015 per 1K output
- Per run: ~$0.36 (40 leads × ~$0.009 each)
- Per month (daily runs): ~$11

---

## Instantly Integration Changes

### Current behavior
- `push_leads()` sends all new leads to a campaign as contacts
- Instantly sends the same template email to all of them

### New behavior
- Each lead is pushed with its `generated_email` as the email body
- Instantly API supports per-lead custom variables in the lead payload
- Map `generated_subject` → Instantly subject field
- Map `generated_email` → Instantly body field (via custom variable
  referenced in the Instantly template)

### Instantly template setup
Create a minimal template in Instantly that just renders the custom var:
```
Subject: {{subject}}
Body: {{email_body}}
```

The AI-generated content fills both fields per lead.

---

## What Changes in the Codebase

### New files
| File | Purpose |
|------|---------|
| `pipeline/enrichment.py` | Waterfall enrichment logic |
| `pipeline/email_generator.py` | Claude API email generation |

### Modified files
| File | Change |
|------|--------|
| `pipeline/run.py` | Add Enrich and Generate stages between Search and Outreach |
| `pipeline/instantly.py` | Pass `generated_subject` + `generated_email` as custom vars |
| `pipeline/db.py` | `update_lead()` function for writing enrichment + email data |
| `pipeline/config.py` | Add `ANTHROPIC_API_KEY` reference, enrichment toggle |
| `api/main.py` | New endpoint: `POST /api/leads/{id}/regenerate` (re-gen one email) |
| `ui/src/components/PipelineGraph.tsx` | 2 new nodes: Enrich, Generate |
| `ui/src/components/LiveFeed.tsx` | Handle new event types |
| `ui/src/types.ts` | New event type fields |
| `.env` | Add `ANTHROPIC_API_KEY` |

### New pipeline events
| Event | When |
|-------|------|
| `enrich_start` | Beginning enrichment phase |
| `enrich_lead` | One lead enriched (with source info) |
| `enrich_done` | All leads enriched (summary counts) |
| `generate_start` | Beginning email generation phase |
| `generate_lead` | One email generated (with company name) |
| `generate_done` | All emails generated (summary) |
| `generate_error` | Claude API error for one lead |

### New pipeline graph (6 nodes)
```
Config → Search → Enrich → Generate → Outreach → Done
```

---

## .env additions

```
ANTHROPIC_API_KEY=sk-ant-...
```

---

## Open Questions

1. **Email tone/template**: should we create multiple prompt variants
   (formal vs casual) and A/B test via Instantly? Could track which
   tone gets more replies.

   - no lets just have one email for now

2. **Re-enrichment**: if a lead was enriched 30+ days ago, should the
   pipeline re-enrich on next run? Or only enrich once?

   - no keep the data as is 

3. **Email preview**: should the UI show a preview of the generated
   email before pushing to Instantly? Would add a "Review & Send"
   step instead of auto-pushing.

   - we will get to this later but just so you know how we will be creating a campaigns feature so all this is configurable at the start, for now we just want to create the pipeline and get a design down

4. **Reply tracking**: Instantly tracks replies. Should we pull reply
   data back into the DB and surface it in the UI? Would close the
   feedback loop (which emails actually work).

   - yes we want to track each reply in the UI, but we can do that after we understand the enrichment information and data storage

5. **Cost guardrails**: should we set a max leads-per-run cap to
   prevent runaway API costs? E.g., enrich max 50 leads per run.

   - yes 50 is good. Cap applied at search phase — once 50 new leads
     are inserted, search stops. Leads beyond 50 are NOT inserted into
     the DB (next run may have different config, don't want stale
     unprocessed rows).

---

## Orthogonal API Data Catalog — What We Can Get Per Company

Below is every piece of information we can extract about a single company
using Orthogonal's API platform, which API provides it, the cost, and
how that data point can be used to personalize an outreach email.

### Data already captured (Openmart — current pipeline)

| Data Point | Field | Source | Email Use |
|---|---|---|---|
| Company name | `company` | Openmart | Addressing — "Hi [Owner], I came across [Company]..." |
| Owner name | `owner_name` | Openmart (staffs[]) | Direct address — makes email feel personal, not bulk |
| Email | `email` | Openmart | Required for sending |
| Phone | `phone` | Openmart | Mention as alt contact — "happy to call at [phone]" |
| Address | `address` | Openmart | Geo reference — "your location on [Street]" |
| City / State | `city`, `state` | Openmart | Regional framing — "businesses in [City]" |
| Website | `website` | Openmart | Research signal — "I was on your site..." |
| Google rating | `rating` | Openmart | Compliment — "your 4.8 rating speaks for itself" |
| Review count | `review_count` | Openmart | Social proof — "with 200+ reviews..." |
| Industry | `industry` | Config (search query) | Industry-specific language |
| Ownership type | `ownership_type` | Openmart | Tailor approach — family vs independent |
| Distance from LA | `distance_miles` | Calculated | Locality — "right here in the LA area" |
| Google Maps link | `google_maps_url` | Constructed from `google_place_id` | Direct link to their Google profile |

### New data — People & Contact Enrichment

| Data Point | Field | API Options (waterfall order) | Cost | Email Use |
|---|---|---|---|---|
| Owner email (verified) | `owner_email` | 1. Hunter.io `/domain-search` 2. Sixtyfour `/find-email` 3. Tomba `/v1/linkedin` 4. Apollo `/v1/people/match` | $0.01-0.10 | Direct owner inbox instead of info@ generic |
| Owner phone (direct) | `owner_phone` | 1. Sixtyfour `/find-phone` 2. Apollo `/v1/people/match` | $0.10 | "I can give you a quick call at [direct]" |
| Owner LinkedIn URL | `owner_linkedin` | 1. Apollo `/v1/people/match` 2. Fiber `/v1/natural-language-search/profiles` | $0.01-0.05 | Reference their profile — "saw on LinkedIn you..." |
| Owner title/role | `owner_title` | 1. Apollo `/v1/people/match` 2. Sixtyfour `/enrich-lead` | $0.01-0.10 | Correct title — "as the Founder..." vs "as the GM..." |
| Employee count | `employee_count` | 1. Sixtyfour `/enrich-lead` 2. Apollo `/v1/people/match` 3. ScrapeGraphAI `/v1/smartscraper` | $0.01-0.10 | Size framing — "a team of 12" vs "your growing crew" |
| Key staff names | `key_staff` | 1. Apollo `/v1/people/match` 2. Fiber `/v1/natural-language-search/profiles` | $0.01-0.05 | Depth — shows we know who works there |

### New data — Company Intelligence

| Data Point | Field | API Options (waterfall order) | Cost | Email Use |
|---|---|---|---|---|
| Year established | `year_established` | 1. ScrapeGraphAI `/v1/smartscraper` (website about page) 2. Brand.dev `/v1/brand/retrieve` | $0.01-0.04 | Longevity — "after 15 years building [Company]..." |
| Services offered | `services_offered` | 1. ScrapeGraphAI `/v1/smartscraper` (website services page) 2. Openmart raw_data | $0.04 | Specificity — "your residential HVAC and ductwork services" |
| Company description | `company_description` | 1. Brand.dev `/v1/brand/retrieve` 2. ScrapeGraphAI `/v1/smartscraper` | $0.01-0.04 | General context for Claude prompt |
| Revenue estimate | `revenue_estimate` | 1. Sixtyfour `/enrich-lead` 2. PredictLeads `/v3/companies/{domain}/financing_events` | $0.10 | Size-appropriate tone — small shop vs larger operation |
| License/certifications | `certifications` | 1. ScrapeGraphAI `/v1/smartscraper` (website) | $0.04 | Compliment — "your EPA certification shows..." |

### New data — Online Presence & Reputation

| Data Point | Field | API Options (waterfall order) | Cost | Email Use |
|---|---|---|---|---|
| Review highlights | `review_summary` | 1. ScrapeGraphAI `/v1/smartscraper` (Google/Yelp page) | $0.04 | Specific praise — "customers love your same-day service" |
| Facebook page | `facebook_url` | 1. ScrapeGraphAI `/v1/smartscraper` (website footer) 2. Brand.dev | $0.01-0.04 | Alt channel reference |

| Yelp page | `yelp_url` | 1. ScrapeGraphAI `/v1/smartscraper` 2. SearchAPI | $0.01-0.04 | Link to review data |

### Failsafe — Claude API (Priority Last)

| Data Point | Source | Cost | Notes |
|---|---|---|---|
| Any field still empty | Claude API (Sonnet) | ~$0.005/lead | Claude infers from company name + website + industry + any partial data already collected. Tagged as `claude_inferred` in enrichment_meta. Not as reliable as API data but ensures no empty fields in email generation. |

---

### Cost Summary Per Lead (worst case — all APIs called)

| Stage | APIs Called | Max Cost |
|---|---|---|
| Search | Openmart | $0.01 (x402) |
| Enrich: contacts | Hunter + Sixtyfour + Apollo | ~$0.22 |
| Enrich: company intel | ScrapeGraphAI (2 calls) + Brand.dev | ~$0.09 |
| Enrich: reputation | ScrapeGraphAI (1 call) | ~$0.04 |
| Enrich: failsafe | Claude Sonnet | ~$0.005 |
| Email generation | Claude Sonnet | ~$0.009 |
| **Total worst case** | | **~$0.37/lead** |
| **50-lead run worst case** | | **~$18.50** |

In practice, the waterfall means most leads won't hit every API.
Expected average: **~$0.12-0.20/lead** ($6-10 per run of 50).