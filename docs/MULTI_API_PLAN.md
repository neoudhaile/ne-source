# Multi-API Orchestration Plan (Deferred)

## Concept
Plug-and-play Orthogonal API integrations organized into a waterfall pipeline.
Two types: Search Sources (produce leads) and Enrichment (fill missing fields on existing leads).

## Waterfall Pattern
For enrichment: try API 1 for a field, if empty try API 2, stop when filled.
Priority order is configurable in UI via drag-to-reorder.

## Pipeline Graph (future state)
[Config] → [Search] → [Enrich] → [Outreach] → [Done]

## Search Source APIs (candidates)
- Openmart (current) — business search via Google Maps data
- Others TBD from Orthogonal API catalog

## Enrichment APIs (candidates)
- Orthogonal lookup_people — owner name, email
- ScrapeGraphAI smartscraper — employee count, revenue, social links
- Others TBD

## Schema additions needed (not yet built)
When this is built out, smb_leads will need new columns for:
- employee_count (int)
- estimated_revenue (text or numeric)
- linkedin_url (text)
- facebook_url (text)
- instagram_url (text)
- yelp_url (text)
- enriched_at (timestamp)
- enrichment_source (text — which API filled the data)

## API Registry (future DB table)
CREATE TABLE api_registry (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    endpoint    TEXT NOT NULL,
    type        TEXT NOT NULL CHECK (type IN ('search', 'enrichment')),
    fields      TEXT[],          -- which smb_leads fields this API fills
    body_template JSONB,         -- request body with {{variable}} placeholders
    priority    INT DEFAULT 0,   -- waterfall order (lower = higher priority)
    enabled     BOOLEAN DEFAULT true,
    created_at  TIMESTAMPTZ DEFAULT now()
);

## Frontend components (future)
- ApiManagementPanel.tsx — register/edit/delete APIs
- SearchSourcesPanel.tsx — toggle and reorder search sources
- EnrichmentPanel.tsx — configure and reorder enrichment waterfall
- New Enrich node in PipelineGraph.tsx

## Decision
Deferred until MVP data quality is validated (2-3 weeks of pipeline runs).
Build Instantly outreach integration first since it uses existing lead data.
